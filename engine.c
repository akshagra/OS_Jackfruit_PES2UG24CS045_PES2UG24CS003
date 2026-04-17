/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;
    
    mkdir(LOG_DIR, 0755);

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) {
            write(fd, item.data, item.length);
            close(fd);
        }
    }
    return NULL;
}

/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = arg;
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc");
        return 1;
    }
    if (cfg->nice_value != 0) {
        nice(cfg->nice_value);
    }
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }
    char *argv_exec[] = { "sh", "-c", cfg->command, NULL };
    execvp(argv_exec[0], argv_exec);
    perror("execvp");
    return 1;
}

typedef struct {
    int fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} pipe_reader_args_t;

void *pipe_reader_thread(void *arg) {
    pipe_reader_args_t *args = arg;
    char buf[LOG_CHUNK_SIZE];
    ssize_t n;
    while ((n = read(args->fd, buf, sizeof(buf))) > 0) {
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, args->container_id, sizeof(item.container_id) - 1);
        item.length = n;
        memcpy(item.data, buf, n);
        bounded_buffer_push(args->buffer, &item);
    }
    close(args->fd);
    free(args);
    return NULL;
}

supervisor_ctx_t *g_ctx = NULL;
void handle_sig(int sig) {
    (void)sig; // Fix unused parameter warning
    if (g_ctx) g_ctx->should_stop = 1;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid);

void reap_children(supervisor_ctx_t *ctx) {
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *curr = ctx->containers;
        while (curr) {
            if (curr->host_pid == pid && (curr->state == CONTAINER_STARTING || curr->state == CONTAINER_RUNNING)) {
                if (WIFEXITED(status)) {
                    curr->exit_code = WEXITSTATUS(status);
                    curr->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    curr->exit_signal = WTERMSIG(status);
                    if (curr->exit_signal == SIGKILL && curr->state != CONTAINER_STOPPED) {
                        curr->state = CONTAINER_KILLED; // hard limit
                    } else if (curr->state != CONTAINER_STOPPED) {
                        curr->state = CONTAINER_EXITED;
                    }
                    curr->exit_code = 128 + curr->exit_signal;
                }
                unregister_from_monitor(ctx->monitor_fd, curr->id, pid);
                break;
            }
            curr = curr->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}


int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        perror("open /dev/container_monitor");
        // Non-fatal, just testing
    }

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        return 1;
    }
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    unlink(CONTROL_PATH);
    if (bind(ctx.server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }
    listen(ctx.server_fd, 10);

    g_ctx = &ctx;
    struct sigaction sa;
    sa.sa_handler = handle_sig;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);

    // Make socket non-blocking
    int flags = fcntl(ctx.server_fd, F_GETFL, 0);
    fcntl(ctx.server_fd, F_SETFL, flags | O_NONBLOCK);

    while (!ctx.should_stop) {
        reap_children(&ctx);
        
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd >= 0) {
            control_request_t req;
            control_response_t resp;
            memset(&resp, 0, sizeof(resp));
            if (read(client_fd, &req, sizeof(req)) == sizeof(req)) {
                pthread_mutex_lock(&ctx.metadata_lock);
                
                if (req.kind == CMD_START || req.kind == CMD_RUN) {
                    // Check if exists
                    container_record_t *curr = ctx.containers;
                    int exists = 0;
                    while (curr) {
                        if (strcmp(curr->id, req.container_id) == 0 && (curr->state == CONTAINER_RUNNING || curr->state == CONTAINER_STARTING)) {
                            exists = 1; break;
                        }
                        curr = curr->next;
                    }
                    if (exists) {
                        resp.status = 1;
                        snprintf(resp.message, sizeof(resp.message), "Container %s already running\n", req.container_id);
                    } else {
                        int pipefd[2];
                        pipe(pipefd);
                        
                        child_config_t *cfg = malloc(sizeof(*cfg));
                        strncpy(cfg->id, req.container_id, sizeof(cfg->id)-1);
                        strncpy(cfg->rootfs, req.rootfs, sizeof(cfg->rootfs)-1);
                        strncpy(cfg->command, req.command, sizeof(cfg->command)-1);
                        cfg->nice_value = req.nice_value;
                        cfg->log_write_fd = pipefd[1];
                        
                        char *stack = malloc(STACK_SIZE);
                        pid_t pid = clone(child_fn, stack + STACK_SIZE, CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD, cfg);
                        if (pid > 0) {
                            close(pipefd[1]); // Close write end
                            pipe_reader_args_t *pargs = malloc(sizeof(*pargs));
                            pargs->fd = pipefd[0];
                            strncpy(pargs->container_id, req.container_id, sizeof(pargs->container_id)-1);
                            pargs->buffer = &ctx.log_buffer;
                            pthread_t reader_tid;
                            pthread_create(&reader_tid, NULL, pipe_reader_thread, pargs);
                            pthread_detach(reader_tid);
                            
                            container_record_t *rec = calloc(1, sizeof(*rec));
                            strncpy(rec->id, req.container_id, sizeof(rec->id)-1);
                            rec->host_pid = pid;
                            rec->started_at = time(NULL);
                            rec->state = CONTAINER_RUNNING;
                            rec->soft_limit_bytes = req.soft_limit_bytes;
                            rec->hard_limit_bytes = req.hard_limit_bytes;
                            snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, rec->id);
                            rec->next = ctx.containers;
                            ctx.containers = rec;
                            
                            if (ctx.monitor_fd >= 0) {
                                register_with_monitor(ctx.monitor_fd, rec->id, pid, rec->soft_limit_bytes, rec->hard_limit_bytes);
                            }
                            
                            resp.status = 0;
                            snprintf(resp.message, sizeof(resp.message), "Started container %s with PID %d\n", rec->id, pid);
                        } else {
                            resp.status = 1;
                            snprintf(resp.message, sizeof(resp.message), "Failed to clone\n");
                        }
                    }
                } else if (req.kind == CMD_PS) {
                    char buf[2048] = {0};
                    int offset = 0;
                    container_record_t *curr = ctx.containers;
                    offset += snprintf(buf + offset, sizeof(buf) - offset, "%-15s %-8s %-15s %-10s\n", "ID", "PID", "STATE", "EXIT");
                    while (curr) {
                        offset += snprintf(buf + offset, sizeof(buf) - offset, "%-15s %-8d %-15s %-10d\n", 
                                curr->id, curr->host_pid, state_to_string(curr->state), curr->exit_code);
                        curr = curr->next;
                    }
                    strncpy(resp.message, buf, sizeof(resp.message) - 1);
                    resp.status = 0;
                } else if (req.kind == CMD_STOP) {
                    container_record_t *curr = ctx.containers;
                    int found = 0;
                    while (curr) {
                        if (strcmp(curr->id, req.container_id) == 0 && (curr->state == CONTAINER_RUNNING || curr->state == CONTAINER_STARTING)) {
                            curr->state = CONTAINER_STOPPED;
                            kill(curr->host_pid, SIGTERM);
                            found = 1;
                            break;
                        }
                        curr = curr->next;
                    }
                    resp.status = found ? 0 : 1;
                    if (found) snprintf(resp.message, sizeof(resp.message), "Stopped container %s\n", req.container_id);
                    else snprintf(resp.message, sizeof(resp.message), "Container not found or not running\n");
                } else if (req.kind == CMD_LOGS) {
                    char log_path[PATH_MAX];
                    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, req.container_id);
                    // Just return success and let client read it
                    resp.status = 0;
                    snprintf(resp.message, sizeof(resp.message), "%s", log_path);
                }
                
                pthread_mutex_unlock(&ctx.metadata_lock);
                write(client_fd, &resp, sizeof(resp));
                
                if (req.kind == CMD_RUN && resp.status == 0) {
                    // Just wait here in a loop checking metadata to see if it exited, and sending it
                    // Wait, RUN blocks the CLIENT, not the supervisor! So RUN should actually be handled client-side by polling or waiting.
                    // To keep it simple, we just return to client, and client will handle waiting.
                }
            }
            close(client_fd);
        } else {
            usleep(10000); // 10ms
        }
    }

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 1;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int sfd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un addr;
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));
    if (sfd < 0) return 1;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    if (connect(sfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect to supervisor");
        close(sfd);
        return 1;
    }
    if (write(sfd, req, sizeof(*req)) != sizeof(*req)) {
        close(sfd);
        return 1;
    }
    
    // Read response
    if (read(sfd, &resp, sizeof(resp)) > 0) {
        if (req->kind == CMD_LOGS && resp.status == 0) {
            // Cat the log file
            char cmd[1024];
            snprintf(cmd, sizeof(cmd), "cat %s", resp.message);
            system(cmd);
        } else {
            printf("%s", resp.message);
        }
    }
    close(sfd);

    if (req->kind == CMD_RUN && resp.status == 0) {
        // Pseudo-block waiting for container to exit.
        // In a real implementation we would stream logs and wait for termination msg.
        // Here we just sleep and periodically poll with a hidden command or just waitpid if we were a child?
        // Wait, the client doesn't know it exited unless it polls ps. 
        // We'll keep it simple: poll ps internally or just wait on logs if we used a pipe.
        // Actually, just looping ps
        int done = 0;
        while (!done) {
            usleep(500000);
            int s = socket(AF_UNIX, SOCK_STREAM, 0);
            if (connect(s, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
                control_request_t preq;
                memset(&preq, 0, sizeof(preq)); preq.kind = CMD_PS;
                write(s, &preq, sizeof(preq));
                control_response_t presp;
                if (read(s, &presp, sizeof(presp)) > 0) {
                    if (strstr(presp.message, req->container_id) && 
                        (strstr(presp.message, "exited") || strstr(presp.message, "killed") || strstr(presp.message, "stopped"))) {
                        done = 1;
                    }
                }
            }
            close(s);
        }
    }

    return resp.status;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    // Sends PS request
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
