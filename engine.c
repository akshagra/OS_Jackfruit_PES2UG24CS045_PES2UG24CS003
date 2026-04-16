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
#include <sys/select.h>
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
#define CONTROL_MESSAGE_LEN 256
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

typedef struct {
    int pipe_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item);
static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item);
static void *logging_thread(void *arg);
static int child_fn(void *arg);
static const char *state_to_string(container_state_t state);
static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes);
static int unregister_from_monitor(int monitor_fd,
                                   const char *container_id,
                                   pid_t host_pid);

static void *producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    log_item_t item;
    ssize_t n;

    strncpy(item.container_id, parg->container_id, sizeof(item.container_id) - 1);

    while ((n = read(parg->pipe_fd, item.data, sizeof(item.data))) > 0) {
        item.length = n;
        if (bounded_buffer_push(parg->buffer, &item) != 0) {
            break;
        }
    }

    close(parg->pipe_fd);
    free(parg);
    return NULL;
}

static void add_container_record(supervisor_ctx_t *ctx, container_record_t *rec)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static container_record_t *find_container_record(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *rec;
    pthread_mutex_lock(&ctx->metadata_lock);
    rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, id) == 0) {
            break;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
    return rec;
}

static void remove_container_record(supervisor_ctx_t *ctx, const char *id)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *prev = NULL;
    container_record_t *curr = ctx->containers;
    while (curr) {
        if (strcmp(curr->id, id) == 0) {
            if (prev) {
                prev->next = curr->next;
            } else {
                ctx->containers = curr->next;
            }
            free(curr);
            break;
        }
        prev = curr;
        curr = curr->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void sigchld_handler(int sig)
{
    (void)sig;
    // Reap children
    while (waitpid(-1, NULL, WNOHANG) > 0);
}

static supervisor_ctx_t *global_ctx = NULL;

static void signal_handler(int sig)
{
    (void)sig;
    if (global_ctx) {
        global_ctx->should_stop = 1;
    }
}

static int handle_start(supervisor_ctx_t *ctx, const control_request_t *req);
static int handle_run(supervisor_ctx_t *ctx, const control_request_t *req);
static int handle_ps(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp);
static int handle_logs(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp);
static int handle_stop(supervisor_ctx_t *ctx, const control_request_t *req);
static void handle_client_request(supervisor_ctx_t *ctx, int client_fd);

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

static int handle_start(supervisor_ctx_t *ctx, const control_request_t *req)
{
    container_record_t *rec = calloc(1, sizeof(*rec));
    if (!rec) return -1;

    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    rec->started_at = time(NULL);
    rec->state = CONTAINER_STARTING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    snprintf(rec->log_path, sizeof(rec->log_path), "%s/%s.log", LOG_DIR, req->container_id);

    // Create pipes for logging
    int pipefd[2];
    if (pipe(pipefd) != 0) {
        free(rec);
        return -1;
    }

    child_config_t config;
    memset(&config, 0, sizeof(config));
    strncpy(config.id, req->container_id, sizeof(config.id) - 1);
    strncpy(config.rootfs, req->rootfs, sizeof(config.rootfs) - 1);
    strncpy(config.command, req->command, sizeof(config.command) - 1);
    config.nice_value = req->nice_value;
    config.log_write_fd = pipefd[1];

    // Clone the child
    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        close(pipefd[0]);
        close(pipefd[1]);
        free(rec);
        return -1;
    }

    pid_t pid = clone(child_fn, stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      &config);
    if (pid < 0) {
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        free(rec);
        return -1;
    }

    rec->host_pid = pid;
    rec->state = CONTAINER_RUNNING;
    add_container_record(ctx, rec);

    // Register with monitor
    if (register_with_monitor(ctx->monitor_fd, req->container_id, pid,
                              req->soft_limit_bytes, req->hard_limit_bytes) != 0) {
        // Not fatal, continue
    }

    // Close the write end in supervisor
    close(pipefd[1]);

    // Start producer thread for this container
    producer_arg_t *parg = malloc(sizeof(*parg));
    if (parg) {
        parg->pipe_fd = pipefd[0];
        strncpy(parg->container_id, req->container_id, sizeof(parg->container_id) - 1);
        parg->buffer = &ctx->log_buffer;
        pthread_t prod_thread;
        if (pthread_create(&prod_thread, NULL, producer_thread, parg) == 0) {
            pthread_detach(prod_thread);
        } else {
            free(parg);
            close(pipefd[0]);
        }
    } else {
        close(pipefd[0]);
    }

    free(stack);
    return 0;
}

static int handle_run(supervisor_ctx_t *ctx, const control_request_t *req)
{
    // Similar to start, but wait for completion
    if (handle_start(ctx, req) != 0) return -1;

    container_record_t *rec = find_container_record(ctx, req->container_id);
    if (!rec) return -1;

    int status;
    waitpid(rec->host_pid, &status, 0);

    if (WIFEXITED(status)) {
        rec->exit_code = WEXITSTATUS(status);
        rec->state = CONTAINER_EXITED;
    } else if (WIFSIGNALED(status)) {
        rec->exit_signal = WTERMSIG(status);
        rec->state = CONTAINER_KILLED;
    }

    return rec->exit_code;
}

static int handle_ps(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp)
{
    (void)req;
    char *buf = resp->message;
    size_t len = sizeof(resp->message);
    int count = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        int written = snprintf(buf, len, "%s %d %s %lu %lu\n",
                               rec->id, rec->host_pid, state_to_string(rec->state),
                               rec->soft_limit_bytes, rec->hard_limit_bytes);
        if (written >= len) break;
        buf += written;
        len -= written;
        count++;
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return count;
}

static int handle_logs(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp)
{
    container_record_t *rec = find_container_record(ctx, req->container_id);
    if (!rec) {
        strcpy(resp->message, "Container not found");
        return -1;
    }

    FILE *f = fopen(rec->log_path, "r");
    if (!f) {
        strcpy(resp->message, "Log file not found");
        return -1;
    }

    size_t n = fread(resp->message, 1, sizeof(resp->message) - 1, f);
    resp->message[n] = '\0';
    fclose(f);
    return 0;
}

static int handle_stop(supervisor_ctx_t *ctx, const control_request_t *req)
{
    container_record_t *rec = find_container_record(ctx, req->container_id);
    if (!rec) return -1;

    rec->state = CONTAINER_STOPPED;
    kill(rec->host_pid, SIGTERM);
    return 0;
}

static void handle_client_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    ssize_t n;

    memset(&resp, 0, sizeof(resp));

    n = read(client_fd, &req, sizeof(req));
    if (n != sizeof(req)) {
        resp.status = -1;
        strcpy(resp.message, "Failed to read request");
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    switch (req.kind) {
    case CMD_START:
        resp.status = handle_start(ctx, &req);
        break;
    case CMD_RUN:
        resp.status = handle_run(ctx, &req);
        break;
    case CMD_PS:
        resp.status = handle_ps(ctx, &req, &resp);
        break;
    case CMD_LOGS:
        resp.status = handle_logs(ctx, &req, &resp);
        break;
    case CMD_STOP:
        resp.status = handle_stop(ctx, &req);
        break;
    default:
        resp.status = -1;
        strcpy(resp.message, "Unknown command");
        break;
    }

    write(client_fd, &resp, sizeof(resp));
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

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
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

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
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

/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    FILE *log_file = NULL;
    char current_id[CONTAINER_ID_LEN] = {0};

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        if (strcmp(current_id, item.container_id) != 0) {
            if (log_file) {
                fclose(log_file);
            }
            // Find the log path for this container
            pthread_mutex_lock(&ctx->metadata_lock);
            container_record_t *rec = ctx->containers;
            while (rec) {
                if (strcmp(rec->id, item.container_id) == 0) {
                    log_file = fopen(rec->log_path, "a");
                    break;
                }
                rec = rec->next;
            }
            pthread_mutex_unlock(&ctx->metadata_lock);
            if (!log_file) {
                fprintf(stderr, "Failed to open log file for container %s\n", item.container_id);
                continue;
            }
            strncpy(current_id, item.container_id, sizeof(current_id) - 1);
        }
        if (log_file) {
            fwrite(item.data, 1, item.length, log_file);
            fflush(log_file);
        }
    }
    if (log_file) {
        fclose(log_file);
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
    child_config_t *config = (child_config_t *)arg;
    char *argv[4] = {"/bin/sh", "-c", config->command, NULL};

    // Change root filesystem
    if (chroot(config->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    // Mount /proc
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount proc");
        return 1;
    }

    // Set nice value
    if (nice(config->nice_value) == -1) {
        perror("nice");
        return 1;
    }

    // Redirect stdout and stderr to the log pipe
    if (dup2(config->log_write_fd, STDOUT_FILENO) == -1 ||
        dup2(config->log_write_fd, STDERR_FILENO) == -1) {
        perror("dup2");
        return 1;
    }
    close(config->log_write_fd);

    // Execute the command
    execvp(argv[0], argv);
    perror("execvp");
    return 1;
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
static void handle_client_request(supervisor_ctx_t *ctx, int client_fd);
static int handle_start(supervisor_ctx_t *ctx, const control_request_t *req);
static int handle_run(supervisor_ctx_t *ctx, const control_request_t *req);
static int handle_ps(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp);
static int handle_logs(supervisor_ctx_t *ctx, const control_request_t *req, control_response_t *resp);
static int handle_stop(supervisor_ctx_t *ctx, const control_request_t *req);

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    int rc;
    struct sigaction sa;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    global_ctx = &ctx;

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

    // Create logs directory
    mkdir(LOG_DIR, 0755);

    // Open monitor device
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        perror("open /dev/container_monitor");
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    // Create UNIX socket
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    unlink(CONTROL_PATH);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(ctx.server_fd);
        close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen");
        close(ctx.server_fd);
        close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    // Install signal handlers
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = signal_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // Start logging thread
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create");
        close(ctx.server_fd);
        close(ctx.monitor_fd);
        bounded_buffer_destroy(&ctx.log_buffer);
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    while (!ctx.should_stop) {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(ctx.server_fd, &fds);
        int max_fd = ctx.server_fd;
        struct timeval tv = {1, 0};
        int ret = select(max_fd + 1, &fds, NULL, NULL, &tv);
        if (ret < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        if (ret == 0)
            continue;

        if (FD_ISSET(ctx.server_fd, &fds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd < 0) {
                if (errno == EINTR)
                    continue;
                perror("accept");
                continue;
            }
            handle_client_request(&ctx, client_fd);
            close(client_fd);
        }
    }

    // Shutdown
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    close(ctx.server_fd);
    close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    // Free containers
    container_record_t *rec = ctx.containers;
    while (rec) {
        container_record_t *next = rec->next;
        free(rec);
        rec = next;
    }

    return 0;
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
    int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock_fd);
        return 1;
    }

    if (write(sock_fd, req, sizeof(*req)) != sizeof(*req)) {
        perror("write");
        close(sock_fd);
        return 1;
    }

    control_response_t resp;
    if (read(sock_fd, &resp, sizeof(resp)) != sizeof(resp)) {
        perror("read");
        close(sock_fd);
        return 1;
    }

    close(sock_fd);

    if (req->kind == CMD_PS || req->kind == CMD_LOGS) {
        printf("%s", resp.message);
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

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
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
