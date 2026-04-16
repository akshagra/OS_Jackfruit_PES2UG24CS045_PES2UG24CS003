/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Provided boilerplate:
 *   - device registration and teardown
 *   - timer setup
 *   - RSS helper
 *   - soft-limit and hard-limit event helpers
 *   - ioctl dispatch shell
 *
 * YOUR WORK: Fill in all sections marked // TODO.
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/string.h>
#include <linux/kthread.h>
#include <linux/delay.h>
#include <linux/types.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"
#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1

/* ==============================================================
 * TODO 1: Define your linked-list node struct.
 *
 * Requirements:
 *   - track PID, container ID, soft limit, and hard limit
 *   - remember whether the soft-limit warning was already emitted
 *   - include `struct list_head` linkage
 * ============================================================== */

struct monitored_entry {
    struct list_head list;
    pid_t pid;
    char container_id[MONITOR_NAME_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    bool soft_limit_warned;
};

/* ==============================================================
 * TODO 2: Declare the global monitored list and a lock.
 *
 * Requirements:
 *   - shared across ioctl and timer code paths
 *   - protect insert, remove, and iteration safely
 *
 * You may choose either a mutex or a spinlock, but your README must
 * justify the choice in terms of the code paths you implemented.
 * ============================================================== */

static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(monitored_lock);


/* --- Provided: internal device / timer state --- */
static struct task_struct *monitor_thread;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 *
 * Returns the Resident Set Size in bytes for the given PID,
 * or -1 if the task no longer exists.
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ---------------------------------------------------------------
 * Provided: soft-limit helper
 *
 * Log a warning when a process exceeds the soft limit.
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: hard-limit helper
 *
 * Kill a process when it exceeds the hard limit.
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Monitor thread - periodically checks tracked entries.
 * --------------------------------------------------------------- */
static int monitor_thread_fn(void *arg)
{
    struct monitored_entry *entry, *tmp;

    while (!kthread_should_stop()) {
        mutex_lock(&monitored_lock);
        list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
            long rss = get_rss_bytes(entry->pid);
            if (rss < 0) {
                /* Process exited, remove entry */
                list_del(&entry->list);
                kfree(entry);
                continue;
            }

            if (!entry->soft_limit_warned && rss > entry->soft_limit_bytes) {
                log_soft_limit_event(entry->container_id, entry->pid, entry->soft_limit_bytes, rss);
                entry->soft_limit_warned = true;
            }

            if (rss > entry->hard_limit_bytes) {
                kill_process(entry->container_id, entry->pid, entry->hard_limit_bytes, rss);
                list_del(&entry->list);
                kfree(entry);
            }
        }
        mutex_unlock(&monitored_lock);

        msleep(CHECK_INTERVAL_SEC * 1000);
    }

    return 0;
}

/* ---------------------------------------------------------------
 * IOCTL Handler
 *
 * Supported operations:
 *   - register a PID with soft + hard limits
 *   - unregister a PID when the runtime no longer needs tracking
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid, req.soft_limit_bytes, req.hard_limit_bytes);

        /* ==============================================================
         * TODO 4: Add a monitored entry.
         *
         * Requirements:
         *   - allocate and initialize one node from req
         *   - validate allocation and limits
         *   - insert into the shared list under the chosen lock
         * ============================================================== */

        struct monitored_entry *entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry) {
            return -ENOMEM;
        }

        if (req.soft_limit_bytes >= req.hard_limit_bytes) {
            kfree(entry);
            return -EINVAL;
        }

        entry->pid = req.pid;
        strncpy(entry->container_id, req.container_id, sizeof(entry->container_id) - 1);
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_limit_warned = false;

        mutex_lock(&monitored_lock);
        list_add(&entry->list, &monitored_list);
        mutex_unlock(&monitored_lock);

        return 0;
    }

    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    /* ==============================================================
     * TODO 5: Remove a monitored entry on explicit unregister.
     *
     * Requirements:
     *   - search by PID, container ID, or both
     *   - remove the matching entry safely if found
     *   - return status indicating whether a matching entry was removed
     * ============================================================== */

    struct monitored_entry *entry, *tmp;
    bool found = false;

    mutex_lock(&monitored_lock);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        if (entry->pid == req.pid &&
            strncmp(entry->container_id, req.container_id, sizeof(entry->container_id)) == 0) {
            list_del(&entry->list);
            kfree(entry);
            found = true;
            break;
        }
    }
    mutex_unlock(&monitored_lock);

    return found ? 0 : -ENOENT;
}

/* --- Provided: file operations --- */
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    /* Start monitor thread */
    monitor_thread = kthread_run(monitor_thread_fn, NULL, "container_monitor");
    if (IS_ERR(monitor_thread)) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        printk(KERN_ERR "[container_monitor] Failed to start monitor thread\n");
        return PTR_ERR(monitor_thread);
    }

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void)
{
    if (monitor_thread)
        kthread_stop(monitor_thread);

    /* ==============================================================
     * TODO 6: Free all remaining monitored entries.
     *
     * Requirements:
     *   - remove and free every list node safely
     *   - leave no leaked state on module unload
     * ============================================================== */

    struct monitored_entry *entry, *tmp;
    mutex_lock(&monitored_lock);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }
    mutex_unlock(&monitored_lock);

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

/* del_timer_sync prototype declared earlier */

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
