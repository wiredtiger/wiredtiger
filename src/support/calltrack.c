#include "wt_internal.h"

__thread WT_CALLTRACK_THREAD wt_calltrack_thread = {
    .nest_level = 0,
    .tnid = 0,
    .pid = 0,
    .is_service_thread = false,
    .cached_reader_pos = 0,
};

WT_CALLTRACK_GLOBAL wt_calltrack_global = {
    .enabled = true,
    .is_running = true,
    .n_flushers_running = 0,
};

void
wiredtiger_calltrack_set(bool enable, int memorder)
{
    __atomic_store_n(&wt_calltrack_global.enabled, enable, memorder);
}

void __global_calibrate_ticks(void);

void __attribute__((constructor)) __wt_calltrack_init_once(void);
void __attribute__((constructor))
__wt_calltrack_init_once(void)
{
    wt_calltrack_thread.is_service_thread = true;
    __global_calibrate_ticks();
    wt_calltrack_global.tstart = __wt_clock(NULL);
    wt_calltrack_thread.is_service_thread = false;
}


// void __attribute__((constructor)) __wt_calltrack_init_tracefile(void);
// void __attribute__((constructor))
// __wt_calltrack_init_tracefile(void)
// {
//     printf("{\"traceEvents\": [\n");
// }

// void __attribute__((destructor)) __wt_calltrack_deinit_tracefile(void);
// void __attribute__((destructor))
// __wt_calltrack_deinit_tracefile(void)
// {
//     /* printf("]\n}\n"); */
//     printf("{}]}\n");
// }

void __attribute__((destructor)) __wt_calltrack_deinit_flushers(void);
void __attribute__((destructor))
__wt_calltrack_deinit_flushers(void)
{
    wt_calltrack_thread.is_service_thread = true;
    __atomic_store_n(&wt_calltrack_global.is_running, false, __ATOMIC_RELEASE);
    for (int i = 0; i < 100 && __atomic_load_n(&wt_calltrack_global.n_flushers_running, __ATOMIC_ACQUIRE) != 0; i++)
        __wt_sleep(0, 1000);
    __wt_sleep(0, 100000);
}

bool __wt_calltrack_can_read(WT_CALLTRACK_THREAD_BUF *buf);
bool
__wt_calltrack_can_read(WT_CALLTRACK_THREAD_BUF *buf)
{
    int writer = __atomic_load_n(&buf->writer, __ATOMIC_ACQUIRE);
    int reader = buf->reader;
    return writer != reader;
}

FILE *__wt_calltrack_open_tracefile(uintmax_t id);
FILE *
__wt_calltrack_open_tracefile(uintmax_t id)
{
    // WT_UNUSED(id);
    // return fopen("/dev/null", "w");
    char filename[256];
    snprintf(filename, sizeof(filename), "calltrack-%05"SCNuMAX".json", id);
    return fopen(filename, "w");
}

#ifdef __linux__
int pthread_tryjoin_np(pthread_t thread, void **retval);
int kill(pid_t pid, int sig);
#else
int pthread_kill(pthread_t, int);
#endif

bool __wt_is_thread_terminated(WT_CALLTRACK_THREAD_BUF *buf);
bool
__wt_is_thread_terminated(WT_CALLTRACK_THREAD_BUF *buf)
{
#ifdef __linux__
    // return pthread_tryjoin_np(buf->ostid, NULL) != EBUSY;
    return kill(buf->linux_tid, 0) != 0;
#else
    int thread_status = pthread_kill((pthread_t)buf->ostid, 0);
    return thread_status == ESRCH || thread_status == EINVAL;
#endif
}

/* If CALLTRACK_INTERLEAVE is defined, then only snapshots at this interval are reported */
// #define CALLTRACK_INTERLEAVE 1000

/* If CALLTRACK_MIN_DURATION is defined, events shorter than this are not reported */
#define CALLTRACK_MIN_DURATION 500

/* Top level calls are always reported */
// #define CALLTRACK_ALWAYS_REPORT_TOPLEVEL


#if defined(CALLTRACK_INTERLEAVE) || defined(CALLTRACK_MIN_DURATION)
#define CALLTRACK_FILTER 1
#endif

#define CT_REPORT_ENTER(entry) \
    fprintf(tracefile, "{\"ts\": %"PRIu64", \"pid\": %"SCNuMAX", \"tid\": %"PRIu64", \"ph\": \"B\", \"name\": \"%s\"},\n", \
        entry->ts, buf->pid, buf->tnid, entry->name)
#define CT_REPORT_LEAVE(entry) \
    fprintf(tracefile, "{\"ts\": %"PRIu64", \"pid\": %"SCNuMAX", \"tid\": %"PRIu64", \"ph\": \"E\", \"args\": {\"<ret>\": \"%"PRId64"\"}},\n", \
        entry->ts, buf->pid, buf->tnid, entry->ret);

WT_THREAD_RET __wt_calltrack_buf_flusher(void *arg) {
    wt_calltrack_thread.is_service_thread = true;
    int cycles = 0;
    WT_CALLTRACK_THREAD_BUF *buf = (WT_CALLTRACK_THREAD_BUF *)arg;
#if defined(CALLTRACK_INTERLEAVE) || defined(CALLTRACK_MIN_DURATION)
#define CALLTRACK_MAX_STACK 100
    WT_CALLTRACK_LOG_ENTRY stack[CALLTRACK_MAX_STACK];
    int last_report_stack_depth = 0;
#endif
#if defined(CALLTRACK_INTERLEAVE)
    uint64_t next_report_ts = 0;
#endif

#define FBUFSZ 4*1024*1024
    char * fbuf = (char *)malloc(FBUFSZ);
    FILE *tracefile = __wt_calltrack_open_tracefile(buf->tnid);
    if (tracefile == NULL) {
        fprintf(stderr, "Failed to open tracefile\n");
        abort();
    }
    setbuffer(tracefile, fbuf, FBUFSZ);
    fprintf(tracefile, "{\"traceEvents\": [\n");
    while (1) {
        if (!__wt_calltrack_can_read(buf)) {
            ++cycles;

            bool is_running = __atomic_load_n(&wt_calltrack_global.is_running, __ATOMIC_ACQUIRE);
            if (wt_calltrack_thread.nest_level == 0 && !is_running)
                break;
            if (__wt_is_thread_terminated(buf)) {
                // fprintf(tracefile, "%"SCNuMAX" %"PRIu64" = %d\n", buf->ostid, buf->tnid, thread_status);
                // __wt_sleep(1, 0);
                // continue;
                break;
            }
            // fprintf(tracefile, "%"SCNuMAX" %"PRIu64" = %d\n", buf->ostid, buf->tnid, thread_status);
            if (cycles != 50)
                __wt_sleep(0, 10000);
            else
                fflush(tracefile);
            if (!is_running && cycles > 3)
                break;

            continue;
        }
        cycles = 0;

        int reader = buf->reader;
        int writer = __atomic_load_n(&buf->writer, __ATOMIC_ACQUIRE);
        while (reader != writer) {
            WT_CALLTRACK_LOG_ENTRY *entry = &buf->entries[reader];
            if (entry->enter) {
                ++wt_calltrack_thread.nest_level;
#ifndef CALLTRACK_FILTER
                CT_REPORT_ENTER(entry);
#else
                if (wt_calltrack_thread.nest_level < CALLTRACK_MAX_STACK) {
                    memcpy(&stack[wt_calltrack_thread.nest_level-1], entry, sizeof(WT_CALLTRACK_LOG_ENTRY));
#if defined(CALLTRACK_INTERLEAVE)
                    if (entry->ts >= next_report_ts) {
                        next_report_ts = entry->ts + CALLTRACK_INTERLEAVE;
                        for (int i = last_report_stack_depth; i < wt_calltrack_thread.nest_level; i++) {
                            WT_CALLTRACK_LOG_ENTRY *entry2 = &stack[i];
                            CT_REPORT_ENTER(entry2);
                        }
                        last_report_stack_depth = wt_calltrack_thread.nest_level;
                    }
#endif
                }
#endif
            } else {
                --wt_calltrack_thread.nest_level;
#ifndef CALLTRACK_FILTER
                CT_REPORT_LEAVE(entry);
#else
#if defined(CALLTRACK_INTERLEAVE)
                if (wt_calltrack_thread.nest_level < last_report_stack_depth) {
                    CT_REPORT_LEAVE(entry);
                    last_report_stack_depth = wt_calltrack_thread.nest_level;
                }
#endif
#if defined(CALLTRACK_MIN_DURATION)
                if (wt_calltrack_thread.nest_level < CALLTRACK_MAX_STACK) {
                    if (
#ifdef CALLTRACK_ALWAYS_REPORT_TOPLEVEL
                        wt_calltrack_thread.nest_level == 0 ||
#endif
                            entry->ts - stack[wt_calltrack_thread.nest_level].ts >= CALLTRACK_MIN_DURATION) {
                        if (last_report_stack_depth <= wt_calltrack_thread.nest_level) {
                            for (int i = last_report_stack_depth; i <= wt_calltrack_thread.nest_level; i++) {
                                WT_CALLTRACK_LOG_ENTRY *entry2 = &stack[i];
                                CT_REPORT_ENTER(entry2);
                            }
                        }
                        CT_REPORT_LEAVE(entry);
                        last_report_stack_depth = wt_calltrack_thread.nest_level;
                    }
                }
#endif
#endif
            }
            reader = (reader + 1) % WT_CALLTRACK_THREAD_BUF_ENTRIES;
        }
        __atomic_store_n(&buf->reader, reader, __ATOMIC_RELEASE);
        WT_COMPILER_BARRIER();
    }
    free(buf);
    fprintf(tracefile, "{}]}\n");
    fclose(tracefile);
    free(fbuf);
    __atomic_fetch_sub(&wt_calltrack_global.n_flushers_running, 1, __ATOMIC_RELAXED);
    return NULL;
}
