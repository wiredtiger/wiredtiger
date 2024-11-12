#include "wt_internal.h"

__thread WT_CALLTRACK_THREAD wt_calltrack_thread = {
    .nest_level = 0,
    .tnid = 0,
    .pid = 0,
    .is_service_thread = false,
};

WT_CALLTRACK_GLOBAL wt_calltrack_global = {
    .is_running = true,
    .n_flushers_running = 0,
};

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
//     printf("{\"displayTimeUnit\": \"us\", \"traceEvents\": [\n");
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
    char filename[256];
    snprintf(filename, sizeof(filename), "calltrack-%05"SCNuMAX".json", id);
    return fopen(filename, "w");
}

WT_THREAD_RET __wt_calltrack_buf_flusher(void *arg) {
    wt_calltrack_thread.is_service_thread = true;
    int cycles = 0;
    WT_CALLTRACK_THREAD_BUF *buf = ((WT_CALLTRACK_THREAD *)arg)->buf;
    FILE *tracefile = __wt_calltrack_open_tracefile(((WT_CALLTRACK_THREAD *)arg)->tnid);
    fprintf(tracefile, "{\"displayTimeUnit\": \"us\", \"traceEvents\": [\n");
    while (1) {
        if (!__wt_calltrack_can_read(buf)) {
            if (++cycles < 1000) {
                __wt_yield();
                WT_COMPILER_BARRIER();
            } else {
                if (wt_calltrack_thread.nest_level == 0 && !__atomic_load_n(&wt_calltrack_global.is_running, __ATOMIC_ACQUIRE))
                    break;
                int thread_status = pthread_kill((pthread_t)buf->ostid, 0);
                if (thread_status == ESRCH || thread_status == EINVAL) {
                    // fprintf(tracefile, "%"SCNuMAX" %"PRIu64" = %d\n", buf->ostid, buf->tnid, thread_status);
                    // __wt_sleep(1, 0);
                    // continue;
                    break;
                }
                // fprintf(tracefile, "%"SCNuMAX" %"PRIu64" = %d\n", buf->ostid, buf->tnid, thread_status);
                __wt_sleep(0, 1000);
            }
            continue;
        }
        cycles = 0;

        int reader = buf->reader;
        int writer = __atomic_load_n(&buf->writer, __ATOMIC_ACQUIRE);
        while (reader != writer) {
            WT_CALLTRACK_LOG_ENTRY *entry = &buf->entries[reader];
            if (entry->enter) {
                ++wt_calltrack_thread.nest_level;
                fprintf(tracefile, "{\"ts\": %"PRIu64", \"pid\": %"SCNuMAX", \"tid\": %"PRIu64", \"ph\": \"B\", \"name\": \"%s\", \"cat\": \"%s\"},\n",
                    entry->ts, buf->pid, buf->tnid, entry->name, entry->cat);
            } else {
                --wt_calltrack_thread.nest_level;
                fprintf(tracefile, "{\"ts\": %"PRIu64", \"pid\": %"SCNuMAX", \"tid\": %"PRIu64", \"ph\": \"E\", \"args\": {\"<ret>\": \"%"PRId64"\"}},\n",
                    entry->ts, buf->pid, buf->tnid, entry->ret);
            }
            reader = (reader + 1) % WT_CALLTRACK_THREAD_BUF_ENTRIES;
        }
        __atomic_store_n(&buf->reader, reader, __ATOMIC_RELEASE);
        WT_COMPILER_BARRIER();
    }
    free(buf);
    fprintf(tracefile, "{}]}\n");
    fclose(tracefile);
    __atomic_fetch_sub(&wt_calltrack_global.n_flushers_running, 1, __ATOMIC_RELAXED);
    return NULL;
}

