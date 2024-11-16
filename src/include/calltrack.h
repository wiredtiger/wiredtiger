#pragma once

#ifndef WT_NOERROR_APPEND
#define WT_NOERROR_APPEND(p, remain, ...)                                 \
    do {                                                                  \
        size_t __len;                                                     \
        WT_UNUSED(__wt_snprintf_len_set(p, remain, &__len, __VA_ARGS__)); \
        if (__len > remain)                                               \
            __len = remain;                                               \
        p += __len;                                                       \
        remain -= __len;                                                  \
    } while (0)
#endif

typedef struct __wt_calltrack_log_entry {
    uint64_t ts;
    int64_t ret;
    const char *name;
    const char *cat;
    int enter;
//    char args[96]; /* 128 - 32 */
} WT_CALLTRACK_LOG_ENTRY;

#ifdef __linux__
pid_t gettid(void);
#endif

typedef struct __wt_calltrack_thread_buf WT_CALLTRACK_THREAD_BUF;
struct __wt_calltrack_thread_buf {
    int writer, reader;
    uintmax_t pid;
    uintmax_t ostid;
#ifdef __linux__
    pid_t linux_tid;
#endif
    uint64_t tnid;
#define WT_CALLTRACK_THREAD_BUF_ENTRIES (10*1024*1024 - 5*8)
    WT_CALLTRACK_LOG_ENTRY entries[WT_CALLTRACK_THREAD_BUF_ENTRIES];
};

typedef struct __wt_calltrack_thread {
    /* Temporary buffers for call wrapper */
    struct {
        char _indent_buf[4096];
        char _session_info_buf[4096];
        char _args_buf[4096];
        // char _tid[128];
    };
    /* Permanent thread data */
    struct {
        char tid_str[128];
        uintmax_t pid;
        uintmax_t ostid;
#ifdef __linux__
    pid_t linux_tid;
#endif
        uint64_t tnid;
        bool is_service_thread;
        WT_CALLTRACK_THREAD_BUF *buf;
    };
    /* Live data */
    int nest_level;
} WT_CALLTRACK_THREAD;
extern __thread WT_CALLTRACK_THREAD wt_calltrack_thread;

typedef struct __wt_calltrack_global {
    bool enabled;
    uint64_t tstart;
    uint64_t tnid;
    bool is_running;
    uint64_t n_flushers_running;
} WT_CALLTRACK_GLOBAL;
extern WT_CALLTRACK_GLOBAL wt_calltrack_global;

void wiredtiger_calltrack_set(bool enable, int memorder);

static WT_INLINE void
__wt_set_indent(int indent)
{
    if (indent < 0) indent = 0;
    if (indent > 0) memset(wt_calltrack_thread._indent_buf, ' ', (size_t)indent);
    wt_calltrack_thread._indent_buf[indent] = 0;
}

/*
 * __wt_clock_to_sec_d --
 *     Convert from clock ticks to nanoseconds.
 */
static WT_INLINE double
__wt_clock_to_sec_d(uint64_t end, uint64_t begin)
{
    double clock_diff;

    /*
     * If the ticks were reset, consider it an invalid check and just return zero as the time
     * difference because we cannot compute anything meaningful.
     */
    if (end < begin)
        return (0.);
    clock_diff = (double)(end - begin);
    return (clock_diff / __wt_process.tsc_nsec_ratio / 1.e9);
}

/*
 * __wt_clock_to_nsec --
 *     Convert from clock ticks to microoseconds.
 */
static WT_INLINE uint64_t
__wt_clock_to_usec(uint64_t end, uint64_t begin)
{
    double clock_diff;

    /*
     * If the ticks were reset, consider it an invalid check and just return zero as the time
     * difference because we cannot compute anything meaningful.
     */
    if (end < begin)
        return (0);
    clock_diff = (double)(end - begin);
    return ((uint64_t)(clock_diff / __wt_process.tsc_nsec_ratio / 1.e3));
}

static WT_INLINE void
__wt_set_session_info(WT_SESSION_IMPL *session)
{
    /* See __eventv() */

    char *p = wt_calltrack_thread._session_info_buf;
    size_t remain = sizeof(wt_calltrack_thread._session_info_buf);

    if (session) {
        const char *prefix;

        /* Session info */
        WT_NOERROR_APPEND(
          p, remain, "(%s)", F_ISSET(session, WT_SESSION_INTERNAL) ? "INTERNAL" : "APP");

        /* Session name. */
        if ((prefix = session->name) != NULL)
            WT_NOERROR_APPEND(p, remain, ", %s", prefix);

        /* Session dhandle name. */
        prefix = session->dhandle == NULL ? NULL : session->dhandle->name;
        if (prefix != NULL)
            WT_NOERROR_APPEND(p, remain, ", %s", prefix);
    }
}

#define __WT_CALL_WRAP_IMPL_TXT(FUNCNAME, CALL, SESSION, RET_INIT, RET_FMT, RET_ARG, RET_RET)      \
    do {                                                                                    \
        WT_SESSION_IMPL *__session__ = SESSION;                                             \
                                                                                            \
        if (!wt_calltrack_thread.tid_str[0])                                                       \
            WT_UNUSED(__wt_thread_str(wt_calltrack_thread.tid_str, sizeof(wt_calltrack_thread.tid_str))); \
                                                                                            \
        uint64_t __ts_start__, __ts_end__;                                                  \
                                                                                            \
        ++wt_calltrack_thread.nest_level;                                                          \
                                                                                            \
        __wt_set_indent(wt_calltrack_thread.nest_level * 2);                                       \
        __wt_set_session_info(__session__);                                                 \
        __ts_start__ = __wt_clock(NULL);                                                    \
        printf("%11.6lf %3d%s%s%-27s\t\t[%s]%s: %s:%d: %s\n",                               \
          __wt_clock_to_sec_d(__ts_start__, wt_calltrack_global.tstart),                    \
          wt_calltrack_thread.nest_level, wt_calltrack_thread._indent_buf,                                \
          FUNCNAME, wt_calltrack_thread._args_buf,                                                 \
          wt_calltrack_thread.tid_str,                                                             \
          wt_calltrack_thread._session_info_buf, __FILE__, __LINE__, __PRETTY_FUNCTION__);         \
                                                                                            \
        RET_INIT CALL;                                                                      \
                                                                                            \
        __ts_end__ = __wt_clock(NULL);                                                      \
        __wt_set_indent(wt_calltrack_thread.nest_level * 2);                                       \
        __wt_set_session_info(__session__);                                                 \
        printf("%11.6lf %3d%s%s " RET_FMT "  (%.6lf)\t\t[%s]%s: %s:%d: %s\n",               \
          __wt_clock_to_sec_d(__ts_end__, wt_calltrack_global.tstart),                      \
          wt_calltrack_thread.nest_level, wt_calltrack_thread._indent_buf,                                \
          FUNCNAME, RET_ARG, __wt_clock_to_sec_d(__ts_end__, __ts_start__),                 \
          wt_calltrack_thread.tid_str,                                                             \
          wt_calltrack_thread._session_info_buf, __FILE__, __LINE__, __PRETTY_FUNCTION__);         \
                                                                                            \
        --wt_calltrack_thread.nest_level;                                                          \
        RET_RET;                                                                            \
    } while (0)

#ifdef __linux__
#define __WT_GET_LINUX_TID(DST) DST = gettid()
#else
#define __WT_GET_LINUX_TID(DST)
#endif

#define __WT_CALL_WRAP_IMPL_GRAPH(FUNCNAME, CALL, SESSION, RET_INIT, RET_FMT, RET_ARG, RET_RET)      \
    do {                                                                                    \
        WT_SESSION_IMPL *__session__ = SESSION;                                             \
                                                                                            \
        if (!wt_calltrack_thread.pid) {                                                            \
            wt_calltrack_thread.pid = (uintmax_t)getpid();                                         \
            wt_calltrack_thread.tnid = __wt_atomic_fetch_add64(&wt_calltrack_global.tnid, 1);        \
            __wt_thread_id(&wt_calltrack_thread.ostid);                                         \
            __WT_GET_LINUX_TID(wt_calltrack_thread.linux_tid);                              \
        }                                                                                   \
                                                                                            \
        uint64_t __ts__;                                                                    \
                                                                                            \
        ++wt_calltrack_thread.nest_level;                                                          \
                                                                                            \
        __wt_set_indent(wt_calltrack_thread.nest_level * 2);                                       \
        __wt_set_session_info(__session__);                                                 \
        __ts__ = __wt_clock(NULL);                                                          \
        printf("{\"ts\": %"PRIu64", \"pid\": %"SCNuMAX", \"tid\": %"PRIu64", \"ph\": \"B\",%s\"name\": \"%s\", \"cat\": \"%s\", \"args\": {\"session in\": \"%s\", \"args\": \"%s\"}},\n", \
          __wt_clock_to_usec(__ts__, wt_calltrack_global.tstart),                           \
          wt_calltrack_thread.pid, wt_calltrack_thread.tnid,                                               \
          wt_calltrack_thread._indent_buf,                                                         \
          FUNCNAME,                                                                         \
          __FILE__,                                                                         \
          wt_calltrack_thread._session_info_buf,                                                   \
          wt_calltrack_thread._args_buf                                                            \
          );                                                                                \
                                                                                            \
        RET_INIT CALL;                                                                      \
                                                                                            \
        __ts__ = __wt_clock(NULL);                                                          \
        __wt_set_indent(wt_calltrack_thread.nest_level * 2);                                       \
        __wt_set_session_info(__session__);                                                 \
        printf("{\"ts\": %"PRIu64", \"pid\": %"SCNuMAX", \"tid\": %"PRIu64", \"ph\": \"E\",%s\"name\": \"%s\", \"args\": {\"session out\": \"%s\", \"<ret>\": \"" RET_FMT "\"}},\n", \
          __wt_clock_to_usec(__ts__, wt_calltrack_global.tstart),                           \
          wt_calltrack_thread.pid, wt_calltrack_thread.tnid,                                               \
          wt_calltrack_thread._indent_buf,                                                         \
          FUNCNAME,                                                                         \
          wt_calltrack_thread._session_info_buf,                                                   \
          RET_ARG                                                                           \
          );                                                                                \
                                                                                            \
        --wt_calltrack_thread.nest_level;                                                          \
        RET_RET;                                                                            \
    } while (0)

#define __WT_CALL_WRAP_ __WT_CALL_WRAP_IMPL_GRAPH

#define __WT_CALL_WRAP(FUNCNAME, CALL, SESSION) \
    __WT_CALL_WRAP_(FUNCNAME, CALL, SESSION, int __ret__ =, "= %d", __ret__, return __ret__)

#define __WT_CALL_WRAP_NORET(FUNCNAME, CALL, SESSION) \
    __WT_CALL_WRAP_(FUNCNAME, CALL, SESSION,              ,   "%s",   "   ",   /* no ret */)

#define __WT_CALL_WRAP_RET(FUNCNAME, CALL, SESSION, RETTYPE, FMT, FMTARG) \
    __WT_CALL_WRAP_(FUNCNAME, CALL, SESSION, RETTYPE __ret__ =,    FMT,  FMTARG, return __ret__)

/*****************************************************************************/

static WT_INLINE void __wt_calltrack_init_thread(void);
static WT_INLINE void
__wt_calltrack_init_thread(void) {
    // if (wt_calltrack_thread.pid)
    //     return;
    wt_calltrack_thread.pid = (uintmax_t)getpid();
    wt_calltrack_thread.tnid = __wt_atomic_fetch_add64(&wt_calltrack_global.tnid, 1);
    __wt_thread_id(&wt_calltrack_thread.ostid);
    __WT_GET_LINUX_TID(wt_calltrack_thread.linux_tid);
}

WT_THREAD_RET __wt_calltrack_buf_flusher(void *arg);
static WT_INLINE void __wt_calltrack_init_thread_and_buf(void);
static WT_INLINE void
__wt_calltrack_init_thread_and_buf(void) {
    // if (wt_calltrack_thread.pid)
    //     return;
    wt_calltrack_thread.buf = malloc(WT_CALLTRACK_THREAD_BUF_ENTRIES * sizeof(WT_CALLTRACK_LOG_ENTRY));
    if (!wt_calltrack_thread.buf) {
        fprintf(stderr, "Failed to allocate buffer\n");
        abort();
    }
    wt_calltrack_thread.buf->writer = wt_calltrack_thread.buf->reader = 0;
    wt_calltrack_thread.buf->pid = wt_calltrack_thread.pid = (uintmax_t)getpid();
    wt_calltrack_thread.buf->tnid = wt_calltrack_thread.tnid = __wt_atomic_fetch_add64(&wt_calltrack_global.tnid, 1);
    __wt_thread_id(&wt_calltrack_thread.ostid);
    wt_calltrack_thread.buf->ostid = wt_calltrack_thread.ostid;
#ifdef __linux__
    wt_calltrack_thread.linux_tid = wt_calltrack_thread.buf->linux_tid = gettid();
#endif
    __atomic_fetch_add(&wt_calltrack_global.n_flushers_running, 1, __ATOMIC_RELAXED);
    pthread_t thread;
    WT_FULL_BARRIER();
    pthread_create(&thread, NULL, __wt_calltrack_buf_flusher, wt_calltrack_thread.buf);
    pthread_detach(thread);
}

static WT_INLINE void __wt_calltrack_wait_for_write(void);
static WT_INLINE void
__wt_calltrack_wait_for_write(void) {
    int reader;
    int max_reader = (wt_calltrack_thread.buf->writer + 1) % WT_CALLTRACK_THREAD_BUF_ENTRIES;
    uint64_t count = 0;
retry:
    /* If next reader is equal to writer, the buffer is full */
    reader = __atomic_load_n(&wt_calltrack_thread.buf->reader, __ATOMIC_ACQUIRE);
    if (reader == max_reader) {
        ++count;
        wt_calltrack_thread.is_service_thread = true;
        if (count < 1000)
            __wt_yield();
        else if (count < 2000)
            __wt_sleep(0, count - 1000);
        else
            __wt_sleep(0, 1000);
        wt_calltrack_thread.is_service_thread = false;
        goto retry;
    }
}

static WT_INLINE void __wt_calltrack_write_entry(uint64_t ts, int64_t ret, const char *name, const char *cat, int enter /*, const char *fmt, ... */);
static WT_INLINE void
__wt_calltrack_write_entry(uint64_t ts, int64_t ret, const char *name, const char *cat, int enter /*, const char *fmt, ... */) {
    __wt_calltrack_wait_for_write();
    WT_CALLTRACK_LOG_ENTRY *entry = &wt_calltrack_thread.buf->entries[wt_calltrack_thread.buf->writer];
    entry->ts = __wt_clock_to_usec(ts, wt_calltrack_global.tstart);
    entry->ret = ret;
    entry->name = name;
    entry->cat = cat;
    entry->enter = enter;
    // WT_COMPILER_BARRIER();
    __atomic_store_n(&wt_calltrack_thread.buf->writer, (wt_calltrack_thread.buf->writer + 1) % WT_CALLTRACK_THREAD_BUF_ENTRIES, __ATOMIC_RELEASE);
    // WT_COMPILER_BARRIER();
}

#define __WT_CALL_WRAP_IMPL_BUF_GRAPH(FUNCNAME, CALL, SESSION, RET_INIT, RET_VAL, RET_RET)  \
    if (wt_calltrack_thread.is_service_thread ||                                            \
             !__atomic_load_n(&wt_calltrack_global.enabled, __ATOMIC_RELAXED)) {            \
        RET_INIT CALL;                                                                      \
        RET_RET;                                                                            \
    } else {                                                                                \
        if (!wt_calltrack_thread.pid) __wt_calltrack_init_thread_and_buf();                 \
        __wt_calltrack_write_entry(__wt_clock(NULL), 0, FUNCNAME, __FILE__, 1);             \
        RET_INIT CALL;                                                                      \
        __wt_calltrack_write_entry(__wt_clock(NULL), RET_VAL, FUNCNAME, __FILE__, 0);       \
        RET_RET;                                                                            \
    }
