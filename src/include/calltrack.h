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

typedef struct __wt_calltrack {
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
        uint64_t tid;
    };
    /* Live data */
    int nest_level;
} WT_CALLTRACK;
extern __thread WT_CALLTRACK wt_calltrack;

typedef struct __wt_calltrack_global {
    uint64_t tstart;
    uint64_t tid;
} WT_CALLTRACK_GLOBAL;
extern WT_CALLTRACK_GLOBAL wt_calltrack_global;

static WT_INLINE void
__wt_set_indent(int indent)
{
    memset(wt_calltrack._indent_buf, ' ', indent);
    wt_calltrack._indent_buf[indent] = 0;
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

    char *p = wt_calltrack._session_info_buf;
    size_t remain = sizeof(wt_calltrack._session_info_buf);

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

#define __WT_CALL_WRAP1_(FUNCNAME, CALL, SESSION, RET_INIT, RET_FMT, RET_ARG, RET_RET)      \
    do {                                                                                    \
        WT_SESSION_IMPL *__session__ = SESSION;                                             \
                                                                                            \
        if (!wt_calltrack.tid_str[0])                                                       \
            WT_UNUSED(__wt_thread_str(wt_calltrack.tid_str, sizeof(wt_calltrack.tid_str))); \
                                                                                            \
        uint64_t __ts_start__, __ts_end__;                                                  \
                                                                                            \
        ++wt_calltrack.nest_level;                                                          \
                                                                                            \
        __wt_set_indent(wt_calltrack.nest_level * 2);                                       \
        __wt_set_session_info(__session__);                                                 \
        __ts_start__ = __wt_clock(NULL);                                                    \
        printf("%11.6lf %3d%s%s%-27s\t\t[%s]%s: %s:%d: %s\n",                               \
          __wt_clock_to_sec_d(__ts_start__, wt_calltrack_global.tstart),                    \
          wt_calltrack.nest_level, wt_calltrack._indent_buf,                                \
          FUNCNAME, wt_calltrack._args_buf,                                                 \
          wt_calltrack.tid_str,                                                             \
          wt_calltrack._session_info_buf, __FILE__, __LINE__, __PRETTY_FUNCTION__);         \
                                                                                            \
        RET_INIT CALL;                                                                      \
                                                                                            \
        __ts_end__ = __wt_clock(NULL);                                                      \
        __wt_set_indent(wt_calltrack.nest_level * 2);                                       \
        __wt_set_session_info(__session__);                                                 \
        printf("%11.6lf %3d%s%s " RET_FMT "  (%.6lf)\t\t[%s]%s: %s:%d: %s\n",               \
          __wt_clock_to_sec_d(__ts_end__, wt_calltrack_global.tstart),                      \
          wt_calltrack.nest_level, wt_calltrack._indent_buf,                                \
          FUNCNAME, RET_ARG, __wt_clock_to_sec_d(__ts_end__, __ts_start__),                 \
          wt_calltrack.tid_str,                                                             \
          wt_calltrack._session_info_buf, __FILE__, __LINE__, __PRETTY_FUNCTION__);         \
                                                                                            \
        --wt_calltrack.nest_level;                                                          \
        RET_RET;                                                                            \
    } while (0)

#define __WT_CALL_WRAP2_(FUNCNAME, CALL, SESSION, RET_INIT, RET_FMT, RET_ARG, RET_RET)      \
    do {                                                                                    \
        WT_SESSION_IMPL *__session__ = SESSION;                                             \
                                                                                            \
        if (!wt_calltrack.pid) {                                                            \
            wt_calltrack.pid = (uintmax_t)getpid();                                         \
            wt_calltrack.tid = __wt_atomic_fetch_add64(&wt_calltrack_global.tid, 1);        \
            /* __wt_thread_id(&wt_calltrack.tid); */                                        \
        }                                                                                   \
                                                                                            \
        uint64_t __ts__;                                                                    \
                                                                                            \
        ++wt_calltrack.nest_level;                                                          \
                                                                                            \
        __wt_set_indent(wt_calltrack.nest_level * 2);                                       \
        __wt_set_session_info(__session__);                                                 \
        __ts__ = __wt_clock(NULL);                                                          \
        printf("{\"ts\": %"PRIu64", \"pid\": %"SCNuMAX", \"tid\": %"PRIu64", \"ph\": \"B\",%s\"name\": \"%s\", \"cat\": \"%s\", \"args\": {\"session in\": \"%s\", \"args\": \"%s\"}},\n", \
          __wt_clock_to_usec(__ts__, wt_calltrack_global.tstart),                           \
          wt_calltrack.pid, wt_calltrack.tid,                                               \
          wt_calltrack._indent_buf,                                                         \
          FUNCNAME,                                                                         \
          __FILE__,                                                                         \
          wt_calltrack._session_info_buf,                                                   \
          wt_calltrack._args_buf                                                            \
          );                                                                                \
                                                                                            \
        RET_INIT CALL;                                                                      \
                                                                                            \
        __ts__ = __wt_clock(NULL);                                                          \
        __wt_set_indent(wt_calltrack.nest_level * 2);                                       \
        __wt_set_session_info(__session__);                                                 \
        printf("{\"ts\": %"PRIu64", \"pid\": %"SCNuMAX", \"tid\": %"PRIu64", \"ph\": \"E\",%s\"name\": \"%s\", \"args\": {\"session out\": \"%s\", \"<ret>\": \"" RET_FMT "\"}},\n", \
          __wt_clock_to_usec(__ts__, wt_calltrack_global.tstart),                           \
          wt_calltrack.pid, wt_calltrack.tid,                                               \
          wt_calltrack._indent_buf,                                                         \
          FUNCNAME,                                                                         \
          wt_calltrack._session_info_buf,                                                   \
          RET_ARG                                                                           \
          );                                                                                \
                                                                                            \
        --wt_calltrack.nest_level;                                                          \
        RET_RET;                                                                            \
    } while (0)

#define __WT_CALL_WRAP_ __WT_CALL_WRAP2_

#define __WT_CALL_WRAP(FUNCNAME, CALL, SESSION) \
    __WT_CALL_WRAP_(FUNCNAME, CALL, SESSION, int __ret__ =, "= %d", __ret__, return __ret__)

#define __WT_CALL_WRAP_NORET(FUNCNAME, CALL, SESSION) \
    __WT_CALL_WRAP_(FUNCNAME, CALL, SESSION,              ,   "%s",   "   ",   /* no ret */)

#define __WT_CALL_WRAP_RET(FUNCNAME, CALL, SESSION, RETTYPE, FMT, FMTARG) \
    __WT_CALL_WRAP_(FUNCNAME, CALL, SESSION, RETTYPE __ret__ =,    FMT,  FMTARG, return __ret__)

