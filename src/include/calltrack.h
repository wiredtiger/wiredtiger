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
    struct {
        char _indent_buf[4096];
        char _session_info_buf[4096];
        char _tid[128];
    };
    int nest_level;
} WT_CALLTRACK;
extern __thread WT_CALLTRACK wt_calltrack;

typedef struct __wt_calltrack_global {
    struct timespec start;
} WT_CALLTRACK_GLOBAL;
extern WT_CALLTRACK_GLOBAL wt_calltrack_global;

extern void __attribute__((constructor)) __wt_calltrack_init_once(void);

#define PRtimespec "%" PRIuMAX ".%06" PRIuMAX
#define PRtimespecFmt "%4" PRIuMAX ".%06" PRIuMAX
#define PRtimespec_arg(ts) (uintmax_t)(ts).tv_sec, (uintmax_t)(ts).tv_nsec / WT_THOUSAND

static WT_INLINE void
__wt_timespec_diff(const struct timespec *start, const struct timespec *end, struct timespec *diff)
{
    diff->tv_sec = end->tv_sec - start->tv_sec;
    diff->tv_nsec = end->tv_nsec - start->tv_nsec;
    if (diff->tv_nsec < 0) {
        diff->tv_sec--;
        diff->tv_nsec += 1000000000;
    }
}

// static WT_INLINE int64_t
// __wt_elapsed_usec(void)
// {
//     struct timespec ts, dt;
//     __wt_epoch_raw(NULL, &ts);
//     __wt_timespec_diff(&wt_calltrack_global.start, &ts, &dt);
//     return dt.tv_sec * WT_THOUSAND + dt.tv_nsec / WT_THOUSAND;
// }

// static WT_INLINE void
// __wt_elapsed_usec_fmt(char *buf, size_t len)
// {
//     struct timespec ts, dt;
//     __wt_epoch_raw(NULL, &ts);
//     __wt_timespec_diff(&wt_calltrack_global.start, &ts, &dt);
//     WT_NOERROR_APPEND(buf, len, "%" PRIuMAX ".%06" PRIuMAX, PRtimespec_arg(dt));
// }

// static WT_INLINE void
// __wt_elapsed_usec_append(char **buf, size_t *len)
// {
//     struct timespec ts, dt;
//     __wt_epoch_raw(NULL, &ts);
//     __wt_timespec_diff(&wt_calltrack_global.start, &ts, &dt);
//     WT_NOERROR_APPEND(*buf, *len, "%" PRIuMAX ".%06" PRIuMAX, PRtimespec_arg(dt));
// }

static WT_INLINE void
__wt_set_indent(int indent)
{
    memset(wt_calltrack._indent_buf, ' ', indent);
    wt_calltrack._indent_buf[indent] = 0;
}

static WT_INLINE void
__wt_set_session_info(WT_SESSION_IMPL *session, const struct timespec ts)
{
    /* See __eventv() */

    char *p = wt_calltrack._session_info_buf;
    size_t remain = sizeof(wt_calltrack._session_info_buf);

    /* Timestamp and thread id. */
    WT_UNUSED(__wt_thread_str(wt_calltrack._tid, sizeof(wt_calltrack._tid)));
    WT_NOERROR_APPEND(p, remain, "[" PRtimespec "][%s]", PRtimespec_arg(ts), wt_calltrack._tid);

    if (session) {
        /* Session info */
        WT_NOERROR_APPEND(
          p, remain, "(%s)", F_ISSET(session, WT_SESSION_INTERNAL) ? "INTERNAL" : "APP");

        /* Session dhandle name. */
        const char *prefix = session->dhandle == NULL ? NULL : session->dhandle->name;
        if (prefix != NULL)
            WT_NOERROR_APPEND(p, remain, ", %s", prefix);

        /* Session name. */
        if ((prefix = session->name) != NULL)
            WT_NOERROR_APPEND(p, remain, ", %s", prefix);
    }
}

#define __WT_CALL_WRAP_(FUNCNAME, CALL, SESSION, RET_INIT, RET_FMT, RET_ARG, RET_RET)       \
    do {                                                                                    \
        WT_SESSION_IMPL *__session__ = SESSION;                                             \
                                                                                            \
        struct timespec __ts_elapsed__;                                                     \
        struct timespec __ts_start__, __ts_end__, __ts_diff__;                              \
        struct timespec __tt_start__, __tt_end__, __tt_diff__;                              \
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &__tt_start__);                              \
        clock_gettime(CLOCK_REALTIME, &__ts_start__);                                       \
        __wt_timespec_diff(&wt_calltrack_global.start, &__ts_start__, &__ts_elapsed__);     \
        /* __wt_epoch_raw(NULL, &__ts_start__); */                                          \
                                                                                            \
        ++wt_calltrack.nest_level;                                                          \
                                                                                            \
        __wt_set_indent(wt_calltrack.nest_level * 2);                                       \
        __wt_set_session_info(__session__, __ts_start__);                                   \
        printf(PRtimespecFmt " %3d%s%s ...                       \t\t%s: %s:%d: %s\n",      \
          PRtimespec_arg(__ts_elapsed__),                                                   \
          wt_calltrack.nest_level, wt_calltrack._indent_buf,                                \
          FUNCNAME,                                                                         \
          wt_calltrack._session_info_buf, __FILE__, __LINE__, __PRETTY_FUNCTION__);         \
                                                                                            \
        RET_INIT CALL;                                                                      \
                                                                                            \
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &__tt_end__);                                \
        clock_gettime(CLOCK_REALTIME, &__ts_end__);                                         \
        /* __wt_epoch_raw(NULL, &__ts_end__); */                                            \
        __wt_timespec_diff(&__ts_start__, &__ts_end__, &__ts_diff__);                       \
        __wt_timespec_diff(&__tt_start__, &__tt_end__, &__tt_diff__);                       \
        __wt_timespec_diff(&wt_calltrack_global.start, &__ts_end__, &__ts_elapsed__);       \
                                                                                            \
        __wt_set_indent(wt_calltrack.nest_level * 2);                                       \
        __wt_set_session_info(__session__, __ts_end__);                                     \
        printf(PRtimespecFmt " %3d%s%s " RET_FMT "  (" PRtimespec " / " PRtimespec ")\t\t%s: %s:%d: %s\n", \
          PRtimespec_arg(__ts_elapsed__),                                                   \
          wt_calltrack.nest_level, wt_calltrack._indent_buf,                                \
          FUNCNAME, RET_ARG, PRtimespec_arg(__tt_diff__), PRtimespec_arg(__ts_diff__),      \
          wt_calltrack._session_info_buf, __FILE__, __LINE__, __PRETTY_FUNCTION__);         \
                                                                                            \
        --wt_calltrack.nest_level;                                                          \
        RET_RET;                                                                            \
    } while (0)

#define __WT_CALL_WRAP(FUNCNAME, CALL, SESSION) \
    __WT_CALL_WRAP_(FUNCNAME, CALL, SESSION, int __ret__ =, "= %d", __ret__, return __ret__)

#define __WT_CALL_WRAP_NORET(FUNCNAME, CALL, SESSION) \
    __WT_CALL_WRAP_(FUNCNAME, CALL, SESSION,              ,   "%s",   "   ",   /* no ret */)

#define __WT_CALL_WRAP_RET(FUNCNAME, CALL, SESSION, RETTYPE, FMT, FMTARG) \
    __WT_CALL_WRAP_(FUNCNAME, CALL, SESSION, RETTYPE __ret__ =,    FMT,  FMTARG, return __ret__)

