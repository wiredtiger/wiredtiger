#pragma once

#ifndef WT_NOERROR_APPEND
#define WT_NOERROR_APPEND(p, remain, ...)                              \
    do {                                                               \
        size_t __len;                                                  \
        WT_UNUSED(__wt_snprintf_len_set(p, remain, &__len, __VA_ARGS__)); \
        if (__len > remain)                                            \
            __len = remain;                                            \
        p += __len;                                                    \
        remain -= __len;                                               \
    } while (0)
#endif

typedef struct __wt_calltrack {
    int nest_level;
    char _indent_buf[4096];
    char _session_info_buf[4096];
} WT_CALLTRACK;

extern __thread WT_CALLTRACK wt_calltrack;

#define __WT_SET_INDENT() do { \
    memset(wt_calltrack._indent_buf, ' ', wt_calltrack.nest_level*2);    \
    wt_calltrack._indent_buf[wt_calltrack.nest_level*2] = 0;    \
} while(0)

#define PRtimespec "%" PRIuMAX ".%06" PRIuMAX
#define PRtimespec_arg(ts) (uintmax_t)(ts).tv_sec, (uintmax_t)(ts).tv_nsec / WT_THOUSAND

#define __WT_SET_SESSION_INFO(__ts__) do { \
    if (__session__) { \
        /* See __eventv() */    \
        \
        char *__p__ = wt_calltrack._session_info_buf;    \
        size_t __remain__ = sizeof(wt_calltrack._session_info_buf);    \
        \
        { /* Timestamp and thread id. */    \
            char __tid__[128];    \
            WT_UNUSED(__wt_thread_str(__tid__, sizeof(__tid__)));    \
            WT_NOERROR_APPEND(__p__, __remain__, "[" PRtimespec "][%s]", PRtimespec_arg(__ts__), __tid__);    \
        }    \
        \
        WT_NOERROR_APPEND(__p__, __remain__, "(%s)", F_ISSET(__session__, WT_SESSION_INTERNAL) ? "INTERNAL" : "APP"); \
        \
        {   /* Session dhandle name. */    \
            const char *__prefix__ = __session__->dhandle == NULL ? NULL : __session__->dhandle->name;    \
            if (__prefix__ != NULL) WT_NOERROR_APPEND(__p__, __remain__, ", %s", __prefix__);    \
            /* Session name. */    \
            if ((__prefix__ = __session__->name) != NULL) WT_NOERROR_APPEND(__p__, __remain__, ", %s", __prefix__);    \
        }    \
    } else {    \
        wt_calltrack._session_info_buf[0] = 0;    \
    }    \
} while(0)

#define __WT_SET_NOSESSION_INFO(__ts__) do { \
    { \
        /* See __eventv() */    \
        \
        char *__p__ = wt_calltrack._session_info_buf;    \
        size_t __remain__ = sizeof(wt_calltrack._session_info_buf);    \
        \
        { /* Timestamp and thread id. */    \
            char __tid__[128];    \
            WT_UNUSED(__wt_thread_str(__tid__, sizeof(__tid__)));    \
            WT_NOERROR_APPEND(__p__, __remain__, "[" PRtimespec "][%s]", PRtimespec_arg(__ts__), __tid__);    \
        }    \
    }    \
} while(0)

static WT_INLINE void __wt_timespec_diff(const struct timespec *start, const struct timespec *end,
    struct timespec *diff)
{
    diff->tv_sec = end->tv_sec - start->tv_sec;
    diff->tv_nsec = end->tv_nsec - start->tv_nsec;
    if (diff->tv_nsec < 0) {
        diff->tv_sec--;
        diff->tv_nsec += 1000000000;
    }
}

