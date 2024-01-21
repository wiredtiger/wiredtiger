/*-
 * Copyright (c) 2024-present MongoDB, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

typedef enum {
    WT_FUTEX_WAKE_ONE = 1,
    WT_FUTEX_WAKE_ALL = INT_MAX
} WT_FUTEX_WAKE;


#define __wt_futex_wake_one(ftx) __wt_futex_wake(ftx, WT_FUTEX_WAKE_ONE)
#define __wt_futex_wake_all(ftx) __wt_futex_wake(ftx, WT_FUTEX_WAKE_ALL)


#ifdef __linux__
/*
 * Linux
 * https://man7.org/linux/man-pages/man2/futex.2.html
 */

#include <linux/futex.h>
#include <sys/syscall.h>
#include <time.h>
#include <errno.h>

static inline int 
__wt_futex_wait(void *futexaddr, uint32_t current, uint32_t timeout_ms)
{
    long sysret;
    uint64_t started, elapsed_ms;
    struct timespec timeout;

    started = __wt_clock(NULL);

reenter_wait:
    timeout.tv_sec = (time_t)(timeout_ms / WT_THOUSAND);
    timeout.tv_nsec = (time_t)(timeout_ms % WT_THOUSAND);
    sysret = syscall(SYS_futex, futexaddr, FUTEX_WAIT_PRIVATE, current, &timeout, NULL, 0);

    if ((sysret < 0 && errno == EINTR) || ((sysret == 0) && (*(uint32_t*)futexaddr == current))) {
        /* Interrupted syscall or spurious wakeup. */

        elapsed_ms = __wt_clock_to_nsec(started, __wt_clock(NULL)) / WT_MILLION;
        if (elapsed_ms < timeout_ms) {
            /* Reduce timeout and re-enter syscall. */
            timeout_ms -= elapsed_ms;
            goto reenter_wait;
        }

        /* 
         * Treat a spurious wakeup or interrupt at or after the timeout boundary
         * as if it timedout.
         */
        errno = ETIMEDOUT;
        return (-1);
    }
    else if (sysret < 0 && errno == EAGAIN) {
        /* 
         * Futex value changed before entering wait in the syscall, treat as
         * equivalent to a wakeup.
         */
        return (0);
    }
    return ((int)sysret);
}

static inline int 
__wt_futex_wake(void *futexaddr, WT_FUTEX_WAKE howmany)
{
    long sysret;

    WT_ASSERT(NULL, howmany == WT_FUTEX_WAKE_ONE || howmany == WT_FUTEX_WAKE_ALL);

    sysret = syscall(SYS_futex, futexaddr, FUTEX_WAKE_PRIVATE, howmany, NULL, 0);
    
    /* 
     * Wake will return the number of waiters awoken. Return zero regardless
     * to keep consistency with other platforms that do not provide this information.
     */
    return ((int)((sysret >= 0) ? 0 : sysret));
}

#endif


#ifdef __APPLE__
/*
 * https://opensource.apple.com/source/xnu/xnu-6153.11.26/bsd/sys/ulock.h.auto.html
 * https://opensource.apple.com/source/xnu/xnu-6153.11.26/bsd/kern/sys_ulock.c.auto.html
 */
#endif


#ifdef _WIN64

static inline int 
__wt_futex_wait(void *uaddr, uint32_t current, uint32_t timeout_ms)
{
    _Alignas(8) uint32_t compare_value = current;
    BOOL success;
    DWORD error;
    uint64_t started, elapsed_ms;
    uint32_t value_on_wake;

    started = __wt_clock(NULL);
try_again:
    success = WaitOnAddress(uaddr, &compare, sizeof(compare), timeout_ms);
    if (success == TRUE) {
        WT_READ_ONCE(value_on_wake, *(uint32_t*)uaddr);
        if (value_on_wake == compare) {
            elapsed_ms = __wt_clock_to_nsec(started, __wt_clock(NULL)) / WT_MILLION;
            WT_ASSERT(NULL, 0);
            goto try_again;
        }
    }
    return (0);
}

static inline int 
__wt_futex_wake(void *uaddr, uint32_t value, WT_FUTEX_WAKE howmany)
{
    WT_ASSERT(NULL, howmany == WT_FUTEX_WAKE_ONE || howmany == WT_FUTEX_WAKE_ALL);
    WT_PUBLISH(*(uint32_t*)uaddr, value);
    if (howmany == WT_FUTEX_WAKE_ONE)
        WakeByAddressSingle(uaddr);
    else if (howmany == WT_FUTEX_WAKE_ALL)
        WakeByAddressAll(uaddr);
    else {
        errno = EINVAL;
        return (-1);
    }
    return (0);
}

#endif

