/*-
 * Copyright (c) 2024-present MongoDB, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
#include <sys/errno.h>

#define PRIVATE 1
#include <ulock.h>
#undef PRIVATE

/*
 * __wt_futex_wait --
 *     Wait on the futex. The timeout is in microseconds and MUST be greater than zero.
 */
int
__wt_futex_wait(
  volatile WT_FUTEX_WORD *addr, WT_FUTEX_WORD expected, time_t usec, WT_FUTEX_WORD *wake_valp)
{
    WT_DECL_RET;
    uint64_t nsec;

    /* Check for overflow? */
    nsec = (uint64_t)usec * 1000;

    __atomic_store_n(wake_valp, expected, __ATOMIC_RELAXED);
    ret = __ulock_wait2(UL_COMPARE_AND_WAIT_SHARED | ULF_NO_ERRNO, (void *)addr, expected, nsec, 0);
    if (ret >= 0 || ret == -EFAULT) {
        *wake_valp = __atomic_load_n(addr, __ATOMIC_SEQ_CST);
        ret = 0;
    } else {
        errno = -ret;
        ret = -1;
    }

    return (ret);
}

/*
 * __wt_futex_wake --
 *     Wake the futex.
 */
int
__wt_futex_wake(volatile WT_FUTEX_WORD *addr, WT_FUTEX_WAKE wake, WT_FUTEX_WORD wake_val)
{
    WT_DECL_RET;
    uint32_t op;

    WT_ASSERT(NULL, wake == WT_FUTEX_WAKE_ONE || wake == WT_FUTEX_WAKE_ALL);
    op =
      UL_COMPARE_AND_WAIT_SHARED | ULF_NO_ERRNO | ((wake == WT_FUTEX_WAKE_ALL) ? ULF_WAKE_ALL : 0);
    __atomic_store_n(addr, wake_val, __ATOMIC_SEQ_CST);
    ret = __ulock_wake(op, (void *)addr, wake_val);
    switch (ret) {
    case -ENOENT: /* No waiters were awoken.  */
        ret = 0;
        break;
    case -EINTR: /* Fall thru. */
    case -EAGAIN:
        errno = EINTR;
        ret = -1;
        break;
    }

    return (ret);
}
