/*-
 * Copyright (c) 2024-present MongoDB, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#include <linux/futex.h>
#include <sys/syscall.h>
#include <errno.h>

/*
 * __wt_futex_op_wait --
 *
 *
 *
 *
 *
 * The timeout is in microseconds and MUST be greater than zero.
 */
int
__wt_futex_op_wait(
  WT_FUTEX_WORD *futexp, uint32_t expected, int64_t timeout_us, uint32_t *wake_valp)
{
    struct timespec timeout;
    long sysret;

    WT_ASSERT(NULL, futexp != NULL && timeout_us > 0 && wake_valp != NULL);

    timeout.tv_sec = (timeout_us / WT_MILLION);
    timeout.tv_nsec = (timeout_us % WT_MILLION) * WT_THOUSAND;

    sysret = syscall(SYS_futex, futexp, FUTEX_WAIT_PRIVATE, expected, &timeout, NULL, 0);
    if (sysret == 0)
        *wake_valp = __wt_atomic_load32(futexp);

    return ((int)sysret);
}

/*
 * __wt_futex_op_wake --
 *
 *
 *
 *
 *
 */
int
__wt_futex_op_wake(WT_FUTEX_WORD *futexp, WT_FUTEX_WAKE whom)
{
    long sysret;

    WT_ASSERT(NULL, whom == WT_FUTEX_WAKE_ONE || whom == WT_FUTEX_WAKE_ALL);

    sysret = syscall(SYS_futex, futexp, FUTEX_WAKE_PRIVATE, whom, NULL, 0);

    /*
     * A positive value represents the number of waiters awoken. Replace with zero to bring into
     * alignment with other operating systems.
     */
    return ((int)((sysret >= 0) ? 0 : sysret));
}
