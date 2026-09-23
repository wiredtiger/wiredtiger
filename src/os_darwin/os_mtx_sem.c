/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_semaphore_init --
 *     Initialize a semaphore.
 */
int
__wt_semaphore_init(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem, uint32_t count, const char *name)
{
    WT_UNUSED(session);
    WT_CLEAR(*sem);

    sem->name = name;
    sem->sem = dispatch_semaphore_create(0);
    if (sem->sem == NULL) {
        WT_CLEAR(*sem);
        return (ENOMEM);
    }

    /* Workaround for semaphore creation not supporting non-zero initial count. */
    for (uint32_t i = 0; i < count; ++i)
        /* Ignore the return value, which indicates whether it woke up a thread. */
        WT_IGNORE_RET(dispatch_semaphore_signal(sem->sem));

    return (0);
}

/*
 * __wt_semaphore_destroy --
 *     Destroy a semaphore.
 */
int
__wt_semaphore_destroy(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem)
{
    WT_UNUSED(session);

    dispatch_release(sem->sem);

    WT_CLEAR(*sem);
    return (0);
}

/*
 * __wt_semaphore_post --
 *     Post a semaphore.
 */
int
__wt_semaphore_post(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem)
{
    WT_UNUSED(session);

    /* Ignore the return value, which indicates whether it woke up a thread. */
    WT_IGNORE_RET(dispatch_semaphore_signal(sem->sem));

    return (0);
}

/*
 * __wt_semaphore_wait --
 *     Wait on a semaphore.
 */
int
__wt_semaphore_wait(WT_SESSION_IMPL *session, WT_SEMAPHORE *sem)
{
    WT_UNUSED(session);

    if (dispatch_semaphore_wait(sem->sem, DISPATCH_TIME_FOREVER) != 0)
        /* We should not get here due to a timeout because we use DISPATCH_TIME_FOREVER. */
        return (EINVAL);

    return (0);
}
