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
    if (sem_init(&sem->sem, 0, count) != 0) {
        WT_CLEAR(*sem);
        return (errno);
    }

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

    if (sem_destroy(&sem->sem) != 0)
        return (errno);

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

    if (sem_post(&sem->sem) != 0)
        return (errno);

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

    while (sem_wait(&sem->sem) != 0)
        /* Retry if interrupted by a signal. */
        if (errno != EINTR)
            return (errno);

    return (0);
}
