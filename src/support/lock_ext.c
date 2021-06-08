/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_ext_spin_init --
 *     Allocate and initialize a spinlock.
 */
int
__wt_ext_spin_init(
  WT_EXTENSION_API *wt_api, WT_SESSION *session, void **spin_retp, const char *name)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *default_session;
    WT_SPINLOCK *lock;

    WT_UNUSED(session); /*Unused parameters */
    *(void **)spin_retp = NULL;
    default_session = ((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;
    if ((ret = __wt_calloc_one(default_session, &lock)) != 0)
        return ret;
    if ((ret = __wt_spin_init(default_session, lock, name)) != 0) {
        __wt_free(default_session, lock);
        return ret;
    }
    *(void **)spin_retp = lock;
    return (0);
}

/*
 * __wt_ext_spin_lock --
 *     Lock the spinlock.
 */
void
__wt_ext_spin_lock(WT_EXTENSION_API *wt_api, WT_SESSION *session, void *spinlock)
{
    WT_SPINLOCK *lock;

    lock = (WT_SPINLOCK *)spinlock;
    WT_UNUSED(wt_api); /*Unused parameters */
    __wt_spin_lock((WT_SESSION_IMPL *)session, lock);
    return;
}

/*
 * __wt_ext_spin_unlock --
 *     Unlock the spinlock.
 */
void
__wt_ext_spin_unlock(WT_EXTENSION_API *wt_api, WT_SESSION *session, void *spinlock)
{
    WT_SPINLOCK *lock;

    lock = (WT_SPINLOCK *)spinlock;
    WT_UNUSED(wt_api); /*Unused parameters */
    __wt_spin_unlock((WT_SESSION_IMPL *)session, lock);
    return;
}

/*
 * __wt_ext_spin_destroy --
 *     Destroy the spinlock.
 */
void
__wt_ext_spin_destroy(WT_EXTENSION_API *wt_api, WT_SESSION *session, void *spinlock)
{
    WT_SESSION_IMPL *default_session;
    WT_SPINLOCK *lock;

    lock = (WT_SPINLOCK *)spinlock;

    __wt_spin_destroy((WT_SESSION_IMPL *)session, lock);
    default_session = ((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;
    __wt_free(default_session, lock);
    return;
}
