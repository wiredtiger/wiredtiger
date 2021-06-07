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
__wt_ext_spin_init(WT_EXTENSION_API *wt_api, WT_SESSION *session, void *spin_retp, const char *name)
{
    WT_DECL_RET;
    WT_SESSION_IMPL *default_session;
    WT_SPINLOCK *lock;

    (void)session; /*Unused parameters */
    *(void **)spin_retp = NULL;
    default_session = ((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;
    if ((ret = __wt_malloc(default_session, sizeof(WT_SPINLOCK), &lock)) != 0)
        return ret;
    if ((ret = __wt_spin_init(default_session, (WT_SPINLOCK *)lock, name)) != 0) {
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
    (void)wt_api; /*Unused parameters */
    __wt_spin_lock((WT_SESSION_IMPL *)session, (WT_SPINLOCK *)spinlock);
    return;
}

/*
 * __wt_ext_spin_unlock --
 *     Unlock the spinlock.
 */
void
__wt_ext_spin_unlock(WT_EXTENSION_API *wt_api, WT_SESSION *session, void *spinlock)
{
    (void)wt_api; /*Unused parameters */
    __wt_spin_unlock((WT_SESSION_IMPL *)session, (WT_SPINLOCK *)spinlock);
    return;
}

/*
 * __wt_ext_spin_destroy --
 *     Destroy the spinlock.
 */
void
__wt_ext_spin_destroy(WT_EXTENSION_API *wt_api, WT_SESSION *session, void **spinlock)
{
    WT_SESSION_IMPL *default_session;

    (void)wt_api; /*Unused parameters */
    __wt_spin_destroy((WT_SESSION_IMPL *)session, *((WT_SPINLOCK **)spinlock));
    default_session = ((WT_CONNECTION_IMPL *)wt_api->conn)->default_session;
    __wt_free(default_session, *spinlock);
    *spinlock = NULL;
    return;
}
