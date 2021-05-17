/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_tiered_push_work --
 *     Push a work unit to the queue. Assumes it is passed an already filled out structure.
 */
void
__wt_tiered_push_work(WT_SESSION_IMPL *session, WT_TIERED_WORK_UNIT *entry)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    __wt_spin_lock(session, &conn->tiered_lock);
    TAILQ_INSERT_TAIL(conn->tieredqh, entry, q);
    WT_STAT_CONN_INCR(session, tiered_work_units_created);
    __wt_spin_unlock(session, &conn->tiered_lock);
    __wt_cond_signal(session, conn->tiered_cond);
    return;
}

/*
 * __wt_tiered_pop_work --
 *     Pop a work unit of the given type from the queue. If a maximum value is given, only return a
 *     work unit that is less than the maximum value.
 */
void
__wt_tiered_pop_work(
  WT_SESSION_IMPL *session, uint32_t type, uint64_t maxval, WT_TIERED_WORK_UNIT **entryp)
{
    WT_CONNECTION_IMPL *conn;
    WT_TIERED_WORK_UNIT *entry;

    *entryp = entry = NULL;

    conn = S2C(session);
    if (TAILQ_EMPTY(&conn->tieredqh))
        return;
    __wt_spin_lock(session, &conn->tiered_lock);

    TAILQ_FOREACH (entry, &conn->tieredqh, q) {
        if (FLD_ISSET(type, entry->type) && (maxval == 0 || entry->op_num < maxval)) {
            TAILQ_REMOVE(&conn->tieredqh, entry, q);
            break;
        }
    }
    *entryp = entry;
    __wt_spin_unlock(session, &conn->tiered_lock);
    return;
}

/*
 * __wt_tiered_get_flush --
 *     Get the first flush work unit from the queue. id information cannot change between our caller
 *     and here.
 */
int
__wt_tiered_get_flush(WT_SESSION_IMPL *session, WT_TIERED_WORK_UNIT **entryp)
{
    __wt_tiered_pop_work(session, WT_TIERED_WORK_FLUSH, 0, entryp);
    return (0);
}

/*
 * __wt_tiered_get_drop_local --
 *     Get a drop local work unit if it is less than the time given.
 */
int
__wt_tiered_get_drop_local(WT_SESSION_IMPL *session, uint64_t now, WT_TIERED_WORK_UNIT **entryp)
{
    __wt_tiered_pop_work(session, WT_TIERED_WORK_DROP_LOCAL, now, entryp);
    return (0);
}

/*
 * __wt_tiered_put_flush --
 *     Add a flush work unit to the queue. We're single threaded so the tiered structure's id
 *     information cannot change between our caller and here.
 */
int
__wt_tiered_put_flush(WT_SESSION_IMPL *session, WT_TIERED *tiered)
{
    WT_TIERED_WORK_UNIT *entry;

    WT_RET(__wt_calloc_one(session, &entry));
    entry->type = WT_TIERED_WORK_FLUSH;
    entry->op_num = tiered->current_id;
    entry->tiered = tiered;
    __wt_tiered_push_work(session, entry);
    return (0);
}

/*
 * __wt_tiered_put_drop_local --
 *     Add a drop local work unit for the given ID to the queue.
 */
int
__wt_tiered_put_drop_local(WT_SESSION_IMPL *session, WT_TIERED *tiered, uint64_t id)
{
    WT_TIERED_WORK_UNIT *entry;

    WT_RET(__wt_calloc_one(session, &entry));
    entry->type = WT_TIERED_WORK_DROP_LOCAL;
    entry->op_num = id;
    entry->tiered = tiered;
    __wt_tiered_push_work(session, entry);
    return (0);
}
