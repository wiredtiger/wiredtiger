/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __snapsort_partition --
 *     Custom quick sort partitioning for snapshots.
 */
static uint32_t
__snapsort_partition(uint64_t *array, uint32_t f, uint32_t l, uint64_t pivot)
{
    uint32_t i, j;

    i = f - 1;
    j = l + 1;
    for (;;) {
        while (pivot < array[--j])
            ;
        while (array[++i] < pivot)
            ;
        if (i < j) {
            uint64_t tmp = array[i];
            array[i] = array[j];
            array[j] = tmp;
        } else
            return (j);
    }
}

/*
 * __snapsort_impl --
 *     Custom quick sort implementation for snapshots.
 */
static void
__snapsort_impl(uint64_t *array, uint32_t f, uint32_t l)
{
    while (f + 16 < l) {
        uint64_t v1 = array[f], v2 = array[l], v3 = array[(f + l) / 2];
        uint64_t median =
          v1 < v2 ? (v3 < v1 ? v1 : WT_MIN(v2, v3)) : (v3 < v2 ? v2 : WT_MIN(v1, v3));
        uint32_t m = __snapsort_partition(array, f, l, median);
        __snapsort_impl(array, f, m);
        f = m + 1;
    }
}

/*
 * __snapsort --
 *     Sort an array of transaction IDs.
 */
static void
__snapsort(uint64_t *array, uint32_t size)
{
    __snapsort_impl(array, 0, size - 1);
    WT_INSERTION_SORT(array, size, uint64_t, WT_TXNID_LT);
}

/*
 * __txn_remove_from_global_table --
 *     Remove the transaction id from the global transaction table.
 */
static inline void
__txn_remove_from_global_table(WT_SESSION_IMPL *session)
{
#ifdef HAVE_DIAGNOSTIC
    WT_TXN *txn;
    WT_TXN_GLOBAL *txn_global;
    WT_TXN_SHARED *txn_shared;

    txn = session->txn;
    txn_global = &S2C(session)->txn_global;
    txn_shared = WT_SESSION_TXN_SHARED(session);

    WT_ASSERT(session, !WT_TXNID_LT(txn->id, txn_global->last_running));
    WT_ASSERT(session, txn->id != WT_TXN_NONE && txn_shared->id != WT_TXN_NONE);
#else
    WT_TXN_SHARED *txn_shared;

    txn_shared = WT_SESSION_TXN_SHARED(session);
#endif
    WT_PUBLISH(txn_shared->id, WT_TXN_NONE);
}

/*
 * __txn_sort_snapshot --
 *     Sort a snapshot for faster searching and set the min/max bounds.
 */
static void
__txn_sort_snapshot(WT_SESSION_IMPL *session, uint32_t n, uint64_t snap_max)
{
    WT_TXN *txn;

    txn = session->txn;

    if (n > 1)
        __snapsort(txn->snapshot, n);

    txn->snapshot_count = n;
    txn->snap_max = snap_max;
    txn->snap_min =
      (n > 0 && WT_TXNID_LE(txn->snapshot[0], snap_max)) ? txn->snapshot[0] : snap_max;
    F_SET(txn, WT_TXN_HAS_SNAPSHOT);
    WT_ASSERT(session, n == 0 || txn->snap_min != WT_TXN_NONE);
}

/*
 * __wt_txn_release_snapshot --
 *     Release the snapshot in the current transaction.
 */
void
__wt_txn_release_snapshot(WT_SESSION_IMPL *session)
{
    WT_TXN *txn;
    WT_TXN_GLOBAL *txn_global;
    WT_TXN_SHARED *txn_shared;

    txn = session->txn;
    txn_global = &S2C(session)->txn_global;
    txn_shared = WT_SESSION_TXN_SHARED(session);

    WT_ASSERT(session,
      txn_shared->pinned_id == WT_TXN_NONE || session->txn->isolation == WT_ISO_READ_UNCOMMITTED ||
        !__wt_txn_visible_all(session, txn_shared->pinned_id, WT_TS_NONE));

    txn_shared->metadata_pinned = txn_shared->pinned_id = WT_TXN_NONE;
    F_CLR(txn, WT_TXN_HAS_SNAPSHOT);

    /* Clear a checkpoint's pinned ID and timestamp. */
    if (WT_SESSION_IS_CHECKPOINT(session)) {
        txn_global->checkpoint_txn_shared.pinned_id = WT_TXN_NONE;
        txn_global->checkpoint_timestamp = WT_TS_NONE;
    }
}

/*
 * __wt_txn_active --
 *     Check if a transaction is still active. If not, it is either committed, prepared, or rolled
 *     back. It is possible that we race with commit, prepare or rollback and a transaction is still
 *     active before the start of the call is eventually reported as resolved.
 */
bool
__wt_txn_active(WT_SESSION_IMPL *session, uint64_t txnid)
{
    WT_CONNECTION_IMPL *conn;
    WT_TXN_GLOBAL *txn_global;
    WT_TXN_SHARED *s;
    uint64_t oldest_id;
    uint32_t i, session_cnt;
    bool active;

    conn = S2C(session);
    txn_global = &conn->txn_global;
    active = true;

    /* We're going to scan the table: wait for the lock. */
    __wt_readlock(session, &txn_global->rwlock);
    oldest_id = txn_global->oldest_id;

    if (WT_TXNID_LT(txnid, oldest_id)) {
        active = false;
        goto done;
    }

    /* Walk the array of concurrent transactions. */
    WT_ORDERED_READ(session_cnt, conn->session_cnt);
    WT_STAT_CONN_INCR(session, txn_walk_sessions);
    for (i = 0, s = txn_global->txn_shared_list; i < session_cnt; i++, s++) {
        WT_STAT_CONN_INCR(session, txn_sessions_walked);
        /* If the transaction is in the list, it is uncommitted. */
        if (s->id == txnid)
            goto done;
    }

    active = false;
done:
    __wt_readunlock(session, &txn_global->rwlock);
    return (active);
}

/*
 * __txn_get_snapshot_int --
 *     Allocate a snapshot, optionally update our shared txn ids.
 */
static void
__txn_get_snapshot_int(WT_SESSION_IMPL *session, bool publish)
{
    WT_CONNECTION_IMPL *conn;
    WT_TXN *txn;
    WT_TXN_GLOBAL *txn_global;
    WT_TXN_SHARED *s, *txn_shared;
    uint64_t commit_gen, current_id, id, prev_oldest_id, pinned_id;
    uint32_t i, n, session_cnt;

    conn = S2C(session);
    txn = session->txn;
    txn_global = &conn->txn_global;
    txn_shared = WT_SESSION_TXN_SHARED(session);
    n = 0;

    /* Fast path if we already have the current snapshot. */
    if ((commit_gen = __wt_session_gen(session, WT_GEN_COMMIT)) != 0) {
        if (F_ISSET(txn, WT_TXN_HAS_SNAPSHOT) && commit_gen == __wt_gen(session, WT_GEN_COMMIT))
            return;
        __wt_session_gen_leave(session, WT_GEN_COMMIT);
    }
    __wt_session_gen_enter(session, WT_GEN_COMMIT);

    /* We're going to scan the table: wait for the lock. */
    __wt_readlock(session, &txn_global->rwlock);

    current_id = pinned_id = txn_global->current;
    prev_oldest_id = txn_global->oldest_id;

    /*
     * Include the checkpoint transaction, if one is running: we should ignore any uncommitted
     * changes the checkpoint has written to the metadata. We don't have to keep the checkpoint's
     * changes pinned so don't go including it in the published pinned ID.
     *
     * We can assume that if a function calls without intention to publish then it is the special
     * case of checkpoint calling it twice. In which case do not include the checkpoint id.
     */
    if ((id = txn_global->checkpoint_txn_shared.id) != WT_TXN_NONE) {
        if (txn->id != id)
            txn->snapshot[n++] = id;
        if (publish)
            txn_shared->metadata_pinned = id;
    }

    /* For pure read-only workloads, avoid scanning. */
    if (prev_oldest_id == current_id) {
        pinned_id = current_id;
        /* Check that the oldest ID has not moved in the meantime. */
        WT_ASSERT(session, prev_oldest_id == txn_global->oldest_id);
        goto done;
    }

    /* Walk the array of concurrent transactions. */
    WT_ORDERED_READ(session_cnt, conn->session_cnt);
    WT_STAT_CONN_INCR(session, txn_walk_sessions);
    for (i = 0, s = txn_global->txn_shared_list; i < session_cnt; i++, s++) {
        WT_STAT_CONN_INCR(session, txn_sessions_walked);
        /*
         * Build our snapshot of any concurrent transaction IDs.
         *
         * Ignore:
         *  - Our own ID: we always read our own updates.
         *  - The ID if it is older than the oldest ID we saw. This
         *    can happen if we race with a thread that is allocating
         *    an ID -- the ID will not be used because the thread will
         *    keep spinning until it gets a valid one.
         *  - The ID if it is higher than the current ID we saw. This
         *    can happen if the transaction is already finished. In
         *    this case, we ignore this transaction because it would
         *    not be visible to the current snapshot.
         */
        while (s != txn_shared && (id = s->id) != WT_TXN_NONE && WT_TXNID_LE(prev_oldest_id, id) &&
          WT_TXNID_LT(id, current_id)) {
            /*
             * If the transaction is still allocating its ID, then we spin here until it gets its
             * valid ID.
             */
            WT_READ_BARRIER();
            if (!s->is_allocating) {
                /*
                 * There is still a chance that fetched ID is not valid after ID allocation, so we
                 * check again here. The read of transaction ID should be carefully ordered: we want
                 * to re-read ID from transaction state after this transaction completes ID
                 * allocation.
                 */
                WT_READ_BARRIER();
                if (id == s->id) {
                    txn->snapshot[n++] = id;
                    if (WT_TXNID_LT(id, pinned_id))
                        pinned_id = id;
                    break;
                }
            }
            WT_PAUSE();
        }
    }

    /*
     * If we got a new snapshot, update the published pinned ID for this session.
     */
    WT_ASSERT(session, WT_TXNID_LE(prev_oldest_id, pinned_id));
    WT_ASSERT(session, prev_oldest_id == txn_global->oldest_id);
done:
    if (publish)
        txn_shared->pinned_id = pinned_id;
    __wt_readunlock(session, &txn_global->rwlock);
    __txn_sort_snapshot(session, n, current_id);
}

/*
 * __wt_txn_get_snapshot --
 *     Common case, allocate a snapshot and update our shared ids.
 */
void
__wt_txn_get_snapshot(WT_SESSION_IMPL *session)
{
    __txn_get_snapshot_int(session, true);
}

/*
 * __wt_txn_bump_snapshot --
 *     Uncommon case, allocate a snapshot but skip updating our shared ids.
 */
void
__wt_txn_bump_snapshot(WT_SESSION_IMPL *session)
{
    __txn_get_snapshot_int(session, false);
}

/*
 * __txn_oldest_scan --
 *     Sweep the running transactions to calculate the oldest ID required.
 */
static void
__txn_oldest_scan(WT_SESSION_IMPL *session, uint64_t *oldest_idp, uint64_t *last_runningp,
  uint64_t *metadata_pinnedp, WT_SESSION_IMPL **oldest_sessionp)
{
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *oldest_session;
    WT_TXN_GLOBAL *txn_global;
    WT_TXN_SHARED *s;
    uint64_t id, last_running, metadata_pinned, oldest_id, prev_oldest_id;
    uint32_t i, session_cnt;

    conn = S2C(session);
    txn_global = &conn->txn_global;
    oldest_session = NULL;

    /* The oldest ID cannot change while we are holding the scan lock. */
    prev_oldest_id = txn_global->oldest_id;
    last_running = oldest_id = txn_global->current;
    if ((metadata_pinned = txn_global->checkpoint_txn_shared.id) == WT_TXN_NONE)
        metadata_pinned = oldest_id;

    /* Walk the array of concurrent transactions. */
    WT_ORDERED_READ(session_cnt, conn->session_cnt);
    WT_STAT_CONN_INCR(session, txn_walk_sessions);
    for (i = 0, s = txn_global->txn_shared_list; i < session_cnt; i++, s++) {
        WT_STAT_CONN_INCR(session, txn_sessions_walked);
        /* Update the last running transaction ID. */
        while ((id = s->id) != WT_TXN_NONE && WT_TXNID_LE(prev_oldest_id, id) &&
          WT_TXNID_LT(id, last_running)) {
            /*
             * If the transaction is still allocating its ID, then we spin here until it gets its
             * valid ID.
             */
            WT_READ_BARRIER();
            if (!s->is_allocating) {
                /*
                 * There is still a chance that fetched ID is not valid after ID allocation, so we
                 * check again here. The read of transaction ID should be carefully ordered: we want
                 * to re-read ID from transaction state after this transaction completes ID
                 * allocation.
                 */
                WT_READ_BARRIER();
                if (id == s->id) {
                    last_running = id;
                    break;
                }
            }
            WT_PAUSE();
        }

        /* Update the metadata pinned ID. */
        if ((id = s->metadata_pinned) != WT_TXN_NONE && WT_TXNID_LT(id, metadata_pinned))
            metadata_pinned = id;

        /*
         * !!!
         * Note: Don't ignore pinned ID values older than the previous
         * oldest ID.  Read-uncommitted operations publish pinned ID
         * values without acquiring the scan lock to protect the global
         * table.  See the comment in __wt_txn_cursor_op for more
         * details.
         */
        if ((id = s->pinned_id) != WT_TXN_NONE && WT_TXNID_LT(id, oldest_id)) {
            oldest_id = id;
            oldest_session = &conn->sessions[i];
        }
    }

    if (WT_TXNID_LT(last_running, oldest_id))
        oldest_id = last_running;

    /* The metadata pinned ID can't move past the oldest ID. */
    if (WT_TXNID_LT(oldest_id, metadata_pinned))
        metadata_pinned = oldest_id;

    *last_runningp = last_running;
    *metadata_pinnedp = metadata_pinned;
    *oldest_idp = oldest_id;
    *oldest_sessionp = oldest_session;
}

/*
 * __wt_txn_update_oldest --
 *     Sweep the running transactions to update the oldest ID required.
 */
int
__wt_txn_update_oldest(WT_SESSION_IMPL *session, uint32_t flags)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_SESSION_IMPL *oldest_session;
    WT_TXN_GLOBAL *txn_global;
    uint64_t current_id, last_running, metadata_pinned, oldest_id;
    uint64_t prev_last_running, prev_metadata_pinned, prev_oldest_id;
    bool strict, wait;

    conn = S2C(session);
    txn_global = &conn->txn_global;
    strict = LF_ISSET(WT_TXN_OLDEST_STRICT);
    wait = LF_ISSET(WT_TXN_OLDEST_WAIT);

    current_id = last_running = metadata_pinned = txn_global->current;
    prev_last_running = txn_global->last_running;
    prev_metadata_pinned = txn_global->metadata_pinned;
    prev_oldest_id = txn_global->oldest_id;

    /* Try to move the pinned timestamp forward. */
    if (strict)
        __wt_txn_update_pinned_timestamp(session, false);

    /*
     * For pure read-only workloads, or if the update isn't forced and the oldest ID isn't too far
     * behind, avoid scanning.
     */
    if ((prev_oldest_id == current_id && prev_metadata_pinned == current_id) ||
      (!strict && WT_TXNID_LT(current_id, prev_oldest_id + 100)))
        return (0);

    /* First do a read-only scan. */
    if (wait)
        __wt_readlock(session, &txn_global->rwlock);
    else if ((ret = __wt_try_readlock(session, &txn_global->rwlock)) != 0)
        return (ret == EBUSY ? 0 : ret);
    __txn_oldest_scan(session, &oldest_id, &last_running, &metadata_pinned, &oldest_session);
    __wt_readunlock(session, &txn_global->rwlock);

    /*
     * If the state hasn't changed (or hasn't moved far enough for non-forced updates), give up.
     */
    if ((oldest_id == prev_oldest_id ||
          (!strict && WT_TXNID_LT(oldest_id, prev_oldest_id + 100))) &&
      ((last_running == prev_last_running) ||
        (!strict && WT_TXNID_LT(last_running, prev_last_running + 100))) &&
      metadata_pinned == prev_metadata_pinned)
        return (0);

    /* It looks like an update is necessary, wait for exclusive access. */
    if (wait)
        __wt_writelock(session, &txn_global->rwlock);
    else if ((ret = __wt_try_writelock(session, &txn_global->rwlock)) != 0)
        return (ret == EBUSY ? 0 : ret);

    /*
     * If the oldest ID has been updated while we waited, don't bother scanning.
     */
    if (WT_TXNID_LE(oldest_id, txn_global->oldest_id) &&
      WT_TXNID_LE(last_running, txn_global->last_running) &&
      WT_TXNID_LE(metadata_pinned, txn_global->metadata_pinned))
        goto done;

    /*
     * Re-scan now that we have exclusive access. This is necessary because threads get transaction
     * snapshots with read locks, and we have to be sure that there isn't a thread that has got a
     * snapshot locally but not yet published its snap_min.
     */
    __txn_oldest_scan(session, &oldest_id, &last_running, &metadata_pinned, &oldest_session);

    /* Update the public IDs. */
    if (WT_TXNID_LT(txn_global->metadata_pinned, metadata_pinned))
        txn_global->metadata_pinned = metadata_pinned;
    if (WT_TXNID_LT(txn_global->oldest_id, oldest_id))
        txn_global->oldest_id = oldest_id;
    if (WT_TXNID_LT(txn_global->last_running, last_running)) {
        txn_global->last_running = last_running;

        /* Output a verbose message about long-running transactions,
         * but only when some progress is being made. */
        if (WT_VERBOSE_ISSET(session, WT_VERB_TRANSACTION) && current_id - oldest_id > 10000 &&
          oldest_session != NULL) {
            __wt_verbose(session, WT_VERB_TRANSACTION,
              "old snapshot %" PRIu64 " pinned in session %" PRIu32 " [%s] with snap_min %" PRIu64,
              oldest_id, oldest_session->id, oldest_session->lastop, oldest_session->txn->snap_min);
        }
    }

done:
    __wt_writeunlock(session, &txn_global->rwlock);
    return (ret);
}

/*
 * __txn_config_operation_timeout --
 *     Configure a transactions operation timeout duration.
 */
static int
__txn_config_operation_timeout(WT_SESSION_IMPL *session, const char *cfg[], bool start_timer)
{
    WT_CONFIG_ITEM cval;
    WT_TXN *txn;

    txn = session->txn;

    if (cfg == NULL)
        return (0);

    /* Retrieve the maximum operation time, defaulting to the database-wide configuration. */
    WT_RET(__wt_config_gets_def(session, cfg, "operation_timeout_ms", 0, &cval));

    /*
     * The default configuration value is 0, we can't tell if they're setting it back to 0 or, if
     * the default was automatically passed in.
     */
    if (cval.val != 0) {
        txn->operation_timeout_us = (uint64_t)(cval.val * WT_THOUSAND);
        /*
         * The op timer will generally be started on entry to the API call however when we configure
         * it internally we need to start it separately.
         */
        if (start_timer)
            __wt_op_timer_start(session);
    }
    return (0);
}

/*
 * __wt_txn_config --
 *     Configure a transaction.
 */
int
__wt_txn_config(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONFIG_ITEM cval;
    WT_DECL_RET;
    WT_TXN *txn;
    wt_timestamp_t read_ts;

    txn = session->txn;

    if (cfg == NULL)
        return (0);

    WT_ERR(__wt_config_gets_def(session, cfg, "isolation", 0, &cval));
    if (cval.len != 0)
        txn->isolation = WT_STRING_MATCH("snapshot", cval.str, cval.len) ?
          WT_ISO_SNAPSHOT :
          WT_STRING_MATCH("read-committed", cval.str, cval.len) ? WT_ISO_READ_COMMITTED :
                                                                  WT_ISO_READ_UNCOMMITTED;

    WT_ERR(__txn_config_operation_timeout(session, cfg, false));

    /*
     * The default sync setting is inherited from the connection, but can be overridden by an
     * explicit "sync" setting for this transaction.
     *
     * We want to distinguish between inheriting implicitly and explicitly.
     */
    F_CLR(txn, WT_TXN_SYNC_SET);
    WT_ERR(__wt_config_gets_def(session, cfg, "sync", (int)UINT_MAX, &cval));
    if (cval.val == 0 || cval.val == 1)
        /*
         * This is an explicit setting of sync. Set the flag so that we know not to overwrite it in
         * commit_transaction.
         */
        F_SET(txn, WT_TXN_SYNC_SET);

    /*
     * If sync is turned off explicitly, clear the transaction's sync field.
     */
    if (cval.val == 0)
        txn->txn_logsync = 0;

    /* Check if prepared updates should be ignored during reads. */
    WT_ERR(__wt_config_gets_def(session, cfg, "ignore_prepare", 0, &cval));
    if (cval.len > 0 && WT_STRING_MATCH("force", cval.str, cval.len))
        F_SET(txn, WT_TXN_IGNORE_PREPARE);
    else if (cval.val)
        F_SET(txn, WT_TXN_IGNORE_PREPARE | WT_TXN_READONLY);

    /* Check if commits without a timestamp are allowed. */
    WT_ERR(__wt_config_gets_def(session, cfg, "no_timestamp", 0, &cval));
    if (cval.val)
        F_SET(txn, WT_TXN_TS_NOT_SET);

    /*
     * Check if the prepare timestamp and the commit timestamp of a prepared transaction need to be
     * rounded up.
     */
    WT_ERR(__wt_config_gets_def(session, cfg, "roundup_timestamps.prepared", 0, &cval));
    if (cval.val)
        F_SET(txn, WT_TXN_TS_ROUND_PREPARED);

    /* Check if read timestamp needs to be rounded up. */
    WT_ERR(__wt_config_gets_def(session, cfg, "roundup_timestamps.read", 0, &cval));
    if (cval.val)
        F_SET(txn, WT_TXN_TS_ROUND_READ);

    WT_ERR(__wt_config_gets_def(session, cfg, "read_timestamp", 0, &cval));
    if (cval.len != 0) {
        WT_ERR(__wt_txn_parse_timestamp(session, "read", &read_ts, &cval));
        WT_ERR(__wt_txn_set_read_timestamp(session, read_ts));
    }

err:
    if (ret != 0)
        /*
         * In the event that we error during configuration we should clear the flags on the
         * transaction so they are not set in a subsequent call to transaction begin.
         */
        txn->flags = 0;
    return (ret);
}

/*
 * __wt_txn_reconfigure --
 *     WT_SESSION::reconfigure for transactions.
 */
int
__wt_txn_reconfigure(WT_SESSION_IMPL *session, const char *config)
{
    WT_CONFIG_ITEM cval;
    WT_DECL_RET;
    WT_TXN *txn;

    txn = session->txn;

    ret = __wt_config_getones(session, config, "isolation", &cval);
    if (ret == 0 && cval.len != 0) {
        session->isolation = txn->isolation = WT_STRING_MATCH("snapshot", cval.str, cval.len) ?
          WT_ISO_SNAPSHOT :
          WT_STRING_MATCH("read-uncommitted", cval.str, cval.len) ? WT_ISO_READ_UNCOMMITTED :
                                                                    WT_ISO_READ_COMMITTED;
    }
    WT_RET_NOTFOUND_OK(ret);

    return (0);
}

/*
 * __wt_txn_release --
 *     Release the resources associated with the current transaction.
 */
void
__wt_txn_release(WT_SESSION_IMPL *session)
{
    WT_TXN *txn;
    WT_TXN_GLOBAL *txn_global;

    txn = session->txn;
    txn_global = &S2C(session)->txn_global;

    WT_ASSERT(session, txn->mod_count == 0);

    /* Clear the transaction's ID from the global table. */
    if (WT_SESSION_IS_CHECKPOINT(session)) {
        WT_ASSERT(session, WT_SESSION_TXN_SHARED(session)->id == WT_TXN_NONE);
        txn->id = txn_global->checkpoint_txn_shared.id = WT_TXN_NONE;

        /*
         * Be extra careful to cleanup everything for checkpoints: once the global checkpoint ID is
         * cleared, we can no longer tell if this session is doing a checkpoint.
         */
        txn_global->checkpoint_id = 0;
    } else if (F_ISSET(txn, WT_TXN_HAS_ID)) {
        /*
         * If transaction is prepared, this would have been done in prepare.
         */
        if (!F_ISSET(txn, WT_TXN_PREPARE))
            __txn_remove_from_global_table(session);
        else
            WT_ASSERT(session, WT_SESSION_TXN_SHARED(session)->id == WT_TXN_NONE);
        txn->id = WT_TXN_NONE;
    }

    __wt_txn_clear_durable_timestamp(session);

    /* Free the scratch buffer allocated for logging. */
    __wt_logrec_free(session, &txn->logrec);

    /* Discard any memory from the session's stash that we can. */
    WT_ASSERT(session, __wt_session_gen(session, WT_GEN_SPLIT) == 0);
    __wt_stash_discard(session);

    /*
     * Reset the transaction state to not running and release the snapshot.
     */
    __wt_txn_release_snapshot(session);
    /* Clear the read timestamp. */
    __wt_txn_clear_read_timestamp(session);
    txn->isolation = session->isolation;

    txn->rollback_reason = NULL;

    /*
     * Ensure the transaction flags are cleared on exit
     *
     * Purposely do NOT clear the commit and durable timestamps on release. Other readers may still
     * find these transactions in the durable queue and will need to see those timestamps.
     */
    txn->flags = 0;
    txn->prepare_timestamp = WT_TS_NONE;

    /* Clear operation timer. */
    txn->operation_timeout_us = 0;
}

/*
 * __txn_locate_hs_record --
 *     Locate the update older than the prepared update in the history store and append it to the
 *     update chain if necessary.
 */
static int
__txn_locate_hs_record(WT_SESSION_IMPL *session, WT_CURSOR *hs_cursor, WT_PAGE *page,
  WT_UPDATE *chain, bool commit, WT_UPDATE **fix_updp, bool *upd_appended,
  WT_UPDATE *first_committed_upd, bool first_committed_upd_in_hs)
{
    WT_DECL_ITEM(hs_value);
    WT_DECL_RET;
    WT_TIME_WINDOW *hs_tw;
    WT_UPDATE *tombstone, *upd;
    wt_timestamp_t durable_ts, hs_stop_durable_ts;
    size_t size, total_size;
    uint64_t type_full;
    char ts_string[2][WT_TS_INT_STRING_SIZE];

    WT_ASSERT(session, chain != NULL);

    hs_tw = NULL;
    *fix_updp = NULL;
    *upd_appended = false;
    size = total_size = 0;
    tombstone = upd = NULL;

    WT_ERR(__wt_scr_alloc(session, 0, &hs_value));

    /* Get current value. */
    WT_ERR(hs_cursor->get_value(hs_cursor, &hs_stop_durable_ts, &durable_ts, &type_full, hs_value));

    /* The value older than the prepared update in the history store must be a full value. */
    WT_ASSERT(session, (uint8_t)type_full == WT_UPDATE_STANDARD);

    /*
     * If the history update already has a stop time point and we are committing the prepared update
     * there is no work to do. This happens if a deleted key is reinserted by a prepared update.
     */
    if (hs_stop_durable_ts != WT_TS_MAX && commit)
        goto done;

    if (!first_committed_upd_in_hs) {
        __wt_hs_upd_time_window(hs_cursor, &hs_tw);
        WT_ERR(__wt_upd_alloc(session, hs_value, WT_UPDATE_STANDARD, &upd, &size));
        upd->txnid = hs_tw->start_txn;
        upd->durable_ts = hs_tw->durable_start_ts;
        upd->start_ts = hs_tw->start_ts;
        if (commit)
            *fix_updp = upd;
    } else if (commit)
        *fix_updp = first_committed_upd;

    /*
     * When the prepared update is getting committed or the history store update is still on the
     * update chain, no need to append it onto the update chain.
     */
    if (commit || first_committed_upd_in_hs)
        goto done;

    /*
     * Set the flag to indicate that this update has been restored from history store for the
     * rollback of a prepared transaction.
     */
    F_SET(upd, WT_UPDATE_RESTORED_FROM_HS | WT_UPDATE_TO_DELETE_FROM_HS);
    total_size += size;

    __wt_verbose(session, WT_VERB_TRANSACTION,
      "update restored from history store (txnid: %" PRIu64 ", start_ts: %s, durable_ts: %s",
      upd->txnid, __wt_timestamp_to_string(upd->start_ts, ts_string[0]),
      __wt_timestamp_to_string(upd->durable_ts, ts_string[1]));

    /* If the history store record has a valid stop time point, append it. */
    if (hs_stop_durable_ts != WT_TS_MAX) {
        WT_ASSERT(session, hs_tw->stop_ts != WT_TS_MAX);
        WT_ERR(__wt_upd_alloc(session, NULL, WT_UPDATE_TOMBSTONE, &tombstone, &size));
        tombstone->durable_ts = hs_tw->durable_stop_ts;
        tombstone->start_ts = hs_tw->stop_ts;
        tombstone->txnid = hs_tw->stop_txn;
        tombstone->next = upd;
        /*
         * Set the flag to indicate that this update has been restored from history store for the
         * rollback of a prepared transaction.
         */
        F_SET(tombstone, WT_UPDATE_RESTORED_FROM_HS | WT_UPDATE_TO_DELETE_FROM_HS);
        total_size += size;

        __wt_verbose(session, WT_VERB_TRANSACTION,
          "tombstone restored from history store (txnid: %" PRIu64 ", start_ts: %s, durable_ts: %s",
          tombstone->txnid, __wt_timestamp_to_string(tombstone->start_ts, ts_string[0]),
          __wt_timestamp_to_string(tombstone->durable_ts, ts_string[1]));

        upd = tombstone;
    }

    /* Walk to the end of the chain and we can only have prepared updates on the update chain. */
    for (;; chain = chain->next) {
        WT_ASSERT(
          session, chain->txnid != WT_TXN_ABORTED && chain->prepare_state == WT_PREPARE_INPROGRESS);

        if (chain->next == NULL)
            break;
    }

    /* Append the update to the end of the chain. */
    WT_PUBLISH(chain->next, upd);
    *upd_appended = true;

    __wt_cache_page_inmem_incr(session, page, total_size);

    if (0) {
err:
        WT_ASSERT(session, tombstone == NULL || upd == tombstone);
        __wt_free_update_list(session, &upd);
    }
done:
    __wt_scr_free(session, &hs_value);
    return (ret);
}

/*
 * __txn_timestamp_usage_check --
 *     Check if a commit will violate timestamp rules.
 */
static inline int
__txn_timestamp_usage_check(WT_SESSION_IMPL *session, WT_TXN_OP *op, WT_UPDATE *upd)
{
    WT_BTREE *btree;
    WT_TXN *txn;
    wt_timestamp_t op_ts, prev_op_durable_ts;
    uint32_t flags;
    char ts_string[2][WT_TS_INT_STRING_SIZE];
    const char *name;
    bool no_ts_ok, txn_has_ts;

    btree = op->btree;
    txn = session->txn;
    flags = btree->dhandle->ts_flags;
    name = btree->dhandle->name;
    txn_has_ts = F_ISSET(txn, WT_TXN_HAS_TS_COMMIT | WT_TXN_HAS_TS_DURABLE);

    /* Timestamps are ignored on logged files. */
    if (F_ISSET(btree, WT_BTREE_LOGGED))
        return (0);

    /*
     * Do not check for timestamp usage in recovery. We don't expect recovery to be using timestamps
     * when applying commits, and it is possible that timestamps may be out-of-order in log replay.
     */
    if (F_ISSET(S2C(session), WT_CONN_RECOVERING))
        return (0);

    op_ts = upd->start_ts != WT_TS_NONE ? upd->start_ts : txn->commit_timestamp;

    /* Check for disallowed timestamps. */
    if (LF_ISSET(WT_DHANDLE_TS_NEVER)) {
        if (!txn_has_ts)
            return (0);

        __wt_err(session, EINVAL,
          "%s: " WT_TS_VERBOSE_PREFIX "timestamp %s set when disallowed by table configuration",
          name, __wt_timestamp_to_string(op_ts, ts_string[0]));
#ifdef HAVE_DIAGNOSTIC
        __wt_abort(session);
#endif
#ifdef WT_STANDALONE_BUILD
        return (EINVAL);
#endif
    }

    prev_op_durable_ts = upd->prev_durable_ts;

    /*
     * Ordered consistency requires all updates use timestamps, once they are first used, but this
     * test can be turned off on a per-transaction basis.
     */
    no_ts_ok = LF_ISSET(WT_DHANDLE_TS_MIXED_MODE) || F_ISSET(txn, WT_TXN_TS_NOT_SET);
    if (!txn_has_ts && prev_op_durable_ts != WT_TS_NONE && !no_ts_ok) {
        __wt_err(session, EINVAL,
          "%s: " WT_TS_VERBOSE_PREFIX
          "no timestamp provided for an update to a table configured to always use timestamps "
          "once they are first used",
          name);
#ifdef HAVE_DIAGNOSTIC
        __wt_abort(session);
#endif
#ifdef WT_STANDALONE_BUILD
        return (EINVAL);
#endif
    }

    /* Ordered consistency requires all updates be in timestamp order. */
    if (txn_has_ts && prev_op_durable_ts > op_ts) {
        __wt_err(session, EINVAL,
          "%s: " WT_TS_VERBOSE_PREFIX
          "updating a value with a timestamp %s before the previous update %s",
          name, __wt_timestamp_to_string(op_ts, ts_string[0]),
          __wt_timestamp_to_string(prev_op_durable_ts, ts_string[1]));
#ifdef HAVE_DIAGNOSTIC
        __wt_abort(session);
#endif
#ifdef WT_STANDALONE_BUILD
        return (EINVAL);
#endif
    }

    return (0);
}

/*
 * __txn_fixup_prepared_update --
 *     Fix the history store record with the max stop time point if we commit the prepared update.
 */
static int
__txn_fixup_prepared_update(WT_SESSION_IMPL *session, WT_CURSOR *hs_cursor, WT_UPDATE *fix_upd)
{
    WT_DECL_RET;
    WT_ITEM hs_value;
    WT_TIME_WINDOW tw;
    WT_TXN *txn;
    uint32_t txn_flags;
#ifdef HAVE_DIAGNOSTIC
    uint64_t hs_upd_type;
    wt_timestamp_t hs_durable_ts, hs_stop_durable_ts;
#endif

    txn = session->txn;
    WT_TIME_WINDOW_INIT(&tw);

    /* We should not fix a history store record already with a valid tombstone. */
    WT_ASSERT(session, fix_upd->type != WT_UPDATE_TOMBSTONE);

    /*
     * Transaction error is cleared temporarily as cursor functions are not allowed after an error
     * or a prepared transaction.
     */
    txn_flags = FLD_MASK(txn->flags, WT_TXN_ERROR);
    F_CLR(txn, txn_flags);

    /*
     * The API layer will immediately return an error if the WT_TXN_PREPARE flag is set before
     * attempting cursor operations. However, we can't clear the WT_TXN_PREPARE flag because a
     * function in the eviction flow may attempt to forcibly rollback the transaction if it is not
     * marked as a prepared transaction. The flag WT_TXN_PREPARE_IGNORE_API_CHECK is set so that
     * cursor operations can proceed without having to clear the WT_TXN_PREPARE flag.
     */
    F_SET(txn, WT_TXN_PREPARE_IGNORE_API_CHECK);

    tw.stop_ts = txn->commit_timestamp;
    tw.durable_stop_ts = txn->durable_timestamp;
    tw.stop_txn = txn->id;
    WT_TIME_WINDOW_SET_START(&tw, fix_upd);

#ifdef HAVE_DIAGNOSTIC
    /* Retrieve the existing update value and stop timestamp. */
    WT_ERR(hs_cursor->get_value(
      hs_cursor, &hs_stop_durable_ts, &hs_durable_ts, &hs_upd_type, &hs_value));
    WT_ASSERT(session, hs_stop_durable_ts == WT_TS_MAX);
    WT_ASSERT(session, (uint8_t)hs_upd_type == WT_UPDATE_STANDARD);
#endif
    /*
     * We need to update the stop durable timestamp stored in the history store value.
     *
     * Pack the value using cursor api.
     */
    hs_value.data = fix_upd->data;
    hs_value.size = fix_upd->size;
    hs_cursor->set_value(hs_cursor, &tw, tw.durable_stop_ts, tw.durable_start_ts,
      (uint64_t)WT_UPDATE_STANDARD, &hs_value);
    WT_ERR(hs_cursor->update(hs_cursor));

err:
    F_SET(txn, txn_flags);
    F_CLR(txn, WT_TXN_PREPARE_IGNORE_API_CHECK);

    return (ret);
}

/*
 * __txn_search_prepared_op --
 *     Search for an operation's prepared update.
 */
static int
__txn_search_prepared_op(
  WT_SESSION_IMPL *session, WT_TXN_OP *op, WT_CURSOR **cursorp, WT_UPDATE **updp)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_TXN *txn;
    uint32_t txn_flags;
    const char *open_cursor_cfg[] = {WT_CONFIG_BASE(session, WT_SESSION_open_cursor), NULL};

    *updp = NULL;

    txn = session->txn;

    cursor = *cursorp;
    if (cursor == NULL || CUR2BT(cursor)->id != op->btree->id) {
        *cursorp = NULL;
        if (cursor != NULL)
            WT_RET(cursor->close(cursor));
        WT_RET(__wt_open_cursor(session, op->btree->dhandle->name, NULL, open_cursor_cfg, &cursor));
        *cursorp = cursor;
    }

    /*
     * Transaction error is cleared temporarily as cursor functions are not allowed after an error.
     */
    txn_flags = FLD_MASK(txn->flags, WT_TXN_ERROR);

    /*
     * The API layer will immediately return an error if the WT_TXN_PREPARE flag is set before
     * attempting cursor operations. However, we can't clear the WT_TXN_PREPARE flag because a
     * function in the eviction flow may attempt to forcibly rollback the transaction if it is not
     * marked as a prepared transaction. The flag WT_TXN_PREPARE_IGNORE_API_CHECK is set so that
     * cursor operations can proceed without having to clear the WT_TXN_PREPARE flag.
     */
    F_SET(txn, WT_TXN_PREPARE_IGNORE_API_CHECK);

    switch (op->type) {
    case WT_TXN_OP_BASIC_COL:
    case WT_TXN_OP_INMEM_COL:
        ((WT_CURSOR_BTREE *)cursor)->iface.recno = op->u.op_col.recno;
        break;
    case WT_TXN_OP_BASIC_ROW:
    case WT_TXN_OP_INMEM_ROW:
        F_CLR(txn, txn_flags);
        __wt_cursor_set_raw_key(cursor, &op->u.op_row.key);
        F_SET(txn, txn_flags);
        break;
    case WT_TXN_OP_NONE:
    case WT_TXN_OP_REF_DELETE:
    case WT_TXN_OP_TRUNCATE_COL:
    case WT_TXN_OP_TRUNCATE_ROW:
        WT_RET_PANIC_ASSERT(session, false, WT_PANIC, "invalid prepared operation update type");
        break;
    }

    F_CLR(txn, txn_flags);
    WT_WITH_BTREE(session, op->btree, ret = __wt_btcur_search_prepared(cursor, updp));
    F_SET(txn, txn_flags);
    F_CLR(txn, WT_TXN_PREPARE_IGNORE_API_CHECK);
    WT_RET(ret);
    WT_RET_ASSERT(session, *updp != NULL, WT_NOTFOUND,
      "unable to locate update associated with a prepared operation");

    return (0);
}

/*
 * __txn_append_tombstone --
 *     Append a tombstone to the end of a keys update chain.
 */
static int
__txn_append_tombstone(WT_SESSION_IMPL *session, WT_TXN_OP *op, WT_CURSOR_BTREE *cbt)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_UPDATE *tombstone;
    size_t not_used;
    tombstone = NULL;
    btree = S2BT(session);

    WT_ERR(__wt_upd_alloc_tombstone(session, &tombstone, &not_used));
#ifdef HAVE_DIAGNOSTIC
    WT_WITH_BTREE(session, op->btree,
      ret = btree->type == BTREE_ROW ?
        __wt_row_modify(cbt, &cbt->iface.key, NULL, tombstone, WT_UPDATE_INVALID, false, false) :
        __wt_col_modify(cbt, cbt->recno, NULL, tombstone, WT_UPDATE_INVALID, false, false));
#else
    WT_WITH_BTREE(session, op->btree,
      ret = btree->type == BTREE_ROW ?
        __wt_row_modify(cbt, &cbt->iface.key, NULL, tombstone, WT_UPDATE_INVALID, false) :
        __wt_col_modify(cbt, cbt->recno, NULL, tombstone, WT_UPDATE_INVALID, false));
#endif
    WT_ERR(ret);
    tombstone = NULL;

err:
    __wt_free(session, tombstone);
    return (ret);
}

/*
 * __txn_resolve_prepared_update_chain --
 *     Helper for resolving updates. Recursively visit the update chain and resolve the updates on
 *     the way back out, so older updates are resolved first; this avoids a race with reconciliation
 *     (see WT-6778).
 */
static void
__txn_resolve_prepared_update_chain(WT_SESSION_IMPL *session, WT_UPDATE *upd, bool commit)
{

    /* If we've reached the end of the chain, we're done looking. */
    if (upd == NULL)
        return;

    /*
     * Aborted updates can exist in the update chain of our transaction. Generally this will occur
     * due to a reserved update. As such we should skip over these updates entirely.
     */
    if (upd->txnid == WT_TXN_ABORTED) {
        __txn_resolve_prepared_update_chain(session, upd->next, commit);
        return;
    }

    /*
     * If the transaction id is then different and not aborted we know we've reached the end of our
     * update chain and don't need to look deeper.
     */
    if (upd->txnid != session->txn->id)
        return;

    /* Go down the chain. Do the resolves on the way back up. */
    __txn_resolve_prepared_update_chain(session, upd->next, commit);

    if (!commit) {
        upd->txnid = WT_TXN_ABORTED;
        WT_STAT_CONN_INCR(session, txn_prepared_updates_rolledback);
        return;
    }

    /*
     * Performing an update on the same key where the truncate operation is performed can lead to
     * updates that are already resolved in the updated list. Ignore the already resolved updates.
     */
    if (upd->prepare_state == WT_PREPARE_RESOLVED) {
        WT_ASSERT(session, upd->type == WT_UPDATE_TOMBSTONE);
        return;
    }

    /* Resolve the prepared update to be a committed update. */
    __txn_resolve_prepared_update(session, upd);
    WT_STAT_CONN_INCR(session, txn_prepared_updates_committed);
}

/*
 * __txn_resolve_prepared_op --
 *     Resolve a transaction's operations indirect references.
 */
static int
__txn_resolve_prepared_op(WT_SESSION_IMPL *session, WT_TXN_OP *op, bool commit, WT_CURSOR **cursorp)
{
    WT_BTREE *btree;
    WT_CURSOR *hs_cursor;
    WT_CURSOR_BTREE *cbt;
    WT_DECL_RET;
    WT_ITEM hs_recno_key;
    WT_PAGE *page;
    WT_TIME_WINDOW tw;
    WT_TXN *txn;
    WT_UPDATE *first_committed_upd, *fix_upd, *upd, *upd_followed_tombstone;
#ifdef HAVE_DIAGNOSTIC
    WT_UPDATE *head_upd;
#endif
    uint8_t *p, hs_recno_key_buf[WT_INTPACK64_MAXSIZE];
    char ts_string[3][WT_TS_INT_STRING_SIZE];
    bool first_committed_upd_in_hs, prepare_on_disk, tw_found, upd_appended;

    hs_cursor = NULL;
    txn = session->txn;
    fix_upd = NULL;
    upd_appended = false;

    WT_RET(__txn_search_prepared_op(session, op, cursorp, &upd));

    if (commit)
        __wt_verbose(session, WT_VERB_TRANSACTION,
          "commit resolving prepared transaction with txnid: %" PRIu64
          "and timestamp: %s to commit and durable timestamps: %s,%s",
          txn->id, __wt_timestamp_to_string(txn->prepare_timestamp, ts_string[0]),
          __wt_timestamp_to_string(txn->commit_timestamp, ts_string[1]),
          __wt_timestamp_to_string(txn->durable_timestamp, ts_string[2]));
    else
        __wt_verbose(session, WT_VERB_TRANSACTION,
          "rollback resolving prepared transaction with txnid: %" PRIu64 "and timestamp:%s",
          txn->id, __wt_timestamp_to_string(txn->prepare_timestamp, ts_string[0]));

    /*
     * Aborted updates can exist in the update chain of our transaction. Generally this will occur
     * due to a reserved update. As such we should skip over these updates.
     */
    for (; upd != NULL && upd->txnid == WT_TXN_ABORTED; upd = upd->next)
        ;
#ifdef HAVE_DIAGNOSTIC
    head_upd = upd;
#endif

    /*
     * The head of the update chain is not a prepared update, which means all the prepared updates
     * of the key are resolved. The head of the update chain can also be null in the scenario that
     * we rolled back all associated updates in the previous iteration of this function.
     */
    if (upd == NULL || upd->prepare_state != WT_PREPARE_INPROGRESS)
        goto prepare_verify;

    /* A prepared operation that is rolled back will not have a timestamp worth asserting on. */
    if (commit)
        WT_RET(__txn_timestamp_usage_check(session, op, upd));

    for (first_committed_upd = upd; first_committed_upd != NULL &&
         (first_committed_upd->txnid == WT_TXN_ABORTED ||
           first_committed_upd->prepare_state == WT_PREPARE_INPROGRESS);
         first_committed_upd = first_committed_upd->next)
        ;

    /*
     * Get the underlying btree and the in-memory page with the prepared updates that are to be
     * resolved. The hazard pointer on the page is already acquired during the cursor search
     * operation to prevent eviction evicting the page while resolving the prepared updates.
     */
    cbt = (WT_CURSOR_BTREE *)(*cursorp);
    page = cbt->ref->page;

    /*
     * Locate the previous update from the history store and append it to the update chain if
     * required. We know there may be content in the history store if the prepared update is written
     * to the disk image or first committed update older than the prepared update is marked as
     * WT_UPDATE_HS. The second case is rare but can happen if the eviction that writes the prepared
     * update to the disk image fails after it has inserted the other updates of the key into the
     * history store.
     *
     * We need to do this before we resolve the prepared updates because if we abort the prepared
     * updates first, the history search logic may race with other sessions modifying the same key
     * and checkpoint moving the new updates to the history store.
     *
     * Fix the history store entry for the updates other than tombstone type or the tombstone
     * followed by the update is also from the same prepared transaction by either restoring the
     * previous update from history store or removing the key.
     */
    prepare_on_disk = F_ISSET(upd, WT_UPDATE_PREPARE_RESTORED_FROM_DS) &&
      (upd->type != WT_UPDATE_TOMBSTONE ||
        (upd->next != NULL && upd->durable_ts == upd->next->durable_ts &&
          upd->txnid == upd->next->txnid && upd->start_ts == upd->next->start_ts));
    first_committed_upd_in_hs =
      first_committed_upd != NULL && F_ISSET(first_committed_upd, WT_UPDATE_HS);

    /*
     * Marked the update older than the prepared update that is already in the history store to be
     * deleted from the history store.
     */
    if (first_committed_upd_in_hs && !commit) {
        if (first_committed_upd->type == WT_UPDATE_TOMBSTONE) {
            for (upd_followed_tombstone = first_committed_upd->next; upd_followed_tombstone != NULL;
                 upd_followed_tombstone = upd_followed_tombstone->next)
                if (upd_followed_tombstone->txnid != WT_TXN_ABORTED)
                    break;
            /* We may not find a full update following the tombstone if it is obsolete. */
            if (upd_followed_tombstone != NULL) {
                WT_ASSERT(session, F_ISSET(upd_followed_tombstone, WT_UPDATE_HS));
                F_SET(first_committed_upd, WT_UPDATE_TO_DELETE_FROM_HS);
                F_SET(upd_followed_tombstone, WT_UPDATE_TO_DELETE_FROM_HS);
            }
        } else
            F_SET(first_committed_upd, WT_UPDATE_TO_DELETE_FROM_HS);
    }

    if (prepare_on_disk || first_committed_upd_in_hs) {
        btree = S2BT(session);

        /*
         * Open a history store table cursor and scan the history store for the given btree and key
         * with maximum start timestamp to let the search point to the last version of the key.
         */
        WT_ERR(__wt_curhs_open(session, NULL, &hs_cursor));
        F_SET(hs_cursor, WT_CURSTD_HS_READ_COMMITTED);
        if (btree->type == BTREE_ROW)
            hs_cursor->set_key(hs_cursor, 4, btree->id, &cbt->iface.key, WT_TS_MAX, UINT64_MAX);
        else {
            p = hs_recno_key_buf;
            WT_ERR(__wt_vpack_uint(&p, 0, cbt->recno));
            hs_recno_key.data = hs_recno_key_buf;
            hs_recno_key.size = WT_PTRDIFF(p, hs_recno_key_buf);
            hs_cursor->set_key(hs_cursor, 4, btree->id, &hs_recno_key, WT_TS_MAX, UINT64_MAX);
        }
        WT_ERR_NOTFOUND_OK(__wt_curhs_search_near_before(session, hs_cursor), true);

        /* We should only get not found if the prepared update is on disk. */
        WT_ASSERT(session, ret != WT_NOTFOUND || prepare_on_disk);
        if (ret == WT_NOTFOUND && !commit) {
            /*
             * Allocate a tombstone and prepend it to the row so when we reconcile the update chain
             * we don't copy the prepared cell, which is now associated with a rolled back prepare,
             * and instead write nothing.
             */
            WT_ERR(__txn_append_tombstone(session, op, cbt));
        } else if (ret == 0)
            WT_ERR(__txn_locate_hs_record(session, hs_cursor, page, upd, commit, &fix_upd,
              &upd_appended, first_committed_upd, first_committed_upd_in_hs));
        else
            ret = 0;
    } else if (F_ISSET(S2C(session), WT_CONN_IN_MEMORY) && !commit && first_committed_upd == NULL) {
        /*
         * For in-memory configurations of WiredTiger if a prepared update is reconciled and then
         * rolled back the on-page value will not be marked as aborted until the next eviction. In
         * the special case where this rollback results in the update chain being entirely comprised
         * of aborted updates other transactions attempting to write to the same key will look at
         * the on-page value, think the prepared transaction is still active, and falsely report a
         * write conflict. To prevent this scenario append a tombstone to the update chain when
         * rolling back a prepared reconciled update would result in only aborted updates on the
         * update chain.
         */
        tw_found = __wt_read_cell_time_window(cbt, &tw);
        if (tw_found && tw.prepare == WT_PREPARE_INPROGRESS)
            WT_ERR(__txn_append_tombstone(session, op, cbt));
    }

    /*
     * Newer updates are inserted at head of update chain, and transaction operations are added at
     * the tail of the transaction modify chain.
     *
     * For example, a transaction has modified [k,v] as
     *	[k, v]  -> [k, u1]   (txn_op : txn_op1)
     *	[k, u1] -> [k, u2]   (txn_op : txn_op2)
     *	update chain : u2->u1
     *	txn_mod      : txn_op1->txn_op2.
     *
     * Only the key is saved in the transaction operation structure, hence we cannot identify
     * whether "txn_op1" corresponds to "u2" or "u1" during commit/rollback.
     *
     * To make things simpler we will handle all the updates that match the key saved in a
     * transaction operation in a single go. As a result, multiple updates of a key, if any will be
     * resolved as part of the first transaction operation resolution of that key, and subsequent
     * transaction operation resolution of the same key will be effectively a no-op.
     *
     * In the above example, we will resolve "u2" and "u1" as part of resolving "txn_op1" and will
     * not do any significant thing as part of "txn_op2".
     */
    __txn_resolve_prepared_update_chain(session, upd, commit);

    /* Mark the page dirty once the prepared updates are resolved. */
    __wt_page_modify_set(session, page);

    /*
     * Fix the history store contents if they exist, when there are no more updates in the update
     * list. Only in eviction, it is possible to write an unfinished history store update when the
     * prepared updates are written to the data store. When the page is read back into memory, there
     * will be only one uncommitted prepared update.
     */
    if (fix_upd != NULL) {
        WT_ASSERT(session, commit && fix_upd->type != WT_UPDATE_TOMBSTONE);
        WT_ERR(__txn_fixup_prepared_update(session, hs_cursor, fix_upd));
    }

prepare_verify:
#ifdef HAVE_DIAGNOSTIC
    for (; head_upd != NULL; head_upd = head_upd->next) {
        /*
         * Assert if we still have an update from the current transaction that hasn't been resolved
         * or aborted.
         */
        WT_ASSERT(session,
          head_upd->txnid == WT_TXN_ABORTED || head_upd->prepare_state == WT_PREPARE_RESOLVED ||
            head_upd->txnid != txn->id);

        if (head_upd->txnid == WT_TXN_ABORTED)
            continue;

        /*
         * If we restored an update from the history store, it should be the last update on the
         * chain.
         */
        if (upd_appended && head_upd->type == WT_UPDATE_STANDARD &&
          F_ISSET(head_upd, WT_UPDATE_RESTORED_FROM_HS))
            WT_ASSERT(session, head_upd->next == NULL);
    }
#endif

err:
    if (hs_cursor != NULL)
        WT_TRET(hs_cursor->close(hs_cursor));
    if (!upd_appended)
        __wt_free(session, fix_upd);
    return (ret);
}

/*
 * __txn_mod_compare --
 *     Qsort comparison routine for transaction modify list.
 */
static int WT_CDECL
__txn_mod_compare(const void *a, const void *b)
{
    WT_TXN_OP *aopt, *bopt;

    aopt = (WT_TXN_OP *)a;
    bopt = (WT_TXN_OP *)b;

    /* If the files are different, order by ID. */
    if (aopt->btree->id != bopt->btree->id)
        return (aopt->btree->id < bopt->btree->id);

    /*
     * If the files are the same, order by the key. Row-store collators require WT_SESSION pointers,
     * and we don't have one. Compare the keys if there's no collator, otherwise return equality.
     * Column-store is always easy.
     */
    if (aopt->type == WT_TXN_OP_BASIC_ROW || aopt->type == WT_TXN_OP_INMEM_ROW)
        return (aopt->btree->collator == NULL ?
            __wt_lex_compare(&aopt->u.op_row.key, &bopt->u.op_row.key, false) :
            0);
    return (aopt->u.op_col.recno < bopt->u.op_col.recno);
}

/*
 * __wt_txn_commit --
 *     Commit the current transaction.
 */
int
__wt_txn_commit(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_TXN *txn;
    WT_TXN_GLOBAL *txn_global;
    WT_TXN_OP *op;
    WT_UPDATE *upd;
    wt_timestamp_t candidate_durable_timestamp, prev_durable_timestamp;
#ifdef HAVE_DIAGNOSTIC
    uint32_t prepare_count;
#endif
    uint8_t previous_state;
    u_int i;
    bool cannot_fail, locked, prepare, readonly, update_durable_ts;

    conn = S2C(session);
    cursor = NULL;
    txn = session->txn;
    txn_global = &conn->txn_global;
#ifdef HAVE_DIAGNOSTIC
    prepare_count = 0;
#endif
    prepare = F_ISSET(txn, WT_TXN_PREPARE);
    readonly = txn->mod_count == 0;
    cannot_fail = locked = false;

    /* Permit the commit if the transaction failed, but was read-only. */
    WT_ASSERT(session, F_ISSET(txn, WT_TXN_RUNNING));
    WT_ASSERT(session, !F_ISSET(txn, WT_TXN_ERROR) || txn->mod_count == 0);

    /* Configure the timeout for this commit operation. */
    WT_ERR(__txn_config_operation_timeout(session, cfg, true));

    /*
     * Clear the prepared round up flag if the transaction is not prepared. There is no rounding up
     * to do in that case.
     */
    if (!prepare)
        F_CLR(txn, WT_TXN_TS_ROUND_PREPARED);

    /* Set the commit and the durable timestamps. */
    WT_ERR(__wt_txn_set_timestamp(session, cfg, true));

    if (prepare) {
        if (!F_ISSET(txn, WT_TXN_HAS_TS_COMMIT))
            WT_ERR_MSG(session, EINVAL, "commit_timestamp is required for a prepared transaction");

        if (!F_ISSET(txn, WT_TXN_HAS_TS_DURABLE))
            WT_ERR_MSG(session, EINVAL, "durable_timestamp is required for a prepared transaction");

        WT_ASSERT(session, txn->prepare_timestamp <= txn->commit_timestamp);
    } else {
        if (F_ISSET(txn, WT_TXN_HAS_TS_PREPARE))
            WT_ERR_MSG(session, EINVAL, "prepare timestamp is set for non-prepared transaction");

        if (F_ISSET(txn, WT_TXN_HAS_TS_DURABLE))
            WT_ERR_MSG(session, EINVAL,
              "durable_timestamp should not be specified for non-prepared transaction");
    }

    /*
     * Release our snapshot in case it is keeping data pinned (this is particularly important for
     * checkpoints). Before releasing our snapshot, copy values into any positioned cursors so they
     * don't point to updates that could be freed once we don't have a snapshot. If this transaction
     * is prepared, then copying values would have been done during prepare.
     */
    if (session->ncursors > 0 && !prepare) {
        WT_DIAGNOSTIC_YIELD;
        WT_ERR(__wt_session_copy_values(session));
    }
    __wt_txn_release_snapshot(session);

    /*
     * Resolving prepared updates is expensive. Sort prepared modifications so all updates for each
     * page within each file are done at the same time.
     */
    if (prepare)
        __wt_qsort(txn->mod, txn->mod_count, sizeof(WT_TXN_OP), __txn_mod_compare);

    /* If we are logging, write a commit log record. */
    if (txn->logrec != NULL) {
        /* Assert environment and tree are logging compatible, the fast-check is short-hand. */
        WT_ASSERT(session,
          !F_ISSET(conn, WT_CONN_RECOVERING) && FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED));

        /*
         * The default sync setting is inherited from the connection, but can be overridden by an
         * explicit "sync" setting for this transaction.
         */
        WT_ERR(__wt_config_gets_def(session, cfg, "sync", 0, &cval));

        /*
         * If the user chose the default setting, check whether sync is enabled for this transaction
         * (either inherited or via begin_transaction). If sync is disabled, clear the field to
         * avoid the log write being flushed.
         *
         * Otherwise check for specific settings. We don't need to check for "on" because that is
         * the default inherited from the connection. If the user set anything in begin_transaction,
         * we only override with an explicit setting.
         */
        if (cval.len == 0) {
            if (!FLD_ISSET(txn->txn_logsync, WT_LOG_SYNC_ENABLED) && !F_ISSET(txn, WT_TXN_SYNC_SET))
                txn->txn_logsync = 0;
        } else {
            /*
             * If the caller already set sync on begin_transaction then they should not be using
             * sync on commit_transaction. Flag that as an error.
             */
            if (F_ISSET(txn, WT_TXN_SYNC_SET))
                WT_ERR_MSG(session, EINVAL, "sync already set during begin_transaction");
            if (WT_STRING_MATCH("off", cval.str, cval.len))
                txn->txn_logsync = 0;
            /*
             * We don't need to check for "on" here because that is the default to inherit from the
             * connection setting.
             */
        }

        /*
         * We hold the visibility lock for reading from the time we write our log record until the
         * time we release our transaction so that the LSN any checkpoint gets will always reflect
         * visible data.
         */
        __wt_readlock(session, &txn_global->visibility_rwlock);
        locked = true;
        WT_ERR(__wt_txn_log_commit(session, cfg));
    }

    /* Process updates. */
    for (i = 0, op = txn->mod; i < txn->mod_count; i++, op++) {
        switch (op->type) {
        case WT_TXN_OP_NONE:
            break;
        case WT_TXN_OP_BASIC_COL:
        case WT_TXN_OP_BASIC_ROW:
        case WT_TXN_OP_INMEM_COL:
        case WT_TXN_OP_INMEM_ROW:
            if (!prepare) {
                upd = op->u.op_upd;

                /*
                 * Switch reserved operations to abort to simplify obsolete update list truncation.
                 */
                if (upd->type == WT_UPDATE_RESERVE) {
                    upd->txnid = WT_TXN_ABORTED;
                    break;
                }

                /*
                 * Don't reset the timestamp of the history store records with history store
                 * transaction timestamp. Those records should already have the original time window
                 * when they are inserted into the history store.
                 */
                if (conn->cache->hs_fileid != 0 && op->btree->id == conn->cache->hs_fileid)
                    break;

                __wt_txn_op_set_timestamp(session, op);
                WT_ERR(__txn_timestamp_usage_check(session, op, upd));
            } else {
                /*
                 * If an operation has the key repeated flag set, skip resolving prepared updates as
                 * the work will happen on a different modification in this txn.
                 */
                if (!F_ISSET(op, WT_TXN_OP_KEY_REPEATED))
                    WT_ERR(__txn_resolve_prepared_op(session, op, true, &cursor));
#ifdef HAVE_DIAGNOSTIC
                ++prepare_count;
#endif
            }
            break;
        case WT_TXN_OP_REF_DELETE:
            __wt_txn_op_set_timestamp(session, op);
            break;
        case WT_TXN_OP_TRUNCATE_COL:
        case WT_TXN_OP_TRUNCATE_ROW:
            /* Other operations don't need timestamps. */
            break;
        }

        /* If we used the cursor to resolve prepared updates, the key now has been freed. */
        if (cursor != NULL)
            WT_CLEAR(cursor->key);
    }

    if (cursor != NULL) {
        WT_ERR(cursor->close(cursor));
        cursor = NULL;
    }

#ifdef HAVE_DIAGNOSTIC
    WT_ASSERT(session, txn->prepare_count == prepare_count);
    txn->prepare_count = 0;
#endif

    /*
     * Note: we're going to commit: nothing can fail after this point. Set a check, it's too easy to
     * call an error handling macro between here and the end of the function.
     */
    cannot_fail = true;

    /*
     * Free updates.
     *
     * Resolve any fast-truncate transactions and allow eviction to proceed on instantiated pages.
     * This isn't done as part of the initial processing because until now the commit could still
     * switch to an abort. The action allowing eviction to proceed is clearing the WT_UPDATE list,
     * (if any), associated with the commit. We're the only consumer of that list and we no longer
     * need it, and eviction knows it means abort or commit has completed on instantiated pages.
     */
    for (i = 0, op = txn->mod; i < txn->mod_count; i++, op++) {
        if (op->type == WT_TXN_OP_REF_DELETE) {
            WT_REF_LOCK(session, op->u.ref, &previous_state);

            /*
             * Only two cases are possible. First: the state is WT_REF_DELETED. In this case
             * ft_info.del cannot be NULL yet because an uncommitted operation cannot have reached
             * global visibility. Otherwise: there is an uncommitted delete operation we're
             * handling, so the page can't be in a non-deleted state, and the tree can't be
             * readonly. Therefore the page must have been instantiated, the state must be
             * WT_REF_MEM, and there should be an update list in ft_info.update.
             */
            if (previous_state == WT_REF_DELETED)
                op->u.ref->ft_info.del->committed = true;
            else
                __wt_free(session, op->u.ref->ft_info.update);
            WT_REF_UNLOCK(op->u.ref, previous_state);
        }
        __wt_txn_op_free(session, op);
    }
    txn->mod_count = 0;

    /*
     * If durable is set, we'll try to update the global durable timestamp with that value. If
     * durable isn't set, durable is implied to be the same as commit so we'll use that instead.
     */
    candidate_durable_timestamp = WT_TS_NONE;
    if (F_ISSET(txn, WT_TXN_HAS_TS_DURABLE))
        candidate_durable_timestamp = txn->durable_timestamp;
    else if (F_ISSET(txn, WT_TXN_HAS_TS_COMMIT))
        candidate_durable_timestamp = txn->commit_timestamp;

    __wt_txn_release(session);
    if (locked)
        __wt_readunlock(session, &txn_global->visibility_rwlock);

    /*
     * If we have made some updates visible, start a new commit generation: any cached snapshots
     * have to be refreshed.
     */
    if (!readonly)
        __wt_gen_next(session, WT_GEN_COMMIT, NULL);

    /* First check if we've made something durable in the future. */
    update_durable_ts = false;
    prev_durable_timestamp = WT_TS_NONE;
    if (candidate_durable_timestamp != WT_TS_NONE) {
        prev_durable_timestamp = txn_global->durable_timestamp;
        update_durable_ts = candidate_durable_timestamp > prev_durable_timestamp;
    }

    /*
     * If it looks like we'll need to move the global durable timestamp, attempt atomic cas and
     * re-check.
     */
    if (update_durable_ts)
        while (candidate_durable_timestamp > prev_durable_timestamp) {
            if (__wt_atomic_cas64(&txn_global->durable_timestamp, prev_durable_timestamp,
                  candidate_durable_timestamp)) {
                txn_global->has_durable_timestamp = true;
                break;
            }
            prev_durable_timestamp = txn_global->durable_timestamp;
        }

    /*
     * Stable timestamp cannot be concurrently increased greater than or equal to the prepared
     * transaction's durable timestamp. Otherwise, checkpoint may only write partial updates of the
     * transaction.
     */
    if (prepare && txn->durable_timestamp <= txn_global->stable_timestamp) {
        WT_ERR(__wt_verbose_dump_sessions(session, true));
        WT_ERR_PANIC(session, WT_PANIC,
          "stable timestamp is larger than or equal to the committing prepared transaction's "
          "durable timestamp");
    }

    /*
     * We're between transactions, if we need to block for eviction, it's a good time to do so.
     * Ignore error returns, the return must reflect the fate of the transaction.
     */
    if (!readonly)
        WT_IGNORE_RET(__wt_cache_eviction_check(session, false, false, NULL));

    return (0);

err:
    if (cursor != NULL)
        WT_TRET(cursor->close(cursor));

    if (locked)
        __wt_readunlock(session, &txn_global->visibility_rwlock);

    /* Check for a failure after we can no longer fail. */
    if (cannot_fail)
        WT_RET_PANIC(session, ret,
          "failed to commit a transaction after data corruption point, failing the system");

    /*
     * Check for a prepared transaction, and quit: we can't ignore the error and we can't roll back
     * a prepared transaction.
     */
    if (prepare)
        WT_RET_PANIC(session, ret, "failed to commit prepared transaction, failing the system");

    WT_TRET(__wt_session_reset_cursors(session, false));
    WT_TRET(__wt_txn_rollback(session, cfg));
    return (ret);
}

/*
 * __wt_txn_prepare --
 *     Prepare the current transaction.
 */
int
__wt_txn_prepare(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_TXN *txn;
    WT_TXN_OP *op;
    WT_UPDATE *upd, *tmp;
    u_int i, prepared_updates, prepared_updates_key_repeated;

    txn = session->txn;
    prepared_updates = prepared_updates_key_repeated = 0;

    WT_ASSERT(session, F_ISSET(txn, WT_TXN_RUNNING));
    WT_ASSERT(session, !F_ISSET(txn, WT_TXN_ERROR));

    /*
     * A transaction should not have updated any of the logged tables, if debug mode logging is not
     * turned on.
     */
    if (txn->logrec != NULL && !FLD_ISSET(S2C(session)->log_flags, WT_CONN_LOG_DEBUG_MODE))
        WT_RET_MSG(session, EINVAL, "a prepared transaction cannot include a logged table");

    /* Set the prepare timestamp. */
    WT_RET(__wt_txn_set_timestamp(session, cfg, false));

    if (!F_ISSET(txn, WT_TXN_HAS_TS_PREPARE))
        WT_RET_MSG(session, EINVAL, "prepare timestamp is not set");

    if (F_ISSET(txn, WT_TXN_HAS_TS_COMMIT))
        WT_RET_MSG(
          session, EINVAL, "commit timestamp must not be set before transaction is prepared");

    /*
     * We are about to release the snapshot: copy values into any positioned cursors so they don't
     * point to updates that could be freed once we don't have a snapshot.
     */
    if (session->ncursors > 0) {
        WT_DIAGNOSTIC_YIELD;
        WT_RET(__wt_session_copy_values(session));
    }

    for (i = 0, op = txn->mod; i < txn->mod_count; i++, op++) {
        /* Assert it's not an update to the history store file. */
        WT_ASSERT(session, S2C(session)->cache->hs_fileid == 0 || !WT_IS_HS(op->btree->dhandle));

        /* Metadata updates should never be prepared. */
        WT_ASSERT(session, !WT_IS_METADATA(op->btree->dhandle));
        if (WT_IS_METADATA(op->btree->dhandle))
            continue;

        /*
         * Logged table updates should never be prepared. As these updates are immediately durable,
         * it is not possible to roll them back if the prepared transaction is rolled back.
         */
        if (F_ISSET(op->btree, WT_BTREE_LOGGED))
            WT_RET_MSG(session, ENOTSUP,
              "%s: transaction prepare is not supported on logged tables or tables without "
              "timestamps",
              op->btree->dhandle->name);
        switch (op->type) {
        case WT_TXN_OP_NONE:
            break;
        case WT_TXN_OP_BASIC_COL:
        case WT_TXN_OP_BASIC_ROW:
        case WT_TXN_OP_INMEM_COL:
        case WT_TXN_OP_INMEM_ROW:
            upd = op->u.op_upd;

            /*
             * Switch reserved operation to abort to simplify obsolete update list truncation. The
             * object free function clears the operation type so we don't try to visit this update
             * again: it can be discarded.
             */
            if (upd->type == WT_UPDATE_RESERVE) {
                upd->txnid = WT_TXN_ABORTED;
                __wt_txn_op_free(session, op);
                break;
            }

            ++prepared_updates;

            /* Set prepare timestamp. */
            upd->start_ts = txn->prepare_timestamp;

            /*
             * By default durable timestamp is assigned with 0 which is same as WT_TS_NONE. Assign
             * it with WT_TS_NONE to make sure in case if we change the macro value it shouldn't be
             * a problem.
             */
            upd->durable_ts = WT_TS_NONE;

            WT_PUBLISH(upd->prepare_state, WT_PREPARE_INPROGRESS);
            op->u.op_upd = NULL;

            /*
             * If there are older updates to this key by the same transaction, set the repeated key
             * flag on this operation. This is later used in txn commit/rollback so we only resolve
             * each set of prepared updates once. Skip reserved updates, they're ignored as they're
             * simply discarded when we find them. Also ignore updates created by instantiating fast
             * truncation pages, they aren't linked into the transaction's modify list and so can't
             * be considered.
             */
            for (tmp = upd->next; tmp != NULL && tmp->txnid == upd->txnid; tmp = tmp->next)
                if (tmp->type != WT_UPDATE_RESERVE &&
                  !F_ISSET(tmp, WT_UPDATE_RESTORED_FAST_TRUNCATE)) {
                    F_SET(op, WT_TXN_OP_KEY_REPEATED);
                    ++prepared_updates_key_repeated;
                    break;
                }
            break;
        case WT_TXN_OP_REF_DELETE:
            __wt_txn_op_delete_apply_prepare_state(session, op->u.ref, false);
            break;
        case WT_TXN_OP_TRUNCATE_COL:
        case WT_TXN_OP_TRUNCATE_ROW:
            /* Other operations don't need timestamps. */
            break;
        }
    }
    WT_STAT_CONN_INCRV(session, txn_prepared_updates, prepared_updates);
    WT_STAT_CONN_INCRV(session, txn_prepared_updates_key_repeated, prepared_updates_key_repeated);
#ifdef HAVE_DIAGNOSTIC
    txn->prepare_count = prepared_updates;
#endif

    /* Set transaction state to prepare. */
    F_SET(session->txn, WT_TXN_PREPARE);

    /* Release our snapshot in case it is keeping data pinned. */
    __wt_txn_release_snapshot(session);

    /*
     * Clear the transaction's ID from the global table, to facilitate prepared data visibility, but
     * not from local transaction structure.
     */
    if (F_ISSET(txn, WT_TXN_HAS_ID))
        __txn_remove_from_global_table(session);

    return (0);
}

/*
 * __wt_txn_rollback --
 *     Roll back the current transaction.
 */
int
__wt_txn_rollback(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_TXN *txn;
    WT_TXN_OP *op;
    WT_UPDATE *upd;
    u_int i;
#ifdef HAVE_DIAGNOSTIC
    u_int prepare_count;
#endif
    bool prepare, readonly;

    cursor = NULL;
    txn = session->txn;
#ifdef HAVE_DIAGNOSTIC
    prepare_count = 0;
#endif
    prepare = F_ISSET(txn, WT_TXN_PREPARE);
    readonly = txn->mod_count == 0;

    WT_ASSERT(session, F_ISSET(txn, WT_TXN_RUNNING));

    /* Configure the timeout for this rollback operation. */
    WT_TRET(__txn_config_operation_timeout(session, cfg, true));

    /*
     * Resolving prepared updates is expensive. Sort prepared modifications so all updates for each
     * page within each file are done at the same time.
     */
    if (prepare)
        __wt_qsort(txn->mod, txn->mod_count, sizeof(WT_TXN_OP), __txn_mod_compare);

    /* Rollback and free updates. */
    for (i = 0, op = txn->mod; i < txn->mod_count; i++, op++) {
        /* Assert it's not an update to the history store file. */
        WT_ASSERT(session, S2C(session)->cache->hs_fileid == 0 || !WT_IS_HS(op->btree->dhandle));

        /* Metadata updates should never be rolled back. */
        WT_ASSERT(session, !WT_IS_METADATA(op->btree->dhandle));
        if (WT_IS_METADATA(op->btree->dhandle))
            continue;

        switch (op->type) {
        case WT_TXN_OP_NONE:
            break;
        case WT_TXN_OP_BASIC_COL:
        case WT_TXN_OP_BASIC_ROW:
        case WT_TXN_OP_INMEM_COL:
        case WT_TXN_OP_INMEM_ROW:
            upd = op->u.op_upd;

            if (!prepare) {
                if (S2C(session)->cache->hs_fileid != 0 &&
                  op->btree->id == S2C(session)->cache->hs_fileid)
                    break;
                WT_ASSERT(session, upd->txnid == txn->id || upd->txnid == WT_TXN_ABORTED);
                upd->txnid = WT_TXN_ABORTED;
            } else {
                /*
                 * If an operation has the key repeated flag set, skip resolving prepared updates as
                 * the work will happen on a different modification in this txn.
                 */
                if (!F_ISSET(op, WT_TXN_OP_KEY_REPEATED))
                    WT_TRET(__txn_resolve_prepared_op(session, op, false, &cursor));
#ifdef HAVE_DIAGNOSTIC
                ++prepare_count;
#endif
            }
            break;
        case WT_TXN_OP_REF_DELETE:
            WT_TRET(__wt_delete_page_rollback(session, op->u.ref));
            break;
        case WT_TXN_OP_TRUNCATE_COL:
        case WT_TXN_OP_TRUNCATE_ROW:
            /*
             * Nothing to do: these operations are only logged for recovery. The in-memory changes
             * will be rolled back with a combination of WT_TXN_OP_REF_DELETE and WT_TXN_OP_INMEM
             * operations.
             */
            break;
        }

        __wt_txn_op_free(session, op);
        /* If we used the cursor to resolve prepared updates, the key now has been freed. */
        if (cursor != NULL)
            WT_CLEAR(cursor->key);
    }
    txn->mod_count = 0;
#ifdef HAVE_DIAGNOSTIC
    WT_ASSERT(session, txn->prepare_count == prepare_count);
    txn->prepare_count = 0;
#endif

    if (cursor != NULL) {
        WT_TRET(cursor->close(cursor));
        cursor = NULL;
    }

    __wt_txn_release(session);

    /*
     * We're between transactions, if we need to block for eviction, it's a good time to do so.
     * Ignore error returns, the return must reflect the fate of the transaction.
     */
    if (!readonly)
        WT_IGNORE_RET(__wt_cache_eviction_check(session, false, false, NULL));

    return (ret);
}

/*
 * __wt_txn_rollback_required --
 *     Prepare to log a reason if the user attempts to use the transaction to do anything other than
 *     rollback.
 */
int
__wt_txn_rollback_required(WT_SESSION_IMPL *session, const char *reason)
{
    session->txn->rollback_reason = reason;
    return (WT_ROLLBACK);
}

/*
 * __wt_txn_init --
 *     Initialize a session's transaction data.
 */
int
__wt_txn_init(WT_SESSION_IMPL *session, WT_SESSION_IMPL *session_ret)
{
    WT_TXN *txn;

    /* Allocate the WT_TXN structure, including a variable length array of snapshot information. */
    WT_RET(__wt_calloc(session, 1,
      sizeof(WT_TXN) + sizeof(txn->snapshot[0]) * S2C(session)->session_size, &session_ret->txn));
    txn = session_ret->txn;
    txn->snapshot = txn->__snapshot;
    txn->id = WT_TXN_NONE;

    WT_ASSERT(session,
      S2C(session_ret)->txn_global.txn_shared_list == NULL ||
        WT_SESSION_TXN_SHARED(session_ret)->pinned_id == WT_TXN_NONE);

    /*
     * Take care to clean these out in case we are reusing the transaction for eviction.
     */
    txn->mod = NULL;

    txn->isolation = session_ret->isolation;
    return (0);
}

/*
 * __wt_txn_init_checkpoint_cursor --
 *     Create a transaction object for a checkpoint cursor. On success, takes charge of the snapshot
 *     array passed down, which should have been allocated separately, and nulls the pointer. (On
 *     failure, the caller must destroy it.)
 */
int
__wt_txn_init_checkpoint_cursor(
  WT_SESSION_IMPL *session, WT_CKPT_SNAPSHOT *snapinfo, WT_TXN **txn_ret)
{
    WT_TXN *txn;

    /*
     * Allocate the WT_TXN structure. Don't use the variable-length array at the end, because the
     * code for reading the snapshot allocates the snapshot list itself; copying it serves no
     * purpose, and twisting up the read code to allow controlling the allocation from here is not
     * worthwhile.
     *
     * Allocate a byte at the end so that __snapshot (at the end of the struct) doesn't point at an
     * adjacent malloc block; we'd like to be able to assert that in checkpoint cursor transactions
     * snapshot doesn't point at __snapshot, to make sure an ordinary transaction doesn't flow to
     * the checkpoint cursor close function. If an adjacent malloc block, that might not be true.
     */
    WT_RET(__wt_calloc(session, 1, sizeof(WT_TXN) + 1, &txn));

    /* We have no transaction ID and won't gain one, being read-only. */
    txn->id = WT_TXN_NONE;

    /* Use snapshot isolation. */
    txn->isolation = WT_ISO_SNAPSHOT;

    /* Save the snapshot data. */
    txn->snap_min = snapinfo->snapshot_min;
    txn->snap_max = snapinfo->snapshot_max;
    txn->snapshot = snapinfo->snapshot_txns;
    txn->snapshot_count = snapinfo->snapshot_count;

    /*
     * At this point we have taken charge of the snapshot's transaction list; it has been moved to
     * the dummy transaction. Null the caller's copy so it doesn't get freed twice if something
     * above us fails after we return.
     */
    snapinfo->snapshot_txns = NULL;

    /* Set the read and oldest timestamps.  */
    txn->checkpoint_read_timestamp = snapinfo->stable_ts;
    txn->checkpoint_oldest_timestamp = snapinfo->oldest_ts;

    /* Set the flag that indicates if we have a timestamp. */
    if (txn->checkpoint_read_timestamp != WT_TS_NONE)
        F_SET(txn, WT_TXN_SHARED_TS_READ);

    /*
     * Set other relevant flags. Always ignore prepared values; they can get into checkpoints.
     *
     * Prepared values don't get written out by checkpoints by default, but can appear if pages get
     * evicted. So whether any given prepared value from any given prepared but yet-uncommitted
     * transaction shows up or not is arbitrary and unpredictable. Therefore, failing on it serves
     * no data integrity purpose and will only make the system flaky.
     *
     * There is a problem, however. Prepared transactions are allowed to commit before stable if
     * stable moves forward, as long as the durable timestamp is after stable. Such transactions can
     * therefore be committed after (in execution time) the checkpoint is taken but with a commit
     * timestamp less than the checkpoint's stable timestamp. They will then exist in the live
     * database and be visible if read as of the checkpoint timestamp, but not exist in the
     * checkpoint, which is inconsistent. There is probably nothing that can be done about this
     * without making prepared transactions durable in prepared state, which is a Big Deal, so
     * applications using prepared transactions and using this commit leeway need to be cognizant of
     * the issue.
     */
    F_SET(txn,
      WT_TXN_HAS_SNAPSHOT | WT_TXN_IS_CHECKPOINT | WT_TXN_READONLY | WT_TXN_RUNNING |
        WT_TXN_IGNORE_PREPARE);

    *txn_ret = txn;
    return (0);
}

/*
 * __wt_txn_close_checkpoint_cursor --
 *     Dispose of the private transaction object in a checkpoint cursor.
 */
void
__wt_txn_close_checkpoint_cursor(WT_SESSION_IMPL *session, WT_TXN **txn_arg)
{
    WT_TXN *txn;

    txn = *txn_arg;
    *txn_arg = NULL;

    /* The snapshot list isn't at the end of the transaction structure here; free it explicitly. */
    WT_ASSERT(session, txn->snapshot != txn->__snapshot);
    __wt_free(session, txn->snapshot);

    __wt_free(session, txn);
}

/*
 * __wt_txn_stats_update --
 *     Update the transaction statistics for return to the application.
 */
void
__wt_txn_stats_update(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_CONNECTION_STATS **stats;
    WT_TXN_GLOBAL *txn_global;
    wt_timestamp_t checkpoint_timestamp;
    wt_timestamp_t durable_timestamp;
    wt_timestamp_t oldest_active_read_timestamp;
    wt_timestamp_t pinned_timestamp;
    uint64_t checkpoint_pinned;

    conn = S2C(session);
    txn_global = &conn->txn_global;
    stats = conn->stats;
    checkpoint_pinned = txn_global->checkpoint_txn_shared.pinned_id;

    WT_STAT_SET(session, stats, txn_pinned_range, txn_global->current - txn_global->oldest_id);

    checkpoint_timestamp = txn_global->checkpoint_timestamp;
    durable_timestamp = txn_global->durable_timestamp;
    pinned_timestamp = txn_global->pinned_timestamp;
    if (checkpoint_timestamp != WT_TS_NONE && checkpoint_timestamp < pinned_timestamp)
        pinned_timestamp = checkpoint_timestamp;
    WT_STAT_SET(session, stats, txn_pinned_timestamp, durable_timestamp - pinned_timestamp);
    WT_STAT_SET(
      session, stats, txn_pinned_timestamp_checkpoint, durable_timestamp - checkpoint_timestamp);
    WT_STAT_SET(session, stats, txn_pinned_timestamp_oldest,
      durable_timestamp - txn_global->oldest_timestamp);

    __wt_txn_get_pinned_timestamp(session, &oldest_active_read_timestamp, 0);
    if (oldest_active_read_timestamp == 0) {
        WT_STAT_SET(session, stats, txn_timestamp_oldest_active_read, 0);
        WT_STAT_SET(session, stats, txn_pinned_timestamp_reader, 0);
    } else {
        WT_STAT_SET(session, stats, txn_timestamp_oldest_active_read, oldest_active_read_timestamp);
        WT_STAT_SET(session, stats, txn_pinned_timestamp_reader,
          durable_timestamp - oldest_active_read_timestamp);
    }

    WT_STAT_SET(session, stats, txn_pinned_checkpoint_range,
      checkpoint_pinned == WT_TXN_NONE ? 0 : txn_global->current - checkpoint_pinned);

    WT_STAT_SET(session, stats, txn_checkpoint_prep_max, conn->ckpt_prep_max);
    if (conn->ckpt_prep_min != UINT64_MAX)
        WT_STAT_SET(session, stats, txn_checkpoint_prep_min, conn->ckpt_prep_min);
    WT_STAT_SET(session, stats, txn_checkpoint_prep_recent, conn->ckpt_prep_recent);
    WT_STAT_SET(session, stats, txn_checkpoint_prep_total, conn->ckpt_prep_total);
    WT_STAT_SET(session, stats, txn_checkpoint_time_max, conn->ckpt_time_max);
    if (conn->ckpt_time_min != UINT64_MAX)
        WT_STAT_SET(session, stats, txn_checkpoint_time_min, conn->ckpt_time_min);
    WT_STAT_SET(session, stats, txn_checkpoint_time_recent, conn->ckpt_time_recent);
    WT_STAT_SET(session, stats, txn_checkpoint_time_total, conn->ckpt_time_total);
}

/*
 * __wt_txn_release_resources --
 *     Release resources for a session's transaction data.
 */
void
__wt_txn_release_resources(WT_SESSION_IMPL *session)
{
    WT_TXN *txn;

    if ((txn = session->txn) == NULL)
        return;

    WT_ASSERT(session, txn->mod_count == 0);
    __wt_free(session, txn->mod);
    txn->mod_alloc = 0;
    txn->mod_count = 0;
}

/*
 * __wt_txn_destroy --
 *     Destroy a session's transaction data.
 */
void
__wt_txn_destroy(WT_SESSION_IMPL *session)
{
    __wt_txn_release_resources(session);
    __wt_free(session, session->txn);
}

/*
 * __wt_txn_global_init --
 *     Initialize the global transaction state.
 */
int
__wt_txn_global_init(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONNECTION_IMPL *conn;
    WT_TXN_GLOBAL *txn_global;
    WT_TXN_SHARED *s;
    u_int i;

    WT_UNUSED(cfg);
    conn = S2C(session);

    txn_global = &conn->txn_global;
    txn_global->current = txn_global->last_running = txn_global->metadata_pinned =
      txn_global->oldest_id = WT_TXN_FIRST;

    WT_RWLOCK_INIT_TRACKED(session, &txn_global->rwlock, txn_global);
    WT_RET(__wt_rwlock_init(session, &txn_global->visibility_rwlock));

    WT_RET(__wt_calloc_def(session, conn->session_size, &txn_global->txn_shared_list));

    for (i = 0, s = txn_global->txn_shared_list; i < conn->session_size; i++, s++)
        s->id = s->metadata_pinned = s->pinned_id = WT_TXN_NONE;

    return (0);
}

/*
 * __wt_txn_global_destroy --
 *     Destroy the global transaction state.
 */
void
__wt_txn_global_destroy(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_TXN_GLOBAL *txn_global;

    conn = S2C(session);
    txn_global = &conn->txn_global;

    if (txn_global == NULL)
        return;

    __wt_rwlock_destroy(session, &txn_global->rwlock);
    __wt_rwlock_destroy(session, &txn_global->visibility_rwlock);
    __wt_free(session, txn_global->txn_shared_list);
}

/*
 * __wt_txn_activity_drain --
 *     Wait for transactions to quiesce.
 */
int
__wt_txn_activity_drain(WT_SESSION_IMPL *session)
{
    bool txn_active;

    /*
     * It's possible that the eviction server is in the middle of a long operation, with a
     * transaction ID pinned. In that case, we will loop here until the transaction ID is released,
     * when the oldest transaction ID will catch up with the current ID.
     */
    for (;;) {
        WT_RET(__wt_txn_activity_check(session, &txn_active));
        if (!txn_active)
            break;

        WT_STAT_CONN_INCR(session, txn_release_blocked);
        __wt_yield();
    }

    return (0);
}

/*
 * __wt_txn_global_shutdown --
 *     Shut down the global transaction state.
 */
int
__wt_txn_global_shutdown(WT_SESSION_IMPL *session, const char **cfg)
{
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_SESSION_IMPL *s;
    char ts_string[WT_TS_INT_STRING_SIZE];
    const char *ckpt_cfg;
    bool use_timestamp;

    conn = S2C(session);
    use_timestamp = false;

    /*
     * Perform a system-wide checkpoint so that all tables are consistent with each other. All
     * transactions are resolved but ignore timestamps to make sure all data gets to disk. Do this
     * before shutting down all the subsystems. We have shut down all user sessions, but send in
     * true for waiting for internal races.
     */
    F_SET(conn, WT_CONN_CLOSING_CHECKPOINT);
    WT_TRET(__wt_config_gets(session, cfg, "use_timestamp", &cval));
    ckpt_cfg = "use_timestamp=false";
    if (cval.val != 0) {
        ckpt_cfg = "use_timestamp=true";
        if (conn->txn_global.has_stable_timestamp)
            use_timestamp = true;
    }
    if (!F_ISSET(conn, WT_CONN_IN_MEMORY | WT_CONN_READONLY | WT_CONN_PANIC)) {
        /*
         * Perform rollback to stable to ensure that the stable version is written to disk on a
         * clean shutdown.
         */
        if (use_timestamp) {
            __wt_verbose(session, WT_VERB_RTS,
              "performing shutdown rollback to stable with stable timestamp: %s",
              __wt_timestamp_to_string(conn->txn_global.stable_timestamp, ts_string));
            WT_TRET(__wt_rollback_to_stable(session, cfg, true));
        }

        s = NULL;
        WT_TRET(__wt_open_internal_session(conn, "close_ckpt", true, 0, 0, &s));
        if (s != NULL) {
            const char *checkpoint_cfg[] = {
              WT_CONFIG_BASE(session, WT_SESSION_checkpoint), ckpt_cfg, NULL};
            WT_TRET(__wt_txn_checkpoint(s, checkpoint_cfg, true));

            /*
             * Mark the metadata dirty so we flush it on close, allowing recovery to be skipped.
             */
            WT_WITH_DHANDLE(s, WT_SESSION_META_DHANDLE(s), __wt_tree_modify_set(s));

            WT_TRET(__wt_session_close_internal(s));
        }
    }

    return (ret);
}

/*
 * __wt_txn_is_blocking --
 *     Return an error if this transaction is likely blocking eviction because of a pinned
 *     transaction ID, called by eviction to determine if a worker thread should be released from
 *     eviction.
 */
int
__wt_txn_is_blocking(WT_SESSION_IMPL *session)
{
    WT_TXN *txn;
    WT_TXN_SHARED *txn_shared;
    uint64_t global_oldest;

    txn = session->txn;
    txn_shared = WT_SESSION_TXN_SHARED(session);
    global_oldest = S2C(session)->txn_global.oldest_id;

    /* We can't roll back prepared transactions. */
    if (F_ISSET(txn, WT_TXN_PREPARE))
        return (0);

#ifndef WT_STANDALONE_BUILD
    /*
     * FIXME: SERVER-44870
     *
     * MongoDB can't (yet) handle rolling back read only transactions. For this reason, don't check
     * unless there's at least one update or we're configured to time out thread operations (a way
     * to confirm our caller is prepared for rollback).
     */
    if (txn->mod_count == 0 && !__wt_op_timer_fired(session))
        return (0);
#endif

    /*
     * Check if either the transaction's ID or its pinned ID is equal to the oldest transaction ID.
     */
    return (txn_shared->id == global_oldest || txn_shared->pinned_id == global_oldest ?
        __wt_txn_rollback_required(session, WT_TXN_ROLLBACK_REASON_OLDEST_FOR_EVICTION) :
        0);
}

/*
 * __wt_verbose_dump_txn_one --
 *     Output diagnostic information about a transaction structure.
 */
int
__wt_verbose_dump_txn_one(
  WT_SESSION_IMPL *session, WT_SESSION_IMPL *txn_session, int error_code, const char *error_string)
{
    WT_TXN *txn;
    WT_TXN_SHARED *txn_shared;
    char buf[512];
    char ts_string[6][WT_TS_INT_STRING_SIZE];
    const char *iso_tag;

    txn = txn_session->txn;
    txn_shared = WT_SESSION_TXN_SHARED(txn_session);

    WT_NOT_READ(iso_tag, "INVALID");
    switch (txn->isolation) {
    case WT_ISO_READ_COMMITTED:
        iso_tag = "WT_ISO_READ_COMMITTED";
        break;
    case WT_ISO_READ_UNCOMMITTED:
        iso_tag = "WT_ISO_READ_UNCOMMITTED";
        break;
    case WT_ISO_SNAPSHOT:
        iso_tag = "WT_ISO_SNAPSHOT";
        break;
    }

    /*
     * Dump the information of the passed transaction into a buffer, to be logged with an optional
     * error message.
     */
    WT_RET(
      __wt_snprintf(buf, sizeof(buf),
        "transaction id: %" PRIu64 ", mod count: %u"
        ", snap min: %" PRIu64 ", snap max: %" PRIu64 ", snapshot count: %u"
        ", commit_timestamp: %s"
        ", durable_timestamp: %s"
        ", first_commit_timestamp: %s"
        ", prepare_timestamp: %s"
        ", pinned_durable_timestamp: %s"
        ", read_timestamp: %s"
        ", checkpoint LSN: [%" PRIu32 "][%" PRIu32 "]"
        ", full checkpoint: %s"
        ", rollback reason: %s"
        ", flags: 0x%08" PRIx32 ", isolation: %s",
        txn->id, txn->mod_count, txn->snap_min, txn->snap_max, txn->snapshot_count,
        __wt_timestamp_to_string(txn->commit_timestamp, ts_string[0]),
        __wt_timestamp_to_string(txn->durable_timestamp, ts_string[1]),
        __wt_timestamp_to_string(txn->first_commit_timestamp, ts_string[2]),
        __wt_timestamp_to_string(txn->prepare_timestamp, ts_string[3]),
        __wt_timestamp_to_string(txn_shared->pinned_durable_timestamp, ts_string[4]),
        __wt_timestamp_to_string(txn_shared->read_timestamp, ts_string[5]), txn->ckpt_lsn.l.file,
        txn->ckpt_lsn.l.offset, txn->full_ckpt ? "true" : "false",
        txn->rollback_reason == NULL ? "" : txn->rollback_reason, txn->flags, iso_tag));

    /*
     * Log a message and return an error if error code and an optional error string has been passed.
     */
    if (0 != error_code) {
        WT_RET_MSG(session, error_code, "%s, %s", buf, error_string != NULL ? error_string : "");
    } else {
        WT_RET(__wt_msg(session, "%s", buf));
    }

    return (0);
}

/*
 * __wt_verbose_dump_txn --
 *     Output diagnostic information about the global transaction state.
 */
int
__wt_verbose_dump_txn(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_SESSION_IMPL *sess;
    WT_TXN_GLOBAL *txn_global;
    WT_TXN_SHARED *s;
    uint64_t id;
    uint32_t i, session_cnt;
    char ts_string[WT_TS_INT_STRING_SIZE];

    conn = S2C(session);
    txn_global = &conn->txn_global;

    WT_RET(__wt_msg(session, "%s", WT_DIVIDER));
    WT_RET(__wt_msg(session, "transaction state dump"));

    WT_RET(__wt_msg(session, "current ID: %" PRIu64, txn_global->current));
    WT_RET(__wt_msg(session, "last running ID: %" PRIu64, txn_global->last_running));
    WT_RET(__wt_msg(session, "metadata_pinned ID: %" PRIu64, txn_global->metadata_pinned));
    WT_RET(__wt_msg(session, "oldest ID: %" PRIu64, txn_global->oldest_id));

    WT_RET(__wt_msg(session, "durable timestamp: %s",
      __wt_timestamp_to_string(txn_global->durable_timestamp, ts_string)));
    WT_RET(__wt_msg(session, "oldest timestamp: %s",
      __wt_timestamp_to_string(txn_global->oldest_timestamp, ts_string)));
    WT_RET(__wt_msg(session, "pinned timestamp: %s",
      __wt_timestamp_to_string(txn_global->pinned_timestamp, ts_string)));
    WT_RET(__wt_msg(session, "stable timestamp: %s",
      __wt_timestamp_to_string(txn_global->stable_timestamp, ts_string)));
    WT_RET(__wt_msg(
      session, "has_durable_timestamp: %s", txn_global->has_durable_timestamp ? "yes" : "no"));
    WT_RET(__wt_msg(
      session, "has_oldest_timestamp: %s", txn_global->has_oldest_timestamp ? "yes" : "no"));
    WT_RET(__wt_msg(
      session, "has_pinned_timestamp: %s", txn_global->has_pinned_timestamp ? "yes" : "no"));
    WT_RET(__wt_msg(
      session, "has_stable_timestamp: %s", txn_global->has_stable_timestamp ? "yes" : "no"));
    WT_RET(__wt_msg(session, "oldest_is_pinned: %s", txn_global->oldest_is_pinned ? "yes" : "no"));
    WT_RET(__wt_msg(session, "stable_is_pinned: %s", txn_global->stable_is_pinned ? "yes" : "no"));

    WT_RET(
      __wt_msg(session, "checkpoint running: %s", txn_global->checkpoint_running ? "yes" : "no"));
    WT_RET(
      __wt_msg(session, "checkpoint generation: %" PRIu64, __wt_gen(session, WT_GEN_CHECKPOINT)));
    WT_RET(__wt_msg(
      session, "checkpoint pinned ID: %" PRIu64, txn_global->checkpoint_txn_shared.pinned_id));
    WT_RET(__wt_msg(session, "checkpoint txn ID: %" PRIu64, txn_global->checkpoint_txn_shared.id));

    WT_ORDERED_READ(session_cnt, conn->session_cnt);
    WT_RET(__wt_msg(session, "session count: %" PRIu32, session_cnt));
    WT_RET(__wt_msg(session, "Transaction state of active sessions:"));

    /*
     * Walk each session transaction state and dump information. Accessing the content of session
     * handles is not thread safe, so some information may change while traversing if other threads
     * are active at the same time, which is OK since this is diagnostic code.
     */
    WT_STAT_CONN_INCR(session, txn_walk_sessions);
    for (i = 0, s = txn_global->txn_shared_list; i < session_cnt; i++, s++) {
        WT_STAT_CONN_INCR(session, txn_sessions_walked);
        /* Skip sessions with no active transaction */
        if ((id = s->id) == WT_TXN_NONE && s->pinned_id == WT_TXN_NONE)
            continue;
        sess = &conn->sessions[i];
        WT_RET(__wt_msg(session,
          "ID: %" PRIu64 ", pinned ID: %" PRIu64 ", metadata pinned ID: %" PRIu64 ", name: %s", id,
          s->pinned_id, s->metadata_pinned, sess->name == NULL ? "EMPTY" : sess->name));
        WT_RET(__wt_verbose_dump_txn_one(session, sess, 0, NULL));
    }

    return (0);
}
