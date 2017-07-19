/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __snapsort_partition --
 *	Custom quick sort partitioning for snapshots.
 */
static uint32_t
__snapsort_partition(uint64_t *array, uint32_t f, uint32_t l, uint64_t pivot)
{
	uint32_t i = f - 1, j = l + 1;

	for (;;) {
		while (pivot < array[--j])
			;
		while (array[++i] < pivot)
			;
		if (i<j) {
			uint64_t tmp = array[i];
			array[i] = array[j];
			array[j] = tmp;
		} else
			return (j);
	}
}

/*
 * __snapsort_impl --
 *	Custom quick sort implementation for snapshots.
 */
static void
__snapsort_impl(uint64_t *array, uint32_t f, uint32_t l)
{
	while (f + 16 < l) {
		uint64_t v1 = array[f], v2 = array[l], v3 = array[(f + l)/2];
		uint64_t median = v1 < v2 ?
		    (v3 < v1 ? v1 : WT_MIN(v2, v3)) :
		    (v3 < v2 ? v2 : WT_MIN(v1, v3));
		uint32_t m = __snapsort_partition(array, f, l, median);
		__snapsort_impl(array, f, m);
		f = m + 1;
	}
}

/*
 * __snapsort --
 *	Sort an array of transaction IDs.
 */
static void
__snapsort(uint64_t *array, uint32_t size)
{
	__snapsort_impl(array, 0, size - 1);
	WT_INSERTION_SORT(array, size, uint64_t, WT_TXNID_LT);
}

/*
 * __txn_sort_snapshot --
 *	Sort a snapshot for faster searching and set the min/max bounds.
 */
static void
__txn_sort_snapshot(WT_SESSION_IMPL *session, uint32_t n, uint64_t snap_max)
{
	WT_TXN *txn;

	txn = &session->txn;

	if (n > 1)
		__snapsort(txn->snapshot, n);

	txn->snapshot_count = n;
	txn->snap_max = snap_max;
	txn->snap_min = (n > 0 && WT_TXNID_LE(txn->snapshot[0], snap_max)) ?
	    txn->snapshot[0] : snap_max;
	F_SET(txn, WT_TXN_HAS_SNAPSHOT);
	WT_ASSERT(session, n == 0 || txn->snap_min != WT_TXN_NONE);
}

/*
 * __wt_txn_release_snapshot --
 *	Release the snapshot in the current transaction.
 */
void
__wt_txn_release_snapshot(WT_SESSION_IMPL *session)
{
	WT_TXN *txn;
	WT_TXN_STATE *txn_state;

	txn = &session->txn;
	txn_state = WT_SESSION_TXN_STATE(session);

	WT_ASSERT(session,
	    txn_state->pinned_id == WT_TXN_NONE ||
	    session->txn.isolation == WT_ISO_READ_UNCOMMITTED ||
	    !__wt_txn_visible_all(session, txn_state->pinned_id, NULL));

	txn_state->metadata_pinned = txn_state->pinned_id = WT_TXN_NONE;
	F_CLR(txn, WT_TXN_HAS_SNAPSHOT);
}

/*
 * __wt_txn_get_snapshot --
 *	Allocate a snapshot.
 */
void
__wt_txn_get_snapshot(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_TXN *txn;
	WT_TXN_GLOBAL *txn_global;
	WT_TXN_STATE *s, *txn_state;
	uint64_t current_id, id;
	uint64_t prev_oldest_id, pinned_id;
	uint32_t i, n, session_cnt;

	conn = S2C(session);
	txn = &session->txn;
	txn_global = &conn->txn_global;
	txn_state = WT_SESSION_TXN_STATE(session);
	n = 0;

	/* We're going to scan the table: wait for the lock. */
	__wt_readlock(session, &txn_global->rwlock);

	current_id = pinned_id = txn_global->current;
	prev_oldest_id = txn_global->oldest_id;

	/*
	 * Include the checkpoint transaction, if one is running: we should
	 * ignore any uncommitted changes the checkpoint has written to the
	 * metadata.  We don't have to keep the checkpoint's changes pinned so
	 * don't including it in the published pinned ID.
	 */
	if ((id = txn_global->checkpoint_state.id) != WT_TXN_NONE) {
		txn->snapshot[n++] = id;
		txn_state->metadata_pinned = id;
	}

	/* For pure read-only workloads, avoid scanning. */
	if (prev_oldest_id == current_id) {
		txn_state->pinned_id = current_id;
		/* Check that the oldest ID has not moved in the meantime. */
		WT_ASSERT(session, prev_oldest_id == txn_global->oldest_id);
		goto done;
	}

	/* Walk the array of concurrent transactions. */
	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	for (i = 0, s = txn_global->states; i < session_cnt; i++, s++) {
		/*
		 * Build our snapshot of any concurrent transaction IDs.
		 *
		 * Ignore:
		 *  - Our own ID: we always read our own updates.
		 *  - The ID if it is older than the oldest ID we saw. This
		 *    can happen if we race with a thread that is allocating
		 *    an ID -- the ID will not be used because the thread will
		 *    keep spinning until it gets a valid one.
		 */
		if (s != txn_state &&
		    (id = s->id) != WT_TXN_NONE &&
		    WT_TXNID_LE(prev_oldest_id, id)) {
			txn->snapshot[n++] = id;
			if (WT_TXNID_LT(id, pinned_id))
				pinned_id = id;
		}
	}

	/*
	 * If we got a new snapshot, update the published pinned ID for this
	 * session.
	 */
	WT_ASSERT(session, WT_TXNID_LE(prev_oldest_id, pinned_id));
	WT_ASSERT(session, prev_oldest_id == txn_global->oldest_id);
	txn_state->pinned_id = pinned_id;

done:	__wt_readunlock(session, &txn_global->rwlock);
	__txn_sort_snapshot(session, n, current_id);
}

/*
 * __txn_oldest_scan --
 *	Sweep the running transactions to calculate the oldest ID required.
 */
static void
__txn_oldest_scan(WT_SESSION_IMPL *session,
    uint64_t *oldest_idp, uint64_t *last_runningp, uint64_t *metadata_pinnedp,
    WT_SESSION_IMPL **oldest_sessionp)
{
	WT_CONNECTION_IMPL *conn;
	WT_SESSION_IMPL *oldest_session;
	WT_TXN_GLOBAL *txn_global;
	WT_TXN_STATE *s;
	uint64_t id, last_running, metadata_pinned, oldest_id, prev_oldest_id;
	uint32_t i, session_cnt;

	conn = S2C(session);
	txn_global = &conn->txn_global;
	oldest_session = NULL;

	/* The oldest ID cannot change while we are holding the scan lock. */
	prev_oldest_id = txn_global->oldest_id;
	last_running = oldest_id = txn_global->current;
	if ((metadata_pinned = txn_global->checkpoint_state.id) == WT_TXN_NONE)
		metadata_pinned = oldest_id;

	/* Walk the array of concurrent transactions. */
	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	for (i = 0, s = txn_global->states; i < session_cnt; i++, s++) {
		/* Update the last running transaction ID. */
		if ((id = s->id) != WT_TXN_NONE &&
		    WT_TXNID_LE(prev_oldest_id, id) &&
		    WT_TXNID_LT(id, last_running))
			last_running = id;

		/* Update the metadata pinned ID. */
		if ((id = s->metadata_pinned) != WT_TXN_NONE &&
		    WT_TXNID_LT(id, metadata_pinned))
			metadata_pinned = id;

		/*
		 * !!!
		 * Note: Don't ignore pinned ID values older than the previous
		 * oldest ID.  Read-uncommitted operations publish pinned ID
		 * values without acquiring the scan lock to protect the global
		 * table.  See the comment in __wt_txn_cursor_op for more
		 * details.
		 */
		if ((id = s->pinned_id) != WT_TXN_NONE &&
		    WT_TXNID_LT(id, oldest_id)) {
			oldest_id = id;
			oldest_session = &conn->sessions[i];
		}
	}

	if (WT_TXNID_LT(last_running, oldest_id))
		oldest_id = last_running;

	/* The oldest ID can't move past any named snapshots. */
	if ((id = txn_global->nsnap_oldest_id) != WT_TXN_NONE &&
	    WT_TXNID_LT(id, oldest_id))
		oldest_id = id;

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
 *	Sweep the running transactions to update the oldest ID required.
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

#ifdef HAVE_TIMESTAMPS
	/* Try to move the pinned timestamp forward. */
	if (strict)
		WT_RET(__wt_txn_update_pinned_timestamp(session));
#endif

	/*
	 * For pure read-only workloads, or if the update isn't forced and the
	 * oldest ID isn't too far behind, avoid scanning.
	 */
	if ((prev_oldest_id == current_id &&
	    prev_metadata_pinned == current_id) ||
	    (!strict && WT_TXNID_LT(current_id, prev_oldest_id + 100)))
		return (0);

	/* First do a read-only scan. */
	if (wait)
		__wt_readlock(session, &txn_global->rwlock);
	else if ((ret =
	    __wt_try_readlock(session, &txn_global->rwlock)) != 0)
		return (ret == EBUSY ? 0 : ret);
	__txn_oldest_scan(session,
	    &oldest_id, &last_running, &metadata_pinned, &oldest_session);
	__wt_readunlock(session, &txn_global->rwlock);

	/*
	 * If the state hasn't changed (or hasn't moved far enough for
	 * non-forced updates), give up.
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
	else if ((ret =
	    __wt_try_writelock(session, &txn_global->rwlock)) != 0)
		return (ret == EBUSY ? 0 : ret);

	/*
	 * If the oldest ID has been updated while we waited, don't bother
	 * scanning.
	 */
	if (WT_TXNID_LE(oldest_id, txn_global->oldest_id) &&
	    WT_TXNID_LE(last_running, txn_global->last_running) &&
	    WT_TXNID_LE(metadata_pinned, txn_global->metadata_pinned))
		goto done;

	/*
	 * Re-scan now that we have exclusive access.  This is necessary because
	 * threads get transaction snapshots with read locks, and we have to be
	 * sure that there isn't a thread that has got a snapshot locally but
	 * not yet published its snap_min.
	 */
	__txn_oldest_scan(session,
	    &oldest_id, &last_running, &metadata_pinned, &oldest_session);

#ifdef HAVE_DIAGNOSTIC
	{
	/*
	 * Make sure the ID doesn't move past any named snapshots.
	 *
	 * Don't include the read/assignment in the assert statement.  Coverity
	 * complains if there are assignments only done in diagnostic builds,
	 * and when the read is from a volatile.
	 */
	uint64_t id = txn_global->nsnap_oldest_id;
	WT_ASSERT(session,
	    id == WT_TXN_NONE || !WT_TXNID_LT(id, oldest_id));
	}
#endif
	/* Update the public IDs. */
	if (WT_TXNID_LT(txn_global->metadata_pinned, metadata_pinned))
		txn_global->metadata_pinned = metadata_pinned;
	if (WT_TXNID_LT(txn_global->oldest_id, oldest_id))
		txn_global->oldest_id = oldest_id;
	if (WT_TXNID_LT(txn_global->last_running, last_running)) {
		txn_global->last_running = last_running;

#ifdef HAVE_VERBOSE
		/* Output a verbose message about long-running transactions,
		 * but only when some progress is being made. */
		if (WT_VERBOSE_ISSET(session, WT_VERB_TRANSACTION) &&
		    current_id - oldest_id > 10000 && oldest_session != NULL) {
			__wt_verbose(session, WT_VERB_TRANSACTION,
			    "old snapshot %" PRIu64
			    " pinned in session %" PRIu32 " [%s]"
			    " with snap_min %" PRIu64,
			    oldest_id, oldest_session->id,
			    oldest_session->lastop,
			    oldest_session->txn.snap_min);
		}
#endif
	}

done:	__wt_writeunlock(session, &txn_global->rwlock);
	return (ret);
}

/*
 * __wt_txn_config --
 *	Configure a transaction.
 */
int
__wt_txn_config(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_TXN *txn;

	txn = &session->txn;

	WT_RET(__wt_config_gets_def(session, cfg, "isolation", 0, &cval));
	if (cval.len != 0)
		txn->isolation =
		    WT_STRING_MATCH("snapshot", cval.str, cval.len) ?
		    WT_ISO_SNAPSHOT :
		    WT_STRING_MATCH("read-committed", cval.str, cval.len) ?
		    WT_ISO_READ_COMMITTED : WT_ISO_READ_UNCOMMITTED;

	/*
	 * The default sync setting is inherited from the connection, but can
	 * be overridden by an explicit "sync" setting for this transaction.
	 *
	 * We want to distinguish between inheriting implicitly and explicitly.
	 */
	F_CLR(txn, WT_TXN_SYNC_SET);
	WT_RET(__wt_config_gets_def(
	    session, cfg, "sync", (int)UINT_MAX, &cval));
	if (cval.val == 0 || cval.val == 1)
		/*
		 * This is an explicit setting of sync.  Set the flag so
		 * that we know not to overwrite it in commit_transaction.
		 */
		F_SET(txn, WT_TXN_SYNC_SET);

	/*
	 * If sync is turned off explicitly, clear the transaction's sync field.
	 */
	if (cval.val == 0)
		txn->txn_logsync = 0;

	WT_RET(__wt_config_gets_def(session, cfg, "snapshot", 0, &cval));
	if (cval.len > 0)
		/*
		 * The layering here isn't ideal - the named snapshot get
		 * function does both validation and setup. Otherwise we'd
		 * need to walk the list of named snapshots twice during
		 * transaction open.
		 */
		WT_RET(__wt_txn_named_snapshot_get(session, &cval));

	WT_RET(__wt_config_gets_def(session, cfg, "read_timestamp", 0, &cval));
	if (cval.len > 0) {
#ifdef HAVE_TIMESTAMPS
		WT_TXN_GLOBAL *txn_global = &S2C(session)->txn_global;
		wt_timestamp_t oldest_timestamp;

		WT_RET(__wt_txn_parse_timestamp(
		    session, "read", txn->read_timestamp, &cval));
		__wt_readlock(session, &txn_global->rwlock);
		__wt_timestamp_set(
		    oldest_timestamp, txn_global->oldest_timestamp);
		__wt_readunlock(session, &txn_global->rwlock);
		if (__wt_timestamp_cmp(
		    txn->read_timestamp, oldest_timestamp) < 0)
			WT_RET_MSG(session, EINVAL,
			    "read timestamp %.*s older than oldest timestamp",
			    (int)cval.len, cval.str);

		__wt_txn_set_read_timestamp(session);
		txn->isolation = WT_ISO_SNAPSHOT;
#else
		WT_RET_MSG(session, EINVAL, "read_timestamp requires a "
		    "version of WiredTiger built with timestamp support");
#endif
	}

	return (0);
}

/*
 * __wt_txn_reconfigure --
 *	WT_SESSION::reconfigure for transactions.
 */
int
__wt_txn_reconfigure(WT_SESSION_IMPL *session, const char *config)
{
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	WT_TXN *txn;

	txn = &session->txn;

	ret = __wt_config_getones(session, config, "isolation", &cval);
	if (ret == 0 && cval.len != 0) {
		session->isolation = txn->isolation =
		    WT_STRING_MATCH("snapshot", cval.str, cval.len) ?
		    WT_ISO_SNAPSHOT :
		    WT_STRING_MATCH("read-uncommitted", cval.str, cval.len) ?
		    WT_ISO_READ_UNCOMMITTED : WT_ISO_READ_COMMITTED;
	}
	WT_RET_NOTFOUND_OK(ret);

	return (0);
}

/*
 * __wt_txn_release --
 *	Release the resources associated with the current transaction.
 */
void
__wt_txn_release(WT_SESSION_IMPL *session)
{
	WT_TXN *txn;
	WT_TXN_GLOBAL *txn_global;
	WT_TXN_STATE *txn_state;

	txn = &session->txn;
	txn_global = &S2C(session)->txn_global;
	txn_state = WT_SESSION_TXN_STATE(session);

	WT_ASSERT(session, txn->mod_count == 0);
	txn->notify = NULL;

	/* Clear the transaction's ID from the global table. */
	if (WT_SESSION_IS_CHECKPOINT(session)) {
		WT_ASSERT(session, txn_state->id == WT_TXN_NONE);
		txn->id = txn_global->checkpoint_state.id =
		    txn_global->checkpoint_state.pinned_id = WT_TXN_NONE;

		/*
		 * Be extra careful to cleanup everything for checkpoints: once
		 * the global checkpoint ID is cleared, we can no longer tell
		 * if this session is doing a checkpoint.
		 */
		txn_global->checkpoint_id = 0;
	} else if (F_ISSET(txn, WT_TXN_HAS_ID)) {
		WT_ASSERT(session,
		    !WT_TXNID_LT(txn->id, txn_global->last_running));

		WT_ASSERT(session, txn_state->id != WT_TXN_NONE &&
		    txn->id != WT_TXN_NONE);
		WT_PUBLISH(txn_state->id, WT_TXN_NONE);

		txn->id = WT_TXN_NONE;
	}

#ifdef HAVE_TIMESTAMPS
	__wt_txn_clear_commit_timestamp(session);
	__wt_txn_clear_read_timestamp(session);
#endif

	/* Free the scratch buffer allocated for logging. */
	__wt_logrec_free(session, &txn->logrec);

	/* Discard any memory from the session's stash that we can. */
	WT_ASSERT(session, __wt_session_gen(session, WT_GEN_SPLIT) == 0);
	__wt_stash_discard(session);

	/*
	 * Reset the transaction state to not running and release the snapshot.
	 */
	__wt_txn_release_snapshot(session);
	txn->isolation = session->isolation;

	/* Ensure the transaction flags are cleared on exit */
	txn->flags = 0;
}

/*
 * __wt_txn_commit --
 *	Commit the current transaction.
 */
int
__wt_txn_commit(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_TXN *txn;
	WT_TXN_OP *op;
#ifdef HAVE_TIMESTAMPS
	WT_TXN_GLOBAL *txn_global = &S2C(session)->txn_global;
	wt_timestamp_t prev_commit_timestamp;
	bool update_timestamp;
#endif
	u_int i;
	bool did_update;

	txn = &session->txn;
	conn = S2C(session);
	did_update = txn->mod_count != 0;

	WT_ASSERT(session, F_ISSET(txn, WT_TXN_RUNNING));
	WT_ASSERT(session, !F_ISSET(txn, WT_TXN_ERROR) || !did_update);

	/*
	 * Look for a commit timestamp.
	 */
	WT_ERR(
	    __wt_config_gets_def(session, cfg, "commit_timestamp", 0, &cval));
	if (cval.len != 0) {
#ifdef HAVE_TIMESTAMPS
		WT_ERR(__wt_txn_parse_timestamp(
		    session, "commit", txn->commit_timestamp, &cval));
		__wt_txn_set_commit_timestamp(session);
#else
		WT_ERR_MSG(session, EINVAL, "commit_timestamp requires a "
		    "version of WiredTiger built with timestamp support");
#endif
	}

	/*
	 * The default sync setting is inherited from the connection, but can
	 * be overridden by an explicit "sync" setting for this transaction.
	 */
	WT_ERR(__wt_config_gets_def(session, cfg, "sync", 0, &cval));

	/*
	 * If the user chose the default setting, check whether sync is enabled
	 * for this transaction (either inherited or via begin_transaction).
	 * If sync is disabled, clear the field to avoid the log write being
	 * flushed.
	 *
	 * Otherwise check for specific settings.  We don't need to check for
	 * "on" because that is the default inherited from the connection.  If
	 * the user set anything in begin_transaction, we only override with an
	 * explicit setting.
	 */
	if (cval.len == 0) {
		if (!FLD_ISSET(txn->txn_logsync, WT_LOG_SYNC_ENABLED) &&
		    !F_ISSET(txn, WT_TXN_SYNC_SET))
			txn->txn_logsync = 0;
	} else {
		/*
		 * If the caller already set sync on begin_transaction then
		 * they should not be using sync on commit_transaction.
		 * Flag that as an error.
		 */
		if (F_ISSET(txn, WT_TXN_SYNC_SET))
			WT_ERR_MSG(session, EINVAL,
			    "Sync already set during begin_transaction");
		if (WT_STRING_MATCH("background", cval.str, cval.len))
			txn->txn_logsync = WT_LOG_BACKGROUND;
		else if (WT_STRING_MATCH("off", cval.str, cval.len))
			txn->txn_logsync = 0;
		/*
		 * We don't need to check for "on" here because that is the
		 * default to inherit from the connection setting.
		 */
	}

	/* Commit notification. */
	if (txn->notify != NULL)
		WT_ERR(txn->notify->notify(txn->notify,
		    (WT_SESSION *)session, txn->id, 1));

	/*
	 * We are about to release the snapshot: copy values into any
	 * positioned cursors so they don't point to updates that could be
	 * freed once we don't have a snapshot.
	 */
	if (session->ncursors > 0) {
		WT_DIAGNOSTIC_YIELD;
		WT_ERR(__wt_session_copy_values(session));
	}

	/* If we are logging, write a commit log record. */
	if (did_update &&
	    FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED) &&
	    !F_ISSET(session, WT_SESSION_NO_LOGGING)) {
		/*
		 * We are about to block on I/O writing the log.
		 * Release our snapshot in case it is keeping data pinned.
		 * This is particularly important for checkpoints.
		 */
		__wt_txn_release_snapshot(session);
		WT_ERR(__wt_txn_log_commit(session, cfg));
	}

	/* Note: we're going to commit: nothing can fail after this point. */

	/* Process and free updates. */
	for (i = 0, op = txn->mod; i < txn->mod_count; i++, op++) {
		switch (op->type) {
		case WT_TXN_OP_BASIC:
		case WT_TXN_OP_BASIC_TS:
		case WT_TXN_OP_INMEM:
			/*
			 * Switch reserved operations to abort to
			 * simplify obsolete update list truncation.
			 */
			if (op->u.upd->type == WT_UPDATE_RESERVED) {
				op->u.upd->txnid = WT_TXN_ABORTED;
				break;
			}

#ifdef HAVE_TIMESTAMPS
			if (F_ISSET(txn, WT_TXN_HAS_TS_COMMIT) &&
			    op->type != WT_TXN_OP_BASIC_TS)
				__wt_timestamp_set(op->u.upd->timestamp,
				    txn->commit_timestamp);
#endif
			break;

		case WT_TXN_OP_REF:
#ifdef HAVE_TIMESTAMPS
			if (F_ISSET(txn, WT_TXN_HAS_TS_COMMIT))
				__wt_timestamp_set(
				    op->u.ref->page_del->timestamp,
				    txn->commit_timestamp);
#endif
			break;

		case WT_TXN_OP_TRUNCATE_COL:
		case WT_TXN_OP_TRUNCATE_ROW:
			/* Other operations don't need timestamps. */
			break;
		}

		__wt_txn_op_free(session, op);
	}
	txn->mod_count = 0;

#ifdef HAVE_TIMESTAMPS
	/*
	 * Track the largest commit timestamp we have seen.
	 *
	 * We don't actually clear the local commit timestamp, just the flag.
	 * That said, we can't update the global commit timestamp until this
	 * transaction is visible, which happens when we release it.
	 */
	update_timestamp = F_ISSET(txn, WT_TXN_HAS_TS_COMMIT);
#endif

	__wt_txn_release(session);

#ifdef HAVE_TIMESTAMPS
	/* First check if we've already committed something in the future. */
	if (update_timestamp) {
		__wt_readlock(session, &txn_global->rwlock);
		__wt_timestamp_set(
		    prev_commit_timestamp, txn_global->commit_timestamp);
		__wt_readunlock(session, &txn_global->rwlock);
		update_timestamp = __wt_timestamp_cmp(
		    txn->commit_timestamp, prev_commit_timestamp) > 0;
	}

	/*
	 * If it looks like we need to move the global commit timestamp,
	 * write lock and re-check.
	 */
	if (update_timestamp) {
		__wt_writelock(session, &txn_global->rwlock);
		if (__wt_timestamp_cmp(txn->commit_timestamp,
		    txn_global->commit_timestamp) > 0) {
			__wt_timestamp_set(txn_global->commit_timestamp,
			    txn->commit_timestamp);
			txn_global->has_commit_timestamp = true;
		}
		__wt_writeunlock(session, &txn_global->rwlock);
	}
#endif

	return (0);

err:	/*
	 * If anything went wrong, roll back.
	 *
	 * !!!
	 * Nothing can fail after this point.
	 */
	WT_TRET(__wt_txn_rollback(session, cfg));
	return (ret);
}

/*
 * __wt_txn_rollback --
 *	Roll back the current transaction.
 */
int
__wt_txn_rollback(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_DECL_RET;
	WT_TXN *txn;
	WT_TXN_OP *op;
	u_int i;

	WT_UNUSED(cfg);

	txn = &session->txn;
	WT_ASSERT(session, F_ISSET(txn, WT_TXN_RUNNING));

	/* Rollback notification. */
	if (txn->notify != NULL)
		WT_TRET(txn->notify->notify(txn->notify, (WT_SESSION *)session,
		    txn->id, 0));

	/* Rollback updates. */
	for (i = 0, op = txn->mod; i < txn->mod_count; i++, op++) {
		/* Metadata updates are never rolled back. */
		if (op->fileid == WT_METAFILE_ID)
			continue;

		switch (op->type) {
		case WT_TXN_OP_BASIC:
		case WT_TXN_OP_BASIC_TS:
		case WT_TXN_OP_INMEM:
		       WT_ASSERT(session, op->u.upd->txnid == txn->id);
			op->u.upd->txnid = WT_TXN_ABORTED;
			break;
		case WT_TXN_OP_REF:
			__wt_delete_page_rollback(session, op->u.ref);
			break;
		case WT_TXN_OP_TRUNCATE_COL:
		case WT_TXN_OP_TRUNCATE_ROW:
			/*
			 * Nothing to do: these operations are only logged for
			 * recovery.  The in-memory changes will be rolled back
			 * with a combination of WT_TXN_OP_REF and
			 * WT_TXN_OP_INMEM operations.
			 */
			break;
		}

		/* Free any memory allocated for the operation. */
		__wt_txn_op_free(session, op);
	}
	txn->mod_count = 0;

	__wt_txn_release(session);
	return (ret);
}

/*
 * __wt_txn_init --
 *	Initialize a session's transaction data.
 */
int
__wt_txn_init(WT_SESSION_IMPL *session, WT_SESSION_IMPL *session_ret)
{
	WT_TXN *txn;

	txn = &session_ret->txn;
	txn->id = WT_TXN_NONE;

	WT_RET(__wt_calloc_def(session,
	    S2C(session_ret)->session_size, &txn->snapshot));

#ifdef HAVE_DIAGNOSTIC
	if (S2C(session_ret)->txn_global.states != NULL) {
		WT_TXN_STATE *txn_state;
		txn_state = WT_SESSION_TXN_STATE(session_ret);
		WT_ASSERT(session, txn_state->pinned_id == WT_TXN_NONE);
	}
#endif

	/*
	 * Take care to clean these out in case we are reusing the transaction
	 * for eviction.
	 */
	txn->mod = NULL;

	txn->isolation = session_ret->isolation;
	return (0);
}

/*
 * __wt_txn_stats_update --
 *	Update the transaction statistics for return to the application.
 */
void
__wt_txn_stats_update(WT_SESSION_IMPL *session)
{
	WT_TXN_GLOBAL *txn_global;
	WT_CONNECTION_IMPL *conn;
	WT_CONNECTION_STATS **stats;
	uint64_t checkpoint_pinned, snapshot_pinned;

	conn = S2C(session);
	txn_global = &conn->txn_global;
	stats = conn->stats;
	checkpoint_pinned = txn_global->checkpoint_state.pinned_id;
	snapshot_pinned = txn_global->nsnap_oldest_id;

	WT_STAT_SET(session, stats, txn_pinned_range,
	    txn_global->current - txn_global->oldest_id);

	WT_STAT_SET(session, stats, txn_pinned_snapshot_range,
	    snapshot_pinned == WT_TXN_NONE ?
	    0 : txn_global->current - snapshot_pinned);

	WT_STAT_SET(session, stats, txn_pinned_checkpoint_range,
	    checkpoint_pinned == WT_TXN_NONE ?
	    0 : txn_global->current - checkpoint_pinned);

	WT_STAT_SET(
	    session, stats, txn_checkpoint_time_max, conn->ckpt_time_max);
	WT_STAT_SET(
	    session, stats, txn_checkpoint_time_min, conn->ckpt_time_min);
	WT_STAT_SET(
	    session, stats, txn_checkpoint_time_recent, conn->ckpt_time_recent);
	WT_STAT_SET(
	    session, stats, txn_checkpoint_time_total, conn->ckpt_time_total);
}

/*
 * __wt_txn_destroy --
 *	Destroy a session's transaction data.
 */
void
__wt_txn_destroy(WT_SESSION_IMPL *session)
{
	WT_TXN *txn;

	txn = &session->txn;
	__wt_free(session, txn->mod);
	__wt_free(session, txn->snapshot);
}

/*
 * __wt_txn_global_init --
 *	Initialize the global transaction state.
 */
int
__wt_txn_global_init(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;
	WT_TXN_GLOBAL *txn_global;
	WT_TXN_STATE *s;
	u_int i;

	WT_UNUSED(cfg);
	conn = S2C(session);

	txn_global = &conn->txn_global;
	txn_global->current = txn_global->last_running =
	    txn_global->metadata_pinned = txn_global->oldest_id = WT_TXN_FIRST;

	WT_RET(__wt_spin_init(
	    session, &txn_global->id_lock, "transaction id lock"));
	WT_RET(__wt_rwlock_init(session, &txn_global->rwlock));

	WT_RET(__wt_rwlock_init(session, &txn_global->commit_timestamp_rwlock));
	TAILQ_INIT(&txn_global->commit_timestamph);

	WT_RET(__wt_rwlock_init(session, &txn_global->read_timestamp_rwlock));
	TAILQ_INIT(&txn_global->read_timestamph);

	WT_RET(__wt_rwlock_init(session, &txn_global->nsnap_rwlock));
	txn_global->nsnap_oldest_id = WT_TXN_NONE;
	TAILQ_INIT(&txn_global->nsnaph);

	WT_RET(__wt_calloc_def(
	    session, conn->session_size, &txn_global->states));

	for (i = 0, s = txn_global->states; i < conn->session_size; i++, s++)
		s->id = s->metadata_pinned = s->pinned_id = WT_TXN_NONE;

	return (0);
}

/*
 * __wt_txn_global_destroy --
 *	Destroy the global transaction state.
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

	__wt_spin_destroy(session, &txn_global->id_lock);
	__wt_rwlock_destroy(session, &txn_global->rwlock);
	__wt_rwlock_destroy(session, &txn_global->commit_timestamp_rwlock);
	__wt_rwlock_destroy(session, &txn_global->read_timestamp_rwlock);
	__wt_rwlock_destroy(session, &txn_global->nsnap_rwlock);
	__wt_free(session, txn_global->states);
}

/*
 * __wt_txn_global_shutdown --
 *	Shut down the global transaction state.
 */
int
__wt_txn_global_shutdown(WT_SESSION_IMPL *session)
{
	WT_DECL_RET;
	WT_TXN_GLOBAL *txn_global;

	txn_global = &S2C(session)->txn_global;

	/*
	 * We're shutting down.  Make sure everything gets freed.
	 *
	 * It's possible that the eviction server is in the middle of a long
	 * operation, with a transaction ID pinned.  In that case, we will loop
	 * here until the transaction ID is released, when the oldest
	 * transaction ID will catch up with the current ID.
	 */
	for (;;) {
		WT_TRET(__wt_txn_update_oldest(session,
		    WT_TXN_OLDEST_STRICT | WT_TXN_OLDEST_WAIT));
		if (txn_global->oldest_id == txn_global->current &&
		    txn_global->metadata_pinned == txn_global->current)
			break;
		__wt_yield();
	}

#ifdef HAVE_TIMESTAMPS
	/*
	 * Now that all transactions have completed, no timestamps should be
	 * pinned.
	 */
	memset(txn_global->pinned_timestamp, 0xff, WT_TIMESTAMP_SIZE);
#endif

	return (ret);
}

#if defined(HAVE_DIAGNOSTIC) || defined(HAVE_VERBOSE)
/*
 * __wt_verbose_dump_txn --
 *	Output diagnostic information about the global transaction state.
 */
int
__wt_verbose_dump_txn(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_TXN_GLOBAL *txn_global;
	WT_TXN *txn;
	WT_TXN_STATE *s;
	const char *iso_tag;
	uint64_t id;
	uint32_t i, session_cnt;

	conn = S2C(session);
	txn_global = &conn->txn_global;

	WT_RET(__wt_msg(session, "%s", WT_DIVIDER));
	WT_RET(__wt_msg(session, "transaction state dump"));

	WT_RET(__wt_msg(session, "current ID: %" PRIu64, txn_global->current));
	WT_RET(__wt_msg(session,
	    "last running ID: %" PRIu64, txn_global->last_running));
	WT_RET(__wt_msg(session, "oldest ID: %" PRIu64, txn_global->oldest_id));
	WT_RET(__wt_msg(session,
	    "oldest named snapshot ID: %" PRIu64, txn_global->nsnap_oldest_id));

	WT_RET(__wt_msg(session, "checkpoint running? %s",
	    txn_global->checkpoint_running ? "yes" : "no"));
	WT_RET(__wt_msg(session, "checkpoint generation: %" PRIu64,
	    __wt_gen(session, WT_GEN_CHECKPOINT)));
	WT_RET(__wt_msg(session, "checkpoint pinned ID: %" PRIu64,
	    txn_global->checkpoint_state.pinned_id));
	WT_RET(__wt_msg(session, "checkpoint txn ID: %" PRIu64,
	    txn_global->checkpoint_state.id));

	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	WT_RET(__wt_msg(session, "session count: %" PRIu32, session_cnt));

	WT_RET(__wt_msg(session, "Transaction state of active sessions:"));

	/*
	 * Walk each session transaction state and dump information. Accessing
	 * the content of session handles is not thread safe, so some
	 * information may change while traversing if other threads are active
	 * at the same time, which is OK since this is diagnostic code.
	 */
	for (i = 0, s = txn_global->states; i < session_cnt; i++, s++) {
		/* Skip sessions with no active transaction */
		if ((id = s->id) == WT_TXN_NONE && s->pinned_id == WT_TXN_NONE)
			continue;

		txn = &conn->sessions[i].txn;
		iso_tag = "INVALID";
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

		WT_RET(__wt_msg(session,
		    "ID: %6" PRIu64
		    ", mod count: %u"
		    ", pinned ID: %" PRIu64
		    ", snap min: %" PRIu64
		    ", snap max: %" PRIu64
		    ", metadata pinned ID: %" PRIu64
		    ", flags: 0x%08" PRIx32
		    ", name: %s"
		    ", isolation: %s",
		    id,
		    txn->mod_count,
		    s->pinned_id,
		    txn->snap_min,
		    txn->snap_max,
		    s->metadata_pinned,
		    txn->flags,
		    conn->sessions[i].name == NULL ?
		    "EMPTY" : conn->sessions[i].name,
		    iso_tag));
	}
	WT_RET(__wt_msg(session, "%s", WT_DIVIDER));

	return (0);
}
#endif
