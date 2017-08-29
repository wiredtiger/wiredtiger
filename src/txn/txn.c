/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __txn_set_snapshot --
 *	Setup a snapshot for faster searching and set the min/max bounds.
 */
static void
__txn_set_snapshot(WT_SESSION_IMPL *session, uint32_t n, uint64_t snap_max)
{
	WT_TXN *txn;

	txn = &session->txn;

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

	txn = &session->txn;

	WT_ASSERT(session,
	    txn->pinned_id == WT_TXN_NONE ||
	    session->txn.isolation == WT_ISO_READ_UNCOMMITTED ||
	    !__wt_txn_visible_all(session, txn->pinned_id, NULL));

	__wt_txn_clear_metadata_pinned(session);
	__wt_txn_clear_pinned_id(session);
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
	WT_TXN *other_txn, *txn;
	WT_TXN_GLOBAL *txn_global;
	uint64_t checkpoint_id, current_id, pinned_id;
	uint32_t n;

	conn = S2C(session);
	txn = &session->txn;
	txn_global = &conn->txn_global;
	n = 0;

	/*
	 * We're going to scan the list of running transactions: wait for the
	 * lock.
	 */
	// XXX -- keep oldest pinned
	__wt_readlock(session, &txn_global->rwlock);
	__wt_readlock(session, &txn_global->id_rwlock);
	current_id = pinned_id = txn_global->current;
	txn->metadata_pinned = checkpoint_id = txn_global->checkpoint_txn_id;
	TAILQ_FOREACH(other_txn, &txn_global->idh, idq) {
		if (other_txn == txn)
			continue;
		/* Insert the checkpoint transaction if necessary. */
		if (checkpoint_id != WT_TXN_NONE &&
		    checkpoint_id < other_txn->id) {
			txn->snapshot[n++] = checkpoint_id;
			checkpoint_id = WT_TXN_NONE;
		}
		if (WT_TXNID_LT(other_txn->id, pinned_id))
			pinned_id = other_txn->id;
		txn->snapshot[n++] = other_txn->id;
	}
	__wt_readunlock(session, &txn_global->id_rwlock);
	if (checkpoint_id != WT_TXN_NONE)
		txn->snapshot[n++] = checkpoint_id;

	if (txn->metadata_pinned != WT_TXN_NONE)
		__wt_txn_publish_metadata_pinned(session);
	if ((txn->pinned_id = pinned_id) != WT_TXN_NONE)
		__wt_txn_publish_pinned_id(session);
	__wt_readunlock(session, &txn_global->rwlock);
	__txn_set_snapshot(session, n, current_id);
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
		wt_timestamp_t oldest_timestamp, stable_timestamp;

		WT_RET(__wt_txn_parse_timestamp(
		    session, "read", &txn->read_timestamp, &cval));
		WT_WITH_TIMESTAMP_READLOCK(session, &txn_global->rwlock,
		    __wt_timestamp_set(
			&oldest_timestamp, &txn_global->oldest_timestamp);
		    __wt_timestamp_set(
			&stable_timestamp, &txn_global->stable_timestamp));
		if (__wt_timestamp_cmp(
		    &txn->read_timestamp, &oldest_timestamp) < 0)
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

	txn = &session->txn;
	txn_global = &S2C(session)->txn_global;

	WT_ASSERT(session, txn->mod_count == 0);
	txn->notify = NULL;

	/* Clear the transaction's ID from the global table. */
	if (WT_SESSION_IS_CHECKPOINT(session)) {
		txn_global->checkpoint_txn_id =
		    txn_global->checkpoint_pinned_id = WT_TXN_NONE;

		/*
		 * Be extra careful to cleanup everything for checkpoints: once
		 * the global checkpoint ID is cleared, we can no longer tell
		 * if this session is doing a checkpoint.
		 */
		txn_global->checkpoint_session_id = 0;
	} else if (F_ISSET(txn, WT_TXN_HAS_ID))
		WT_ASSERT(session,
		    !WT_TXNID_LT(txn->id, txn_global->last_running));

	__wt_txn_clear_id(session);

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
	WT_TXN_GLOBAL *txn_global;
	WT_TXN_OP *op;
	u_int i;
	bool did_update, locked;
#ifdef HAVE_TIMESTAMPS
	wt_timestamp_t prev_commit_timestamp;
	bool update_timestamp;
#endif

	txn = &session->txn;
	conn = S2C(session);
	txn_global = &conn->txn_global;
	did_update = txn->mod_count != 0;
	locked = false;

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
		    session, "commit", &txn->commit_timestamp, &cval));
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
		/*
		 * We hold the visibility lock for reading from the time
		 * we write our log record until the time we release our
		 * transaction so that the LSN any checkpoint gets will
		 * always reflect visible data.
		 */
		__wt_readlock(session, &txn_global->visibility_rwlock);
		locked = true;
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
			    op->type != WT_TXN_OP_BASIC_TS) {
				WT_ASSERT(session,
				    op->fileid != WT_METAFILE_ID);
				__wt_timestamp_set(&op->u.upd->timestamp,
				    &txn->commit_timestamp);
			}
#endif
			break;

		case WT_TXN_OP_REF:
#ifdef HAVE_TIMESTAMPS
			if (F_ISSET(txn, WT_TXN_HAS_TS_COMMIT))
				__wt_timestamp_set(
				    &op->u.ref->page_del->timestamp,
				    &txn->commit_timestamp);
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
	if (locked)
		__wt_readunlock(session, &txn_global->visibility_rwlock);

#ifdef HAVE_TIMESTAMPS
	/* First check if we've already committed something in the future. */
	if (update_timestamp) {
		WT_WITH_TIMESTAMP_READLOCK(session, &txn_global->rwlock,
		    __wt_timestamp_set(
			&prev_commit_timestamp, &txn_global->commit_timestamp));
		update_timestamp = __wt_timestamp_cmp(
		    &txn->commit_timestamp, &prev_commit_timestamp) > 0;
	}

	/*
	 * If it looks like we need to move the global commit timestamp,
	 * write lock and re-check.
	 */
	if (update_timestamp) {
		__wt_writelock(session, &txn_global->rwlock);
		if (__wt_timestamp_cmp(&txn->commit_timestamp,
		    &txn_global->commit_timestamp) > 0) {
			__wt_timestamp_set(&txn_global->commit_timestamp,
			    &txn->commit_timestamp);
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
	if (locked)
		__wt_readunlock(session, &txn_global->visibility_rwlock);
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
	txn->id = txn->metadata_pinned = txn->pinned_id = WT_TXN_NONE;

	WT_RET(__wt_calloc_def(session,
	    S2C(session_ret)->session_size, &txn->snapshot));

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
	checkpoint_pinned = txn_global->checkpoint_pinned_id;
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
