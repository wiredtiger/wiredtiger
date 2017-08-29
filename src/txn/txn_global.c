/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_txn_publish_id --
 *	Publish a transaction's ID.
 *
 *	The txn_global->id_rwlock must be held by our caller.
 */
void
__wt_txn_publish_id(WT_SESSION_IMPL *session)
{
	WT_TXN *prev, *txn;
	WT_TXN_GLOBAL *txn_global;

	txn = &session->txn;
	txn_global = &S2C(session)->txn_global;

	WT_ASSERT(session, F_ISSET(txn, WT_TXN_HAS_ID) &&
	    !F_ISSET(txn, WT_TXN_PUBLIC_ID));

	for (prev = TAILQ_LAST(&txn_global->idh, __wt_txn_id_qh);
	    prev != NULL && WT_TXNID_LT(txn->id, prev->id);
	    prev = TAILQ_PREV(prev, __wt_txn_cts_qh, idq))
		;
	if (prev == NULL)
		TAILQ_INSERT_HEAD(&txn_global->idh, txn, idq);
	else
		TAILQ_INSERT_AFTER(&txn_global->idh, prev, txn, idq);
	F_SET(txn, WT_TXN_PUBLIC_ID);
}

/*
 * __wt_txn_clear_id --
 *	Clear a transaction's published ID.
 */
void
__wt_txn_clear_id(WT_SESSION_IMPL *session)
{
	WT_TXN *txn;
	WT_TXN_GLOBAL *txn_global;

	txn = &session->txn;
	txn_global = &S2C(session)->txn_global;

	if (!F_ISSET(txn, WT_TXN_PUBLIC_ID))
		return;

	__wt_writelock(session, &txn_global->id_rwlock);
	TAILQ_REMOVE(&txn_global->idh, txn, idq);
	txn->id = WT_TXN_NONE;
	__wt_writeunlock(session, &txn_global->id_rwlock);
	F_CLR(txn, WT_TXN_HAS_ID | WT_TXN_PUBLIC_ID);
}

/*
 * __wt_txn_publish_metadata_pinned --
 *	Publish a transaction's metadata pinned transaction ID.
 */
void
__wt_txn_publish_metadata_pinned(WT_SESSION_IMPL *session)
{
	WT_TXN *prev, *txn;
	WT_TXN_GLOBAL *txn_global;

	txn = &session->txn;
	txn_global = &S2C(session)->txn_global;

	WT_ASSERT(session, txn->metadata_pinned != WT_TXN_NONE &&
	    !F_ISSET(txn, WT_TXN_PUBLIC_METADATA_PINNED));

	__wt_writelock(session, &txn_global->metadata_pinned_rwlock);
	for (prev = TAILQ_LAST(&txn_global->metadata_pinnedh, __wt_txn_id_qh);
	    prev != NULL &&
	    WT_TXNID_LT(txn->metadata_pinned, prev->metadata_pinned);
	    prev = TAILQ_PREV(prev, __wt_txn_mp_qh, metadata_pinnedq))
		;
	if (prev == NULL)
		TAILQ_INSERT_HEAD(
		    &txn_global->metadata_pinnedh, txn, metadata_pinnedq);
	else
		TAILQ_INSERT_AFTER(
		    &txn_global->metadata_pinnedh, prev, txn, metadata_pinnedq);
	__wt_writeunlock(session, &txn_global->metadata_pinned_rwlock);
	F_SET(txn, WT_TXN_PUBLIC_METADATA_PINNED);
}

/*
 * __wt_txn_clear_metadata_pinned --
 *	Clear a transaction's published pinned ID.
 */
void
__wt_txn_clear_metadata_pinned(WT_SESSION_IMPL *session)
{
	WT_TXN *txn;
	WT_TXN_GLOBAL *txn_global;

	txn = &session->txn;
	txn_global = &S2C(session)->txn_global;

	if (!F_ISSET(txn, WT_TXN_PUBLIC_METADATA_PINNED))
		return;

	__wt_writelock(session, &txn_global->metadata_pinned_rwlock);
	TAILQ_REMOVE(&txn_global->metadata_pinnedh, txn, metadata_pinnedq);
	txn->metadata_pinned = WT_TXN_NONE;
	__wt_writeunlock(session, &txn_global->metadata_pinned_rwlock);
	F_CLR(txn, WT_TXN_PUBLIC_METADATA_PINNED);
}

/*
 * __wt_txn_publish_pinned_id --
 *	Publish a transaction's pinned transaction ID.
 */
void
__wt_txn_publish_pinned_id(WT_SESSION_IMPL *session)
{
	WT_TXN *prev, *txn;
	WT_TXN_GLOBAL *txn_global;

	txn = &session->txn;
	txn_global = &S2C(session)->txn_global;

	WT_ASSERT(session, txn->pinned_id != WT_TXN_NONE &&
	    !F_ISSET(txn, WT_TXN_PUBLIC_PINNED_ID));

	__wt_writelock(session, &txn_global->pinned_id_rwlock);
	for (prev = TAILQ_LAST(&txn_global->pinned_idh, __wt_txn_id_qh);
	    prev != NULL && WT_TXNID_LT(txn->pinned_id, prev->pinned_id);
	    prev = TAILQ_PREV(prev, __wt_txn_cts_qh, pinned_idq))
		;
	if (prev == NULL)
		TAILQ_INSERT_HEAD(&txn_global->pinned_idh, txn, pinned_idq);
	else
		TAILQ_INSERT_AFTER(
		    &txn_global->pinned_idh, prev, txn, pinned_idq);
	__wt_writeunlock(session, &txn_global->pinned_id_rwlock);
	F_SET(txn, WT_TXN_PUBLIC_PINNED_ID);
}

/*
 * __wt_txn_clear_pinned_id --
 *	Clear a transaction's published pinned ID.
 */
void
__wt_txn_clear_pinned_id(WT_SESSION_IMPL *session)
{
	WT_TXN *txn;
	WT_TXN_GLOBAL *txn_global;

	txn = &session->txn;
	txn_global = &S2C(session)->txn_global;

	if (!F_ISSET(txn, WT_TXN_PUBLIC_PINNED_ID))
		return;

	__wt_writelock(session, &txn_global->pinned_id_rwlock);
	TAILQ_REMOVE(&txn_global->pinned_idh, txn, pinned_idq);
	txn->pinned_id = WT_TXN_NONE;
	__wt_writeunlock(session, &txn_global->pinned_id_rwlock);
	F_CLR(txn, WT_TXN_PUBLIC_PINNED_ID);
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
	WT_TXN *txn;
	WT_TXN_GLOBAL *txn_global;
	uint64_t id, last_running, metadata_pinned, oldest_id;
	uint32_t i, session_cnt;

	conn = S2C(session);
	txn_global = &conn->txn_global;
	oldest_session = NULL;

	/* The oldest ID cannot change while we are holding the scan lock. */
	last_running = oldest_id = txn_global->current;
	if ((metadata_pinned = txn_global->checkpoint_txn_id) == WT_TXN_NONE)
		metadata_pinned = oldest_id;

	__wt_readlock(session, &txn_global->id_rwlock);
	if ((txn = TAILQ_FIRST(&txn_global->idh)) != NULL)
		last_running = txn->id;
	__wt_readunlock(session, &txn_global->id_rwlock);

	__wt_readlock(session, &txn_global->metadata_pinned_rwlock);
	if ((txn = TAILQ_FIRST(&txn_global->metadata_pinnedh)) != NULL)
		metadata_pinned = txn->metadata_pinned;
	__wt_readunlock(session, &txn_global->metadata_pinned_rwlock);

	__wt_readlock(session, &txn_global->pinned_id_rwlock);
	/*
	 * !!!
	 * Note: Don't ignore pinned ID values older than the previous oldest
	 * ID.  Read-uncommitted operations publish pinned ID values without
	 * acquiring the scan lock to protect the global table.  See the
	 * comment in __wt_txn_cursor_op for more details.
	 */
	if ((txn = TAILQ_FIRST(&txn_global->pinned_idh)) != NULL) {
		oldest_id = txn->pinned_id;
		oldest_session =
		    WT_STRUCT_FROM_FIELD(WT_SESSION_IMPL, txn, txn);
	}
	__wt_readunlock(session, &txn_global->pinned_id_rwlock);

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
 * __wt_txn_global_init --
 *	Initialize the global transaction state.
 */
int
__wt_txn_global_init(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;
	WT_TXN_GLOBAL *txn_global;
	u_int i;

	WT_UNUSED(cfg);
	conn = S2C(session);

	txn_global = &conn->txn_global;
	txn_global->current = txn_global->last_running =
	    txn_global->metadata_pinned = txn_global->oldest_id = WT_TXN_FIRST;

	WT_RET(__wt_rwlock_init(session, &txn_global->rwlock));
	WT_RET(__wt_rwlock_init(session, &txn_global->visibility_rwlock));

	WT_RET(__wt_rwlock_init(session, &txn_global->id_rwlock));
	TAILQ_INIT(&txn_global->idh);

	WT_RET(__wt_rwlock_init(session, &txn_global->metadata_pinned_rwlock));
	TAILQ_INIT(&txn_global->metadata_pinnedh);

	WT_RET(__wt_rwlock_init(session, &txn_global->pinned_id_rwlock));
	TAILQ_INIT(&txn_global->pinned_idh);

	WT_RET(__wt_rwlock_init(session, &txn_global->commit_timestamp_rwlock));
	TAILQ_INIT(&txn_global->commit_timestamph);

	WT_RET(__wt_rwlock_init(session, &txn_global->read_timestamp_rwlock));
	TAILQ_INIT(&txn_global->read_timestamph);

	WT_RET(__wt_rwlock_init(session, &txn_global->nsnap_rwlock));
	txn_global->nsnap_oldest_id = WT_TXN_NONE;
	TAILQ_INIT(&txn_global->nsnaph);

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

	__wt_rwlock_destroy(session, &txn_global->rwlock);
	__wt_rwlock_destroy(session, &txn_global->id_rwlock);
	__wt_rwlock_destroy(session, &txn_global->metadata_pinned_rwlock);
	__wt_rwlock_destroy(session, &txn_global->pinned_id_rwlock);
	__wt_rwlock_destroy(session, &txn_global->commit_timestamp_rwlock);
	__wt_rwlock_destroy(session, &txn_global->read_timestamp_rwlock);
	__wt_rwlock_destroy(session, &txn_global->nsnap_rwlock);
	__wt_rwlock_destroy(session, &txn_global->visibility_rwlock);
}

/*
 * __wt_txn_global_shutdown --
 *	Shut down the global transaction state.
 */
int
__wt_txn_global_shutdown(WT_SESSION_IMPL *session)
{
	bool txn_active;

	/*
	 * We're shutting down.  Make sure everything gets freed.
	 *
	 * It's possible that the eviction server is in the middle of a long
	 * operation, with a transaction ID pinned.  In that case, we will loop
	 * here until the transaction ID is released, when the oldest
	 * transaction ID will catch up with the current ID.
	 */
	for (;;) {
		WT_RET(__wt_txn_activity_check(session, &txn_active));
		if (!txn_active)
			break;

		WT_STAT_CONN_INCR(session, txn_release_blocked);
		__wt_yield();
	}

#ifdef HAVE_TIMESTAMPS
	/*
	 * Now that all transactions have completed, no timestamps should be
	 * pinned.
	 */
	__wt_timestamp_set_inf(&S2C(session)->txn_global.pinned_timestamp);
#endif

	return (0);
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
	const char *iso_tag;
	uint32_t i, session_cnt;
#ifdef HAVE_TIMESTAMPS
	char hex_timestamp[3][2 * WT_TIMESTAMP_SIZE + 1];
#endif
	conn = S2C(session);
	txn_global = &conn->txn_global;

	WT_RET(__wt_msg(session, "%s", WT_DIVIDER));
	WT_RET(__wt_msg(session, "transaction state dump"));

	WT_RET(__wt_msg(session, "current ID: %" PRIu64, txn_global->current));
	WT_RET(__wt_msg(session,
	    "last running ID: %" PRIu64, txn_global->last_running));
	WT_RET(__wt_msg(session, "oldest ID: %" PRIu64, txn_global->oldest_id));

#ifdef HAVE_TIMESTAMPS
	WT_RET(__wt_timestamp_to_hex_string(
	    session, hex_timestamp[0], &txn_global->commit_timestamp));
	WT_RET(__wt_msg(session, "commit timestamp: %s", hex_timestamp[0]));
	WT_RET(__wt_timestamp_to_hex_string(
	    session, hex_timestamp[0], &txn_global->oldest_timestamp));
	WT_RET(__wt_msg(session, "oldest timestamp: %s", hex_timestamp[0]));
	WT_RET(__wt_timestamp_to_hex_string(
	    session, hex_timestamp[0], &txn_global->pinned_timestamp));
	WT_RET(__wt_msg(session, "pinned timestamp: %s", hex_timestamp[0]));
	WT_RET(__wt_timestamp_to_hex_string(
	    session, hex_timestamp[0], &txn_global->stable_timestamp));
	WT_RET(__wt_msg(session, "stable timestamp: %s", hex_timestamp[0]));
	WT_RET(__wt_msg(session, "has_commit_timestamp: %s",
	    txn_global->has_commit_timestamp ? "yes" : "no"));
	WT_RET(__wt_msg(session, "has_oldest_timestamp: %s",
	    txn_global->has_oldest_timestamp ? "yes" : "no"));
	WT_RET(__wt_msg(session, "has_pinned_timestamp: %s",
	    txn_global->has_pinned_timestamp ? "yes" : "no"));
	WT_RET(__wt_msg(session, "has_stable_timestamp: %s",
	    txn_global->has_stable_timestamp ? "yes" : "no"));
	WT_RET(__wt_msg(session, "oldest_is_pinned: %s",
	    txn_global->oldest_is_pinned ? "yes" : "no"));
	WT_RET(__wt_msg(session, "stable_is_pinned: %s",
	    txn_global->stable_is_pinned ? "yes" : "no"));
#endif

	WT_RET(__wt_msg(session, "checkpoint running: %s",
	    txn_global->checkpoint_running ? "yes" : "no"));
	WT_RET(__wt_msg(session, "checkpoint generation: %" PRIu64,
	    __wt_gen(session, WT_GEN_CHECKPOINT)));
	WT_RET(__wt_msg(session, "checkpoint pinned ID: %" PRIu64,
	    txn_global->checkpoint_pinned_id));
	WT_RET(__wt_msg(session, "checkpoint txn ID: %" PRIu64,
	    txn_global->checkpoint_txn_id));

	WT_RET(__wt_msg(session,
	    "oldest named snapshot ID: %" PRIu64, txn_global->nsnap_oldest_id));

	WT_ORDERED_READ(session_cnt, conn->session_cnt);
	WT_RET(__wt_msg(session, "session count: %" PRIu32, session_cnt));
	WT_RET(__wt_msg(session, "Transaction state of active sessions:"));

	/*
	 * Walk each session transaction state and dump information. Accessing
	 * the content of session handles is not thread safe, so some
	 * information may change while traversing if other threads are active
	 * at the same time, which is OK since this is diagnostic code.
	 */
	for (i = 0; i < session_cnt; i++) {
		/* Skip sessions with no active transaction */
		txn = &conn->sessions[i].txn;
		if (!F_ISSET(txn, WT_TXN_PUBLIC_ID) &&
		    txn->pinned_id == WT_TXN_NONE)
			continue;

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
#ifdef HAVE_TIMESTAMPS
		WT_RET(__wt_timestamp_to_hex_string(
		    session, hex_timestamp[0], &txn->commit_timestamp));
		WT_RET(__wt_timestamp_to_hex_string(
		    session, hex_timestamp[1], &txn->first_commit_timestamp));
		WT_RET(__wt_timestamp_to_hex_string(
		    session, hex_timestamp[2], &txn->read_timestamp));
		WT_RET(__wt_msg(session,
		    "ID: %8" PRIu64
		    ", mod count: %u"
		    ", pinned ID: %8" PRIu64
		    ", snap min: %" PRIu64
		    ", snap max: %" PRIu64
		    ", commit_timestamp: %s"
		    ", first_commit_timestamp: %s"
		    ", read_timestamp: %s"
		    ", metadata pinned ID: %" PRIu64
		    ", flags: 0x%08" PRIx32
		    ", name: %s"
		    ", isolation: %s",
		    txn->id,
		    txn->mod_count,
		    txn->pinned_id,
		    txn->snap_min,
		    txn->snap_max,
		    hex_timestamp[0],
		    hex_timestamp[1],
		    hex_timestamp[2],
		    txn->metadata_pinned,
		    txn->flags,
		    conn->sessions[i].name == NULL ?
		    "EMPTY" : conn->sessions[i].name,
		    iso_tag));
#else
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
		    txn->pinned_id,
		    txn->snap_min,
		    txn->snap_max,
		    txn->metadata_pinned,
		    txn->flags,
		    conn->sessions[i].name == NULL ?
		    "EMPTY" : conn->sessions[i].name,
		    iso_tag));
#endif
	}
	WT_RET(__wt_msg(session, "%s", WT_DIVIDER));

	return (0);
}
#endif
