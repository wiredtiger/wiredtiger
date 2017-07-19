/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#ifdef HAVE_TIMESTAMPS
/*
 * __wt_txn_parse_timestamp --
 *	Decodes and sets a timestamp.
 */
int
__wt_txn_parse_timestamp(WT_SESSION_IMPL *session,
     const char *name, void *timestamp, WT_CONFIG_ITEM *cval)
{
	__wt_timestamp_set_zero(timestamp);

	if (cval->len == 0)
		return (0);

	/* Protect against unexpectedly long hex strings. */
	if (cval->len > 2 * WT_TIMESTAMP_SIZE)
		WT_RET_MSG(session, EINVAL,
		    "Failed to parse %s timestamp '%.*s': too long",
		    name, (int)cval->len, cval->str);

#ifdef WT_TIMESTAMP_UINT64
	{
	static const u_char hextable[] = {
	    0,  0,  0,  0,  0,  0,  0,  0,
	    0,  0,  0,  0,  0,  0,  0,  0,
	    0,  0,  0,  0,  0,  0,  0,  0,
	    0,  0,  0,  0,  0,  0,  0,  0,
	    0,  0,  0,  0,  0,  0,  0,  0,
	    0,  0,  0,  0,  0,  0,  0,  0,
	    0,  1,  2,  3,  4,  5,  6,  7,
	    8,  9,  0,  0,  0,  0,  0,  0,
	    0, 10, 11, 12, 13, 14, 15,  0,
	    0,  0,  0,  0,  0,  0,  0,  0,
	    0,  0,  0,  0,  0,  0,  0,  0,
	    0,  0,  0,  0,  0,  0,  0,  0,
	    0, 10, 11, 12, 13, 14, 15
	};
	uint64_t ts;
	size_t len;
	const char *hex;

	for (ts = 0, hex = cval->str, len = cval->len; len > 0; --len)
		ts = (ts << 4) | hextable[(int)*hex++];
	__wt_timestamp_set(timestamp, &ts);
	}
#else
	{
	WT_DECL_RET;
	WT_ITEM ts;
	wt_timestamp_t tsbuf;
	size_t hexlen;
	const char *hexts;
	char padbuf[2 * WT_TIMESTAMP_SIZE + 1];

	/*
	 * The decoding function assumes it is decoding data produced by dump
	 * and so requires an even number of hex digits.
	 */
	if ((cval->len & 1) == 0) {
		hexts = cval->str;
		hexlen = cval->len;
	} else {
		padbuf[0] = '0';
		memcpy(padbuf + 1, cval->str, cval->len);
		hexts = padbuf;
		hexlen = cval->len + 1;
	}

	/* Avoid memory allocation to decode timestamps. */
	ts.data = ts.mem = tsbuf;
	ts.memsize = sizeof(tsbuf);

	if ((ret = __wt_nhex_to_raw(session, hexts, hexlen, &ts)) != 0)
		WT_RET_MSG(session, ret, "Failed to parse %s timestamp '%.*s'",
		    name, (int)cval->len, cval->str);
	WT_ASSERT(session, ts.size <= WT_TIMESTAMP_SIZE);

	/* Copy the raw value to the end of the timestamp. */
	memcpy((uint8_t *)timestamp + WT_TIMESTAMP_SIZE - ts.size,
	    ts.data, ts.size);
	}
#endif
	if (__wt_timestamp_iszero(timestamp))
		WT_RET_MSG(session, EINVAL,
		    "Failed to parse %s timestamp '%.*s': zero not permitted",
		    name, (int)cval->len, cval->str);

	return (0);
}

/*
 * __txn_global_query_timestamp --
 *	Query a timestamp.
 */
static int
__txn_global_query_timestamp(
    WT_SESSION_IMPL *session, wt_timestamp_t *tsp, const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;
	WT_CONFIG_ITEM cval;
	WT_TXN_GLOBAL *txn_global;
	WT_TXN_STATE *txn_state;
	wt_timestamp_t ts;
	uint32_t i, session_cnt;

	conn = S2C(session);
	txn_global = &conn->txn_global;

	WT_RET(__wt_config_gets(session, cfg, "get", &cval));
	if (WT_STRING_MATCH("all_committed", cval.str, cval.len)) {
		if (!txn_global->has_commit_timestamp)
			return (WT_NOTFOUND);
		__wt_readlock(session, &txn_global->rwlock);
		WT_TIMESTAMP_SET(ts, txn_global->commit_timestamp);
		WT_ORDERED_READ(session_cnt, conn->session_cnt);
		for (i = 0, txn_state = txn_global->states; i < session_cnt;
		    i++, txn_state++) {
			if (txn_state->id == WT_TXN_NONE ||
			    WT_TIMESTAMP_ISZERO(txn_state->commit_timestamp))
				continue;
			if (WT_TIMESTAMP_CMP(
			    txn_state->commit_timestamp, ts) < 0)
				WT_TIMESTAMP_SET(
				    ts, txn_state->commit_timestamp);
		}
		__wt_readunlock(session, &txn_global->rwlock);
	} else if (WT_STRING_MATCH("oldest_reader", cval.str, cval.len)) {
		if (!txn_global->has_oldest_timestamp)
			return (WT_NOTFOUND);
		__wt_readlock(session, &txn_global->rwlock);
		WT_TIMESTAMP_SET(ts, txn_global->oldest_timestamp);
		/* Look at running checkpoints. */
		txn_state = &txn_global->checkpoint_state;
		if (txn_state->pinned_id != WT_TXN_NONE &&
		    !WT_TIMESTAMP_ISZERO(txn_state->read_timestamp) &&
		    WT_TIMESTAMP_CMP(txn_state->read_timestamp, ts) < 0)
			WT_TIMESTAMP_SET(ts, txn_state->read_timestamp);
		WT_ORDERED_READ(session_cnt, conn->session_cnt);
		for (i = 0, txn_state = txn_global->states; i < session_cnt;
		    i++, txn_state++) {
			if (txn_state->pinned_id == WT_TXN_NONE ||
			    WT_TIMESTAMP_ISZERO(txn_state->read_timestamp))
				continue;
			if (WT_TIMESTAMP_CMP(txn_state->read_timestamp, ts) < 0)
				WT_TIMESTAMP_SET(ts, txn_state->read_timestamp);
		}
		__wt_readunlock(session, &txn_global->rwlock);
	} else
		WT_RET_MSG(session, EINVAL,
		    "unknown timestamp query configuration %.*s",
		    (int)cval.len, cval.str);

	__wt_timestamp_set(tsp, WT_TIMESTAMP(ts));
	return (0);
}
#endif

/*
 * __wt_txn_global_query_timestamp --
 *	Query a timestamp.
 */
int
__wt_txn_global_query_timestamp(
    WT_SESSION_IMPL *session, char *hex_timestamp, const char *cfg[])
{
#ifdef HAVE_TIMESTAMPS
	wt_timestamp_t ts;

	WT_RET(__txn_global_query_timestamp(session, WT_TIMESTAMP(ts), cfg));

#ifdef WT_TIMESTAMP_UINT64
	{
	char *p, v;

	for (p = hex_timestamp; ts != 0; ts >>= 4)
		*p++ = (char)__wt_hex((u_char)(ts & 0x0f));
	*p = '\0';

	/* Reverse the string. */
	for (--p; p > hex_timestamp;) {
		v = *p;
		*p-- = *hex_timestamp;
		*hex_timestamp++ = v;
	}
	}
#else
	{
	WT_ITEM hexts;
	size_t len;
	uint8_t *tsp;

	/* Avoid memory allocation: set up an item guaranteed large enough. */
	hexts.data = hexts.mem = hex_timestamp;
	hexts.memsize = 2 * WT_TIMESTAMP_SIZE + 1;
	/* Trim leading zeros. */
	for (tsp = ts, len = WT_TIMESTAMP_SIZE;
	    len > 0 && *tsp == 0;
	    ++tsp, --len)
		;
	WT_RET(__wt_raw_to_hex(session, tsp, len, &hexts));
	}
#endif
	return (0);
#else
	WT_UNUSED(session);
	WT_UNUSED(hex_timestamp);
	WT_UNUSED(cfg);

	WT_RET_MSG(session, ENOTSUP, "WT_CONNECTION.query_timestamp "
	    "requires a version of WiredTiger built with timestamp support");
#endif
}

#ifdef HAVE_TIMESTAMPS
/*
 * __wt_txn_update_pinned_timestamp --
 *	Update the pinned timestamp (the oldest timestamp that has to be
 *	maintained for current or future readers).
 */
int
__wt_txn_update_pinned_timestamp(WT_SESSION_IMPL *session)
{
	WT_DECL_RET;
	WT_TXN_GLOBAL *txn_global;
	wt_timestamp_t active_timestamp, oldest_timestamp, pinned_timestamp;
	const char *query_cfg[] = { WT_CONFIG_BASE(session,
	    WT_CONNECTION_query_timestamp), "get=oldest_reader", NULL };

	txn_global = &S2C(session)->txn_global;

	/* Skip locking and scanning when the oldest timestamp is pinned. */
	if (txn_global->oldest_is_pinned)
		return (0);

	__wt_readlock(session, &txn_global->rwlock);
	WT_TIMESTAMP_SET(oldest_timestamp, txn_global->oldest_timestamp);
	__wt_readunlock(session, &txn_global->rwlock);

	/* Scan to find the global pinned timestamp. */
	if ((ret = __txn_global_query_timestamp(
	    session, WT_TIMESTAMP(active_timestamp), query_cfg)) != 0)
		return (ret == WT_NOTFOUND ? 0 : ret);

	if (WT_TIMESTAMP_CMP(oldest_timestamp, active_timestamp) < 0) {
		WT_TIMESTAMP_SET(pinned_timestamp, oldest_timestamp);
	} else
		WT_TIMESTAMP_SET(pinned_timestamp, active_timestamp);

	__wt_writelock(session, &txn_global->rwlock);
	if (!txn_global->has_pinned_timestamp || WT_TIMESTAMP_CMP(
	    txn_global->pinned_timestamp, pinned_timestamp) < 0) {
		WT_TIMESTAMP_SET(
		    txn_global->pinned_timestamp, pinned_timestamp);
		txn_global->has_pinned_timestamp = true;
		txn_global->oldest_is_pinned = WT_TIMESTAMP_CMP(
		    txn_global->pinned_timestamp,
		    txn_global->oldest_timestamp) == 0;
	}
	__wt_writeunlock(session, &txn_global->rwlock);

	return (0);
}
#endif

/*
 * __wt_txn_global_set_timestamp --
 *	Set a global transaction timestamp.
 */
int
__wt_txn_global_set_timestamp(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval;

	/*
	 * Look for a commit timestamp.
	 */
	WT_RET(
	    __wt_config_gets_def(session, cfg, "oldest_timestamp", 0, &cval));
	if (cval.len != 0) {
#ifdef HAVE_TIMESTAMPS
		WT_TXN_GLOBAL *txn_global;
		wt_timestamp_t oldest_timestamp;

		WT_RET(__wt_txn_parse_timestamp(session,
		    "oldest", WT_TIMESTAMP(oldest_timestamp), &cval));

		/*
		 * This method can be called from multiple threads, check that
		 * we are moving the global oldest timestamp forwards.
		 */
		txn_global = &S2C(session)->txn_global;
		__wt_writelock(session, &txn_global->rwlock);
		if (!txn_global->has_oldest_timestamp || WT_TIMESTAMP_CMP(
		    txn_global->oldest_timestamp, oldest_timestamp) < 0) {
			WT_TIMESTAMP_SET(
			    txn_global->oldest_timestamp, oldest_timestamp);
			txn_global->has_oldest_timestamp = true;
			txn_global->oldest_is_pinned = false;
		}
		__wt_writeunlock(session, &txn_global->rwlock);

		WT_RET(__wt_txn_update_pinned_timestamp(session));
#else
		WT_RET_MSG(session, EINVAL, "oldest_timestamp requires a "
		    "version of WiredTiger built with timestamp support");
#endif
	}

	return (0);
}

/*
 * __wt_txn_set_timestamp --
 *	Set a transaction's timestamp.
 */
int
__wt_txn_set_timestamp(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;

	/*
	 * Look for a commit timestamp.
	 */
	ret = __wt_config_gets(session, cfg, "commit_timestamp", &cval);
	if (ret == 0 && cval.len != 0) {
#ifdef HAVE_TIMESTAMPS
		WT_TXN *txn = &session->txn;
		WT_TXN_GLOBAL *txn_global = &S2C(session)->txn_global;
		WT_TXN_STATE *txn_state = WT_SESSION_TXN_STATE(session);

		WT_RET(__wt_txn_parse_timestamp(session,
		    "commit", WT_TIMESTAMP(txn->commit_timestamp), &cval));
		if (!F_ISSET(txn, WT_TXN_HAS_TS_COMMIT)) {
			__wt_writelock(session, &txn_global->rwlock);
			WT_TIMESTAMP_SET(txn_state->commit_timestamp,
			    txn->commit_timestamp);
			__wt_writeunlock(session, &txn_global->rwlock);
			F_SET(txn, WT_TXN_HAS_TS_COMMIT);
		}
#else
		WT_RET_MSG(session, EINVAL, "commit_timestamp requires a "
		    "version of WiredTiger built with timestamp support");
#endif
	}
	WT_RET_NOTFOUND_OK(ret);

	return (0);
}
