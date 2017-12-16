/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Tuning constants.
 */

/*
 * Default value of WT_CONNECTION->dhandle_history_count.
 */
#define	WT_CURSOR_CACHE_HISTORY		20

/*
 * Time between runs of cursor cache server. This, along with
 * WT_CONNECTION->dhandle_history_count define how long the it will be
 * between the time of the last use of a uri and the time cached cursors
 * for that uri begin to be closed.
 */
#define	WT_CURSOR_CACHE_WAIT		250000

/*
 * __cursor_cache_server_run_chk --
 *	Check to decide if the checkpoint server should continue running.
 */
static bool
__cursor_cache_server_run_chk(WT_SESSION_IMPL *session)
{
	return (F_ISSET(S2C(session), WT_CONN_SERVER_CURSOR_CACHE));
}

/*
 * __wt_conn_cursor_cache_pass --
 *	Visit all idle sessions to close stale cached cursors. If called
 *	in service of removing references to specific data handles, it
 *	only visits those sessions that are using those handles.
 */
int
__wt_conn_cursor_cache_pass(WT_SESSION_IMPL *session, bool close, bool usage,
    WT_BITMAP *remove_reference, uint64_t *closed_cnt)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *s;
	u_int i;

	conn = S2C(session);

	WT_ASSERT(session, remove_reference == NULL ||
	    __wt_bitmap_test_any(remove_reference));

	__wt_writelock(session, &conn->cursor_cache_lock);

	if (usage) {
		__wt_bitmap_free(session,
		    &conn->dhandle_history[conn->dhandle_history_cnt - 1]);
		memmove(&conn->dhandle_history[1], &conn->dhandle_history[0],
		    (conn->dhandle_history_cnt - 1) * sizeof(WT_BITMAP));
		memset(&conn->dhandle_history[0], 0, sizeof(WT_BITMAP));
	}
	if (remove_reference != NULL) {
		/* Discourage any session from keeping these handles cached. */
		for (i = 0; i < conn->dhandle_history_cnt; i++)
			__wt_bitmap_clear_bitmap(session,
			    &conn->dhandle_history[i], remove_reference);
		__wt_bitmap_clear_bitmap(session, &conn->dhandle_hot,
		    remove_reference);
	}
	if ((s = conn->sessions) != NULL)
		for (i = 0; i < conn->session_size; ++s, ++i)
			WT_ERR(__wt_session_cursor_cache_server(
			    session, s, usage, close, remove_reference,
			    closed_cnt));
	if (usage) {
		__wt_bitmap_clear_all(&conn->dhandle_hot);
		for (i = 0; i < conn->dhandle_history_cnt; i++)
			__wt_bitmap_or_bitmap(session,
			    &conn->dhandle_hot, &conn->dhandle_history[i]);
	}
err:	__wt_writeunlock(session, &conn->cursor_cache_lock);
	return (ret);
}

/*
 * __cursor_cache_server --
 *	Thread to collect and publish cursor usage for cursor caching.
 *	This thread also visits idle sessions to close stale cached cursors.
 */
static WT_THREAD_RET
__cursor_cache_server(void *arg)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	uint64_t ignored;

	session = (WT_SESSION_IMPL *)arg;
	conn = S2C(session);
	ignored = 0;

	for (;;) {
		/* Wait until the next event. */
		__wt_cond_wait(session, conn->cursor_cache_cond,
		    WT_CURSOR_CACHE_WAIT, __cursor_cache_server_run_chk);

		/* Check if we're quitting. */
		if (!__cursor_cache_server_run_chk(session))
			break;

		WT_ERR(__wt_conn_cursor_cache_pass(session, false, true,
		    NULL, &ignored));
	}

	if (0) {
err:		WT_PANIC_MSG(session, ret, "cursor cache server error");
	}
	return (WT_THREAD_RET_VALUE);
}

/*
 * __wt_conn_cursor_cache_open --
 *	Allocate resources for cursor caching.
 */
int
__wt_conn_cursor_cache_open(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/* Set first, the thread might run before we finish up. */
	F_SET(conn, WT_CONN_SERVER_CURSOR_CACHE);

	conn->dhandle_history_cnt = WT_CURSOR_CACHE_HISTORY;
	WT_RET(__wt_calloc(session, conn->dhandle_history_cnt,
	    sizeof(WT_BITMAP), &conn->dhandle_history));

	/* To avoid confusion, we disallow 0 as a descriptor. */
	WT_RET(__wt_bitmap_set(session, &conn->dhandle_alloced, 0));

	/* Initialize the cursor cache lock. */
	WT_RET(__wt_rwlock_init(session, &conn->cursor_cache_lock));

	WT_RET(__wt_cond_alloc(
	    session, "cursor cache server", &conn->cursor_cache_cond));

	WT_RET(__wt_thread_create(
	    session, &conn->cursor_cache_tid, __cursor_cache_server, session));
	conn->cursor_cache_tid_set = 1;

	return (0);
}

/*
 * __wt_conn_cursor_cache_destroy --
 *	Destroy resources for cursor caching.
 */
int
__wt_conn_cursor_cache_destroy(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	u_int i;

	conn = S2C(session);

	F_CLR(conn, WT_CONN_SERVER_CURSOR_CACHE);
	if (conn->cursor_cache_tid_set) {
		__wt_cond_signal(session, conn->cursor_cache_cond);
		WT_TRET(__wt_thread_join(session, conn->cursor_cache_tid));
		conn->cursor_cache_tid_set = 0;
	}
	__wt_cond_destroy(session, &conn->cursor_cache_cond);

	__wt_rwlock_destroy(session, &conn->cursor_cache_lock);

	__wt_bitmap_free(session, &conn->dhandle_hot);
	__wt_bitmap_free(session, &conn->dhandle_alloced);
	if (conn->dhandle_history != NULL) {
		for (i = 0; i < conn->dhandle_history_cnt; i++)
			__wt_bitmap_free(session, &conn->dhandle_history[i]);
		__wt_free(session, conn->dhandle_history);
	}

	return (0);
}
