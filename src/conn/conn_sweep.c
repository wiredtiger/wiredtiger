/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __sweep_mark --
 *	Mark idle handles with a time of death, and note if we see dead
 *	handles.
 */
static int
__sweep_mark(WT_SESSION_IMPL *session, int *dead_handlesp)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	time_t now;

	conn = S2C(session);
	*dead_handlesp = 0;

	/* Don't discard handles that have been open recently. */
	WT_RET(__wt_seconds(session, &now));

	WT_STAT_FAST_CONN_INCR(session, dh_conn_sweeps);
	SLIST_FOREACH(dhandle, &conn->dhlh, l) {
		if (WT_IS_METADATA(dhandle))
			continue;
		if (F_ISSET(dhandle, WT_DHANDLE_DEAD)) {
			++*dead_handlesp;
			continue;
		}
		if (dhandle->session_inuse != 0 ||
		    now <= dhandle->timeofdeath + conn->sweep_idle_time)
			continue;
		if (dhandle->timeofdeath == 0) {
			dhandle->timeofdeath = now;
			WT_STAT_FAST_CONN_INCR(session, dh_conn_tod);
			continue;
		}

		/* We now have a candidate to close. */
		++*dead_handlesp;
	}

	return (0);
}

/*
 * __sweep_expire --
 *	Mark trees dead if they are clean and haven't been accessed recently,
 *	until we have reached the configured minimum number of handles.
 */
static int
__sweep_expire(WT_SESSION_IMPL *session)
{
	WT_BTREE *btree;
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	time_t now;

	conn = S2C(session);

	/* Don't discard handles that have been open recently. */
	WT_RET(__wt_seconds(session, &now));

	WT_STAT_FAST_CONN_INCR(session, dh_conn_sweeps);
	SLIST_FOREACH(dhandle, &conn->dhlh, l) {
		/*
		 * Ignore open files once the open file count reaches the
		 * minimum number of handles.
		 */
		if (conn->open_file_count < conn->sweep_handles_min)
			break;

		if (WT_IS_METADATA(dhandle))
			continue;
		if (!F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
		    F_ISSET(dhandle, WT_DHANDLE_DEAD))
			continue;
		if (dhandle->session_inuse != 0 ||
		    now <= dhandle->timeofdeath + conn->sweep_idle_time)
			continue;

		/*
		 * We have a candidate for closing; if it's open, acquire an
		 * exclusive lock on the handle and mark it dead.
		 *
		 * The close would require I/O if an update cannot be written
		 * (updates in a no-longer-referenced file might not yet be
		 * globally visible if sessions have disjoint sets of files
		 * open).  In that case, skip it: we'll retry the close the
		 * next time, after the transaction state has progressed.
		 *
		 * We don't set WT_DHANDLE_EXCLUSIVE deliberately, we want
		 * opens to block on us and then retry rather than returning an
		 * EBUSY error to the application.  This is done holding the
		 * handle list lock so that connection-level handle searches
		 * never need to retry.
		 */
		if ((ret =
		    __wt_try_writelock(session, dhandle->rwlock)) == EBUSY)
			continue;
		WT_RET(ret);

		/* Only sweep clean trees where all updates are visible. */
		btree = dhandle->handle;
		if (btree->modified ||
		    !__wt_txn_visible_all(session, btree->rec_max_txn))
			goto unlock;

		/*
		 * Mark the handle as dead and close the underlying file
		 * handle. Closing the handle decrements the open file count,
		 * meaning the close loop won't overrun the configured minimum.
		 */
		WT_WITH_DHANDLE(session, dhandle, ret =
		    __wt_conn_btree_sync_and_close(session, 0, 1));

unlock:		WT_TRET(__wt_writeunlock(session, dhandle->rwlock));
		WT_RET_BUSY_OK(ret);
	}

	return (0);
}
/*
 * __sweep_flush --
 *	Flush pages from dead trees.
 */
static int
__sweep_flush(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;

	conn = S2C(session);

	WT_STAT_FAST_CONN_INCR(session, dh_conn_sweeps);
	SLIST_FOREACH(dhandle, &conn->dhlh, l) {
		if (!F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
		    !F_ISSET(dhandle, WT_DHANDLE_DEAD))
			continue;

		/* If the handle is marked "dead", flush it from cache. */
		WT_WITH_DHANDLE(session, dhandle, ret =
		    __wt_conn_btree_sync_and_close(session, 0, 0));

		/* We closed the btree handle, bump the statistic. */
		if (ret == 0)
			WT_STAT_FAST_CONN_INCR(session, dh_conn_handles);

		WT_RET_BUSY_OK(ret);
	}

	return (0);
}

/*
 * __sweep_remove_handles --
 *	Remove closed dhandles from the connection list.
 */
static int
__sweep_remove_handles(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle, *dhandle_next;
	WT_DECL_RET;

	conn = S2C(session);
	dhandle = SLIST_FIRST(&conn->dhlh);

	for (; dhandle != NULL; dhandle = dhandle_next) {
		dhandle_next = SLIST_NEXT(dhandle, l);
		if (WT_IS_METADATA(dhandle))
			continue;
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
		    dhandle->session_inuse != 0 ||
		    dhandle->session_ref != 0)
			continue;

		/* Make sure we get exclusive access. */
		if ((ret =
		    __wt_try_writelock(session, dhandle->rwlock)) == EBUSY)
			continue;
		WT_RET(ret);

		/*
		 * If there are no longer any references to the handle in any
		 * sessions, attempt to discard it.
		 */
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
		    dhandle->session_inuse != 0 || dhandle->session_ref != 0) {
			WT_RET(__wt_writeunlock(session, dhandle->rwlock));
			continue;
		}

		WT_WITH_DHANDLE(session, dhandle,
		    ret = __wt_conn_dhandle_discard_single(session, 0, 1));

		/* If the handle was not successfully discarded, unlock it. */
		if (ret != 0)
			WT_TRET(__wt_writeunlock(session, dhandle->rwlock));
		WT_RET_BUSY_OK(ret);
		WT_STAT_FAST_CONN_INCR(session, dh_conn_ref);
	}

	return (ret == EBUSY ? 0 : ret);
}

/*
 * __sweep_server --
 *	The handle sweep server thread.
 */
static WT_THREAD_RET
__sweep_server(void *arg)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	int dead_handles;

	session = arg;
	conn = S2C(session);

	/*
	 * Sweep for dead and excess handles.
	 */
	while (F_ISSET(conn, WT_CONN_SERVER_RUN) &&
	    F_ISSET(conn, WT_CONN_SERVER_SWEEP)) {
		/* Wait until the next event. */
		WT_ERR(__wt_cond_wait(session, conn->sweep_cond,
		    (uint64_t)conn->sweep_interval * WT_MILLION));

		/*
		 * Mark handles with a time of death, and report whether any
		 * handles are marked dead.
		 */
		WT_ERR(__sweep_mark(session, &dead_handles));

		if (dead_handles == 0 &&
		    conn->open_file_count < conn->sweep_handles_min)
			continue;

		/* Close handles if we have reached the configured limit */
		if (conn->open_file_count >= conn->sweep_handles_min) {
			WT_WITH_HANDLE_LIST_LOCK(session,
			    ret = __sweep_expire(session));
			WT_ERR(ret);
		}

		WT_ERR(__sweep_flush(session));

		WT_WITH_HANDLE_LIST_LOCK(session,
		    ret = __sweep_remove_handles(session));
		WT_ERR(ret);
	}

	if (0) {
err:		WT_PANIC_MSG(session, ret, "handle sweep server error");
	}
	return (WT_THREAD_RET_VALUE);
}

/*
 * __wt_sweep_config --
 *	Pull out sweep configuration settings
 */
int
__wt_sweep_config(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/* Pull out the sweep configurations. */
	WT_RET(__wt_config_gets(session,
	    cfg, "file_manager.close_idle_time", &cval));
	conn->sweep_idle_time = (time_t)cval.val;

	WT_RET(__wt_config_gets(session,
	    cfg, "file_manager.close_scan_interval", &cval));
	conn->sweep_interval = (time_t)cval.val;

	WT_RET(__wt_config_gets(session,
	    cfg, "file_manager.close_handle_minimum", &cval));
	conn->sweep_handles_min = (u_int)cval.val;

	return (0);
}

/*
 * __wt_sweep_create --
 *	Start the handle sweep thread.
 */
int
__wt_sweep_create(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/* Set first, the thread might run before we finish up. */
	F_SET(conn, WT_CONN_SERVER_SWEEP);

	WT_RET(__wt_open_internal_session(
	    conn, "sweep-server", 1, 1, &conn->sweep_session));
	session = conn->sweep_session;

	/*
	 * Handle sweep does enough I/O it may be called upon to perform slow
	 * operations for the block manager.
	 */
	F_SET(session, WT_SESSION_CAN_WAIT);

	WT_RET(__wt_cond_alloc(
	    session, "handle sweep server", 0, &conn->sweep_cond));

	WT_RET(__wt_thread_create(
	    session, &conn->sweep_tid, __sweep_server, session));
	conn->sweep_tid_set = 1;

	return (0);
}

/*
 * __wt_sweep_destroy --
 *	Destroy the handle-sweep thread.
 */
int
__wt_sweep_destroy(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;

	conn = S2C(session);

	F_CLR(conn, WT_CONN_SERVER_SWEEP);
	if (conn->sweep_tid_set) {
		WT_TRET(__wt_cond_signal(session, conn->sweep_cond));
		WT_TRET(__wt_thread_join(session, conn->sweep_tid));
		conn->sweep_tid_set = 0;
	}
	WT_TRET(__wt_cond_destroy(session, &conn->sweep_cond));

	if (conn->sweep_session != NULL) {
		wt_session = &conn->sweep_session->iface;
		WT_TRET(wt_session->close(wt_session, NULL));

		conn->sweep_session = NULL;
	}
	return (ret);
}
