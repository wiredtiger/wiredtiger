/*-
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_connection_open --
 *	Open a connection.
 */
int
__wt_connection_open(WT_CONNECTION_IMPL *conn, const char *cfg[])
{
	WT_SESSION_IMPL *session;

	/* Default session. */
	session = conn->default_session;
	WT_ASSERT(session, session->iface.connection == &conn->iface);

	/* WT_SESSION_IMPL array. */
	WT_RET(__wt_calloc(session,
	    conn->session_size, sizeof(WT_SESSION_IMPL), &conn->sessions));

	/* Create the cache. */
	WT_RET(__wt_cache_create(conn, cfg));

	/* Initialize transaction support. */
	WT_RET(__wt_txn_global_init(conn, cfg));

	/*
	 * Publish: there must be a barrier to ensure the connection structure
	 * fields are set before other threads read from the pointer.
	 */
	WT_WRITE_BARRIER();

	/*
	 * Open the default session.  We open this before starting service
	 * threads because those may allocate and use session resources that
	 * need to get cleaned up on close.
	 */
	WT_RET(__wt_open_session(conn, 1, NULL, NULL, &session));
	conn->default_session = session;

	return (0);
}

/*
 * __wt_connection_close --
 *	Close a connection handle.
 */
int
__wt_connection_close(WT_CONNECTION_IMPL *conn)
{
	WT_CONNECTION *wt_conn;
	WT_DECL_RET;
	WT_DLH *dlh;
	WT_FH *fh;
	WT_NAMED_COLLATOR *ncoll;
	WT_NAMED_COMPRESSOR *ncomp;
	WT_NAMED_DATA_SOURCE *ndsrc;
	WT_SESSION_IMPL *s, *session;
	u_int i;

	wt_conn = &conn->iface;
	session = conn->default_session;

	/* We're shutting down.  Make sure everything gets freed. */
	__wt_txn_update_oldest(session);

	/* Clear any pending async ops. */
	WT_TRET(__wt_async_flush(conn));

	/*
	 * Shut down server threads other than the eviction server, which is
	 * needed later to close btree handles.  Some of these threads access
	 * btree handles, so take care in ordering shutdown to make sure they
	 * exit before files are closed.
	 */
	F_CLR(conn, WT_CONN_SERVER_RUN);
	WT_TRET(__wt_async_destroy(conn));
	WT_TRET(__wt_checkpoint_server_destroy(conn));
	WT_TRET(__wt_statlog_destroy(conn, 1));
	WT_TRET(__wt_sweep_destroy(conn));

	/* Clean up open LSM handles. */
	WT_TRET(__wt_lsm_tree_close_all(session));

	/* Close open data handles. */
	WT_TRET(__wt_conn_dhandle_discard(conn));

	/*
	 * Now that all data handles are closed, tell logging that a checkpoint
	 * has completed then shut down the log manager (only after closing
	 * data handles).
	 */
	if (conn->logging) {
		WT_TRET(__wt_txn_checkpoint_log(
		    session, 1, WT_TXN_LOG_CKPT_STOP, NULL));
		WT_TRET(__wt_logmgr_destroy(conn));
	}

	/* Free memory for collators */
	while ((ncoll = TAILQ_FIRST(&conn->collqh)) != NULL)
		WT_TRET(__wt_conn_remove_collator(conn, ncoll));

	/* Free memory for compressors */
	while ((ncomp = TAILQ_FIRST(&conn->compqh)) != NULL)
		WT_TRET(__wt_conn_remove_compressor(conn, ncomp));

	/* Free memory for data sources */
	while ((ndsrc = TAILQ_FIRST(&conn->dsrcqh)) != NULL)
		WT_TRET(__wt_conn_remove_data_source(conn, ndsrc));

	/*
	 * Complain if files weren't closed, ignoring the lock and logging
	 * files, we'll close them in a minute.
	 */
	TAILQ_FOREACH(fh, &conn->fhqh, q) {
		if (fh == conn->lock_fh)
			continue;

		__wt_errx(session,
		    "Connection has open file handles: %s", fh->name);
		WT_TRET(__wt_close(session, fh));
		fh = TAILQ_FIRST(&conn->fhqh);
	}

	/* Shut down the eviction server thread. */
	WT_TRET(__wt_evict_destroy(conn));

	/* Disconnect from shared cache - must be before cache destroy. */
	WT_TRET(__wt_conn_cache_pool_destroy(conn));

	/* Discard the cache. */
	WT_TRET(__wt_cache_destroy(conn));

	/* Discard transaction state. */
	__wt_txn_global_destroy(conn);

	/* Close extensions, first calling any unload entry point. */
	while ((dlh = TAILQ_FIRST(&conn->dlhqh)) != NULL) {
		TAILQ_REMOVE(&conn->dlhqh, dlh, q);

		if (dlh->terminate != NULL)
			WT_TRET(dlh->terminate(wt_conn));
		WT_TRET(__wt_dlclose(session, dlh));
	}

	/*
	 * Close the internal (default) session, and switch back to the dummy
	 * session in case of any error messages from the remaining operations
	 * while destroying the connection handle.
	 */
	if (session != &conn->dummy_session) {
		WT_TRET(session->iface.close(&session->iface, NULL));
		session = conn->default_session = &conn->dummy_session;
	}

	/*
	 * The session's split stash isn't discarded during normal session close
	 * because it may persist past the life of the session.  Discard it now.
	 */
	if ((s = conn->sessions) != NULL)
		for (i = 0; i < conn->session_size; ++s, ++i)
			__wt_split_stash_discard_all(session, s);

	/*
	 * The session's hazard pointer memory isn't discarded during normal
	 * session close because access to it isn't serialized.  Discard it
	 * now.
	 */
	if ((s = conn->sessions) != NULL)
		for (i = 0; i < conn->session_size; ++s, ++i)
			if (s != session)
				__wt_free(session, s->hazard);

	/* Destroy the handle. */
	WT_TRET(__wt_connection_destroy(conn));

	return (ret);
}

/*
 * __wt_connection_workers --
 *	Start the worker threads.
 */
int
__wt_connection_workers(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	F_SET(conn, WT_CONN_SERVER_RUN);

	/*
	 * Start the eviction thread.
	 */
	WT_RET(__wt_evict_create(conn));

	/*
	 * Start the handle sweep thread.
	 */
	WT_RET(__wt_sweep_create(conn));

	/*
	 * Start the optional statistics thread.  Start statistics first so that
	 * other optional threads can know if statistics are enabled or not.
	 */
	WT_RET(__wt_statlog_create(conn, cfg));

	/* Start the optional async threads. */
	WT_RET(__wt_async_create(conn, cfg));

	/* Start the optional logging/archive thread. */
	WT_RET(__wt_logmgr_create(conn, cfg));

	/* Start the optional checkpoint thread. */
	WT_RET(__wt_checkpoint_server_create(conn, cfg));

	return (0);
}
