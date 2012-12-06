/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
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
	WT_DECL_RET;
	WT_SESSION_IMPL *evict_session, *session;

	/* Default session. */
	session = conn->default_session;
	WT_ASSERT(session, session->iface.connection == &conn->iface);

	/* WT_SESSION_IMPL array. */
	WT_ERR(__wt_calloc(session,
	    conn->session_size, sizeof(WT_SESSION_IMPL), &conn->sessions));

	/* Create the cache. */
	WT_ERR(__wt_cache_create(conn, cfg));

	/* Initialize transaction support. */
	WT_ERR(__wt_txn_global_init(conn, cfg));

	/*
	 * Publish: there must be a barrier to ensure the connection structure
	 * fields are set before other threads read from the pointer.
	 */
	WT_WRITE_BARRIER();

	/* Start worker threads. */
	F_SET(conn, WT_SERVER_RUN);

	/*
	 * Start the eviction thread.
	 *
	 * It needs a session handle because it is reading/writing pages.
	 * Allocate a session here so the eviction thread never needs
	 * to acquire the connection spinlock, which can lead to deadlock.
	 */
	WT_ERR(__wt_open_session(conn, 1, NULL, NULL, &evict_session));
	evict_session->name = "eviction-server";
	WT_ERR(__wt_thread_create(
	    &conn->cache_evict_tid, __wt_cache_evict_server, evict_session));

	return (0);

err:	(void)__wt_connection_close(conn);
	return (ret);
}

/*
 * __wt_connection_close --
 *	Close a connection handle.
 */
int
__wt_connection_close(WT_CONNECTION_IMPL *conn)
{
	WT_SESSION_IMPL *session;
	WT_DECL_RET;
	WT_DLH *dlh;
	WT_FH *fh;

	session = conn->default_session;

	/*
	 * Complain if files weren't closed (ignoring the lock and logging
	 * files, we'll close them in a minute.
	 */
	TAILQ_FOREACH(fh, &conn->fhqh, q) {
		if (fh == conn->lock_fh || fh == conn->log_fh)
			continue;

		__wt_errx(session,
		    "Connection has open file handles: %s", fh->name);
		WT_TRET(__wt_close(session, fh));
		fh = TAILQ_FIRST(&conn->fhqh);
	}

	/* Shut down the server threads. */
	F_CLR(conn, WT_SERVER_RUN);
	if (conn->cache_evict_tid != 0) {
		__wt_evict_server_wake(session);
		WT_TRET(__wt_thread_join(conn->cache_evict_tid));
	}

	/* Disconnect from shared cache - must be before cache destroy. */
	WT_TRET(__wt_conn_cache_pool_destroy(conn));

	/* Discard the cache. */
	__wt_cache_destroy(conn);

	/* Discard transaction state. */
	__wt_txn_global_destroy(conn);

	/* Close extensions. */
	while ((dlh = TAILQ_FIRST(&conn->dlhqh)) != NULL) {
		TAILQ_REMOVE(&conn->dlhqh, dlh, q);
		WT_TRET(__wt_dlclose(session, dlh));
	}

	/*
	 * Close the internal (default) session, and switch back to the dummy
	 * session in case of any error messages from the remaining operations
	 * while destroying the connection handle.
	 *
	 * Additionally, the session's hazard pointer memory isn't discarded
	 * during normal session close because access to it isn't serialized.
	 * Discard it now.
	 */
	if (session != &conn->dummy_session) {
		WT_TRET(session->iface.close(&session->iface, NULL));
		__wt_free(&conn->dummy_session, session->hazard);

		conn->default_session = &conn->dummy_session;
	}

	/* Destroy the handle. */
	__wt_connection_destroy(conn);

	return (ret);
}
