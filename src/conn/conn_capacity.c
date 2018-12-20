/*
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define	WT_CAPACITY_CHK(v, str)	do {				\
	if ((v) != 0 && (v) < WT_THROTTLE_MIN)			\
		WT_RET_MSG(session, EINVAL,			\
		    "%s I/O capacity value %" PRId64		\
		    " below minimum %d",			\
		    str, v, WT_THROTTLE_MIN);			\
} while (0)

/*
 * __capacity_config --
 *	Set I/O capacity configuration.
 */
static int
__capacity_config(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	WT_RET(__wt_config_gets(session, cfg, "io_capacity.checkpoint", &cval));
	WT_CAPACITY_CHK(cval.val, "checkpoint");
	conn->capacity_ckpt = (uint64_t)cval.val;
	WT_RET(__wt_config_gets(session, cfg, "io_capacity.eviction", &cval));
	WT_CAPACITY_CHK(cval.val, "eviction");
	conn->capacity_evict = (uint64_t)cval.val;
	WT_RET(__wt_config_gets(session, cfg, "io_capacity.log", &cval));
	WT_CAPACITY_CHK(cval.val, "log");
	conn->capacity_log = (uint64_t)cval.val;
	WT_RET(__wt_config_gets(session, cfg, "io_capacity.read", &cval));
	WT_CAPACITY_CHK(cval.val, "read");
	conn->capacity_read = (uint64_t)cval.val;
	WT_RET(__wt_config_gets(session, cfg, "io_capacity.total", &cval));
	WT_CAPACITY_CHK(cval.val, "total");
	conn->capacity_total = (uint64_t)cval.val;

	conn->capacity_written = conn->capacity_ckpt +
	    conn->capacity_evict + conn->capacity_log;

	return (0);
}

/*
 * __capacity_sync --
 *	Background sync if the number of bytes written is sufficient.
 */
static int
__capacity_sync(WT_SESSION_IMPL *session)
{
	WT_UNUSED(session);
	return (0);
}

/*
 * __capacity_server_run_chk --
 *	Check to decide if the capacity server should continue running.
 */
static bool
__capacity_server_run_chk(WT_SESSION_IMPL *session)
{
	return (F_ISSET(S2C(session), WT_CONN_SERVER_CAPACITY));
}

/*
 * __capacity_server --
 *	The capacity server thread.
 */
static WT_THREAD_RET
__capacity_server(void *arg)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;
	WT_SESSION_IMPL *session;

	session = arg;
	conn = S2C(session);
	wt_session = (WT_SESSION *)session;

	for (;;) {
		/*
		 * Wait...
		 */
		__wt_cond_wait(session, conn->capacity_cond,
		    conn->capacity_usecs, __capacity_server_run_chk);

		/* Check if we're quitting or being reconfigured. */
		if (!__capacity_server_run_chk(session))
			break;

		WT_ERR(__capacity_sync(session));
		/*
		 * In case we crossed the written limit and the
		 * condition variable was already signalled, do
		 * a tiny wait to clear it so we don't do another
		 * sync immediately.
		 */
		__wt_cond_wait(session, conn->capacity_cond, 1, NULL);
	}

	if (0) {
err:		WT_PANIC_MSG(session, ret, "capacity server error");
	}
	return (WT_THREAD_RET_VALUE);
}

/*
 * __capacity_server_start --
 *	Start the capacity server thread.
 */
static int
__capacity_server_start(WT_CONNECTION_IMPL *conn)
{
	WT_SESSION_IMPL *session;

	/* Nothing to do if the server is already running. */
	if (conn->capacity_session != NULL)
		return (0);

	F_SET(conn, WT_CONN_SERVER_CAPACITY);

	/*
	 * The capacity server gets its own session.
	 */
	WT_RET(__wt_open_internal_session(conn,
	    "capacity-server", false, 0, &conn->capacity_session));
	session = conn->capacity_session;

	WT_RET(__wt_cond_alloc(session,
	    "capacity server", &conn->capacity_cond));

	/*
	 * Start the thread.
	 */
	WT_RET(__wt_thread_create(
	    session, &conn->capacity_tid, __capacity_server, session));
	conn->capacity_tid_set = true;

	return (0);
}

/*
 * __wt_capacity_server_create --
 *	Configure and start the capacity server.
 */
int
__wt_capacity_server_create(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/*
	 * Stop any server that is already running. This means that each time
	 * reconfigure is called we'll bounce the server even if there are no
	 * configuration changes. This makes our life easier as the underlying
	 * configuration routine doesn't have to worry about freeing objects
	 * in the connection structure (it's guaranteed to always start with a
	 * blank slate), and we don't have to worry about races where a running
	 * server is reading configuration information that we're updating, and
	 * it's not expected that reconfiguration will happen a lot.
	 */
	if (conn->capacity_session != NULL)
		WT_RET(__wt_capacity_server_destroy(session));

	WT_RET(__capacity_config(session, cfg));
	if (conn->capacity_written != 0)
		WT_RET(__capacity_server_start(conn));

	return (0);
}

/*
 * __wt_capacity_server_destroy --
 *	Destroy the capacity server thread.
 */
int
__wt_capacity_server_destroy(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;

	conn = S2C(session);

	F_CLR(conn, WT_CONN_SERVER_CAPACITY);
	if (conn->capacity_tid_set) {
		__wt_cond_signal(session, conn->capacity_cond);
		WT_TRET(__wt_thread_join(session, &conn->capacity_tid));
		conn->capacity_tid_set = false;
	}
	__wt_cond_destroy(session, &conn->capacity_cond);

	/* Close the server thread's session. */
	if (conn->capacity_session != NULL) {
		wt_session = &conn->capacity_session->iface;
		WT_TRET(wt_session->close(wt_session, NULL));
	}

	/*
	 * Ensure capacity settings are cleared - so that reconfigure doesn't
	 * get confused.
	 */
	conn->capacity_session = NULL;
	conn->capacity_tid_set = false;
	conn->capacity_cond = NULL;
	conn->capacity_usecs = 0;

	return (ret);
}

/*
 * __wt_capacity_signal --
 *	Signal the capacity thread if sufficient log has been written.
 */
void
__wt_capacity_signal(WT_SESSION_IMPL *session, uint64_t written)
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);
	WT_ASSERT(session, WT_CAPACITY_SIZE(conn));
	if (written >= conn->capacity_written && !conn->capacity_signalled) {
		__wt_cond_signal(session, conn->capacity_cond);
		conn->capacity_signalled = true;
	}
}
