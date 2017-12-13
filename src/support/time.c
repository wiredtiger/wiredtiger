/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define	TIMER_PRECISION_US 1
#define	TIMER_RECHECK_PERIOD 25

/*
 * __time_check_monotonic --
 *	Check and prevent time running backward.  If we detect that it has, we
 *	set the time structure to the previous values, making time stand still
 *	until we see a time in the future of the highest value seen so far.
 */
static void
__time_check_monotonic(WT_SESSION_IMPL *session, struct timespec *tsp)
{
	/*
	 * Detect time going backward.  If so, use the last
	 * saved timestamp.
	 */
	if (session == NULL)
		return;

	if (tsp->tv_sec < session->last_epoch.tv_sec ||
	     (tsp->tv_sec == session->last_epoch.tv_sec &&
	     tsp->tv_nsec < session->last_epoch.tv_nsec)) {
		WT_STAT_CONN_INCR(session, time_travel);
		*tsp = session->last_epoch;
	} else
		session->last_epoch = *tsp;
}

/*
 * __wt_epoch --
 *	Return the time since the Epoch.
 */
void
__wt_epoch(WT_SESSION_IMPL *session, struct timespec *tsp)
    WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
	struct timespec tmp;

	/*
	 * Read into a local variable, then check for monotonically increasing
	 * time, ensuring single threads never see time move backward. We don't
	 * prevent multiple threads from seeing time move backwards (even when
	 * reading time serially, the saved last-read time is per thread, not
	 * per timer, so multiple threads can race the time). Nor do we prevent
	 * multiple threads simultaneously reading the time from seeing random
	 * time or time moving backwards (assigning the time structure to the
	 * returned memory location implies multicycle writes to memory).
	 */
	__wt_epoch_raw(session, &tmp);
	__time_check_monotonic(session, &tmp);
	*tsp = tmp;
}

/*
 * __wt_seconds --
 *	Return the seconds since the Epoch.
 */
void
__wt_seconds(WT_SESSION_IMPL *session, time_t *timep)
{
	struct timespec t;

	__wt_epoch(session, &t);

	*timep = t.tv_sec;
}

/*
 * __clock_server --
 *	Within WiredTiger there are a number of places where we wish to time
 *	the execution of operations, but the cost of calling time() is too high.
 *	To work around this issue, we create a clock server below which keeps
 *	time on the connection accessible globally.
 *
 * 	The clock server aims to keep time to roughly the precision of the
 * 	TIMER_PRECISION_US value and we accept that sometimes things may skew
 *	until the next reset.
 */
static WT_THREAD_RET
__clock_server(void *arg)
{
	struct timespec time;
	WT_CONNECTION_IMPL *conn;
	WT_SESSION_IMPL *session;
	uint64_t timer_runs;

	session = arg;
	conn = S2C(session);
	timer_runs = 0;

	while (F_ISSET(conn, WT_CONN_SERVER_CLOCK)) {
		/*
		 * We re-capture timing the long way every so often to ensure
		 * that we don't skew too much in our timing.
		 */
		if (timer_runs % TIMER_RECHECK_PERIOD == 0) {
			__wt_epoch(NULL, &time);
			WT_CLOCK_SET_TIME(session, time);
		} else {
			__wt_atomic_add64(
			    &conn->server_clock, TIMER_PRECISION_US);
		}
		/*
		 * Fuzzy time tracking means we don't mind loosing some
		 * precision when sleeping.
		 */
		__wt_sleep(0, TIMER_PRECISION_US);
		timer_runs += 1;
	}
	return (WT_THREAD_RET_VALUE);
}

/*
 * __wt_clock_server_start --
 *	Start the clock server.
 */
int
__wt_clock_server_start(WT_SESSION_IMPL *session)
{
	struct timespec time;
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/* Set the initial time value before starting the thread */
	__wt_epoch(NULL, &time);
	WT_CLOCK_SET_TIME(session, time);

	F_SET(conn, WT_CONN_SERVER_CLOCK);

	WT_RET(__wt_thread_create(
	    session, &conn->clock_tid, __clock_server, session));
	conn->clock_tid_set = true;

	return (0);
}

/*
 * __wt_clock_server_destroy --
 *	Destroy the clock server.
 */
int
__wt_clock_server_destroy(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);

	/*
	 * Clear the clock server flag and then sleep for 2 clock sleeps to
	 * allow the clock server to finish.
	 */
	F_CLR(conn, WT_CONN_SERVER_CLOCK);
	__wt_sleep(0, 2 * TIMER_PRECISION_US);

	if (conn->sweep_tid_set) {
		WT_RET(__wt_thread_join(session, conn->clock_tid));
		conn->clock_tid_set = false;
	}

	return (0);
}
