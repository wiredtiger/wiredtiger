/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

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
