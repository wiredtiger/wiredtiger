/*-
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_epoch_raw --
 *	Return the time since the Epoch as reported by the system.
 */
void
__wt_epoch_raw(WT_SESSION_IMPL *session, struct timespec *tsp)
{
	FILETIME time;
	uint64_t ns100;

	WT_UNUSED(session);

	GetSystemTimeAsFileTime(&time);

	ns100 = (((int64_t)time.dwHighDateTime << 32) + time.dwLowDateTime)
	    - 116444736000000000LL;
	tsp->tv_sec = ns100 / 10000000;
	tsp->tv_nsec = (long)((ns100 % 10000000) * 100);
}

/*
 * localtime_r --
 *	Return the current local time.
 */
struct tm *
localtime_r(const time_t *timer, struct tm *result)
{
	errno_t err;

	err = localtime_s(result, timer);
	if (err != 0) {
		__wt_err(NULL, err, "localtime_s");
		return (NULL);
	}

	return (result);
}
