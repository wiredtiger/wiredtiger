/*-
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __wt_dhandle_incr_use --
 *	Increment the in-use counter in the data source.
 */
static inline void
__wt_dhandle_incr_use(WT_SESSION_IMPL *session)
{
	WT_DATA_HANDLE *dhandle;

	dhandle = session->dhandle;

	/* If we open a handle with a time of death set, clear it. */
	if (__wt_atomic_addi32(&dhandle->session_inuse, 1) == 1 &&
	    dhandle->timeofdeath != 0)
		dhandle->timeofdeath = 0;
}

/*
 * __wt_dhandle_decr_use --
 *	Decrement the in-use counter in the data source.
 */
static inline void
__wt_dhandle_decr_use(WT_SESSION_IMPL *session)
{
	WT_DATA_HANDLE *dhandle;

	dhandle = session->dhandle;

	/* If we close a handle with a time of death set, clear it. */
	WT_ASSERT(session, dhandle->session_inuse > 0);
	if (__wt_atomic_subi32(&dhandle->session_inuse, 1) == 0 &&
	    dhandle->timeofdeath != 0)
		dhandle->timeofdeath = 0;
}
