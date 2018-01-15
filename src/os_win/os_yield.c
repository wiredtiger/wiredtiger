/*-
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_yield --
 *	Yield the thread of control.
 */
void
__wt_yield(void)
{
	/*
	 * Yielding the processor isn't documented as a memory barrier, and it's
	 * a reasonable expectation to have. There's no reason not to explicitly
	 * include a barrier since we're giving up the CPU, and ensures callers
	 * aren't ever surprised.
	 */
	WT_FULL_BARRIER();

	SwitchToThread();
}
