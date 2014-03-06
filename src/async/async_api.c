/*-
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * wiredtiger_async_open --
 *	Open an asynchronous connection to a WiredTiger database.
 */
int
wiredtiger_async_open(const char *home, WT_EVENT_HANDLER *event_handler,
    const char *config, WT_ASYNC_CONNECTION **async_connp)
{
	WT_UNUSED(home);
	WT_UNUSED(event_handler);
	WT_UNUSED(config);

	*async_connp = NULL;
	return (ENOTSUP);
}
