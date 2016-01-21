/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_range_truncate --
 *	Truncate of a cursor range, default implementation.
 */
int
__wt_range_truncate(WT_CURSOR *start, WT_CURSOR *stop)
{
	WT_DECL_RET;
	int cmp;

	if (start == NULL) {
		do {
			WT_RET(stop->remove(stop));
		} while ((ret = stop->prev(stop)) == 0);
		WT_RET_NOTFOUND_OK(ret);
	} else {
		cmp = -1;
		do {
			if (stop != NULL)
				WT_RET(start->compare(start, stop, &cmp));
			WT_RET(start->remove(start));
		} while (cmp < 0 && (ret = start->next(start)) == 0);
		WT_RET_NOTFOUND_OK(ret);
	}
	return (0);
}

/*
 * __wt_schema_range_truncate --
 *	WT_SESSION::truncate with a range.
 */
int
__wt_schema_range_truncate(
    WT_SESSION_IMPL *session, WT_CURSOR *start, WT_CURSOR *stop)
{
	WT_CURSOR *cursor;
	WT_DATA_SOURCE *dsrc;
	WT_DECL_RET;
	const char *uri;

	cursor = (start != NULL) ? start : stop;
	uri = cursor->internal_uri;

	if (WT_PREFIX_MATCH(uri, "file:")) {
		if (start != NULL)
			WT_CURSOR_NEEDKEY(start);
		if (stop != NULL)
			WT_CURSOR_NEEDKEY(stop);
		WT_WITH_BTREE(session, ((WT_CURSOR_BTREE *)cursor)->btree,
		    ret = __wt_btcur_range_truncate(
			(WT_CURSOR_BTREE *)start, (WT_CURSOR_BTREE *)stop));
	} else if (WT_PREFIX_MATCH(uri, "table:"))
		ret = __wt_table_range_truncate(
		    (WT_CURSOR_TABLE *)start, (WT_CURSOR_TABLE *)stop);
	else if ((dsrc = __wt_schema_get_source(session, uri)) != NULL &&
	    dsrc->range_truncate != NULL)
		ret = dsrc->range_truncate(dsrc, &session->iface, start, stop);
	else
		ret = __wt_range_truncate(start, stop);
err:
	return (ret);
}
