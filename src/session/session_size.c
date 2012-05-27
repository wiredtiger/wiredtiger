/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __size_file --
 *	Add in the byte count of the most recent snapshot of a file.
 */
static int
__size_file(WT_SESSION_IMPL *session, const char *file, uint64_t *bytesp)
{
	WT_DECL_RET;
	WT_SNAPSHOT *snap, *snapbase, *snapmax;

	WT_RET(__wt_meta_snaplist_get(session, file, &snapbase));

	/*
	 * The objects are supposed to be in sorted order, but it's just as
	 * easy to check.
	 */
	snapmax = NULL;
	WT_SNAPSHOT_FOREACH(snapbase, snap)
		if (snapmax == NULL || snap->order > snapmax->order)
			snapmax = snap;

	if (snapmax == NULL)
		ret = WT_NOTFOUND;
	else
		*bytesp += snapmax->snapshot_size;

	__wt_meta_snaplist_free(session, &snapbase);
	return (ret);
}

/*
 * __size_uri --
 *	Add in the byte count of the most recent snapshot of a single object.
 */
static int
__size_uri(WT_SESSION_IMPL *session, const char *uri, uint64_t *bytesp)
{
	WT_CONFIG_ITEM cval;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	WT_ITEM *uribuf;
	const char *conf;

	cursor = NULL;
	uribuf = NULL;

	WT_ERR(__wt_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, uri);
	WT_ERR(cursor->search(cursor));
	WT_ERR(cursor->get_value(cursor, &conf));

	/* Get the filename from the metadata. */
	WT_ERR(__wt_scr_alloc(session, 0, &uribuf));
	WT_ERR(__wt_config_getones(session, conf, "filename", &cval));
	WT_ERR(__wt_buf_fmt(
	    session, uribuf, "file:%.*s", (int)cval.len, cval.str));
	WT_ERR(__size_file(session, uribuf->data, bytesp));

err:	__wt_scr_free(&uribuf);
	if (cursor != NULL)
		WT_TRET(cursor->close(cursor));
	return (ret);
}

/*
 * __wt_session_size --
 *	Return the size of an object's active pages.
 */
int
__wt_session_size(WT_SESSION_IMPL *session,
    const char *uri, uint64_t *bytesp, const char *cfg[])
{
	WT_TABLE *table;
	const char *tablename;
	int i;

	*bytesp = 0;
	tablename = uri;

	if (WT_PREFIX_MATCH(uri, "file:")) {
		WT_RET(__size_file(session, uri, bytesp));
	} else if (WT_PREFIX_MATCH(uri, "colgroup:") ||
	    WT_PREFIX_MATCH(uri, "index:")) {
		WT_RET(__size_uri(session, uri, bytesp));
	} else if (WT_PREFIX_SKIP(tablename, "table:")) {
		WT_RET(__wt_schema_get_table(
		    session, tablename, strlen(tablename), &table));
		for (i = 0; i < WT_COLGROUPS(table); i++)
			WT_RET(__size_uri(session, table->cg_name[i], bytesp));
	} else
		return (__wt_unknown_object_type(session, uri));

	WT_UNUSED(cfg);
	return (0);
}
