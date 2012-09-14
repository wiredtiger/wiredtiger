/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_schema_worker --
 *	Get Btree handles for the object and cycle through calls to an
 * underlying worker function with each handle.
 */
int
__wt_schema_worker(WT_SESSION_IMPL *session,
   const char *uri,
   int (*func)(WT_SESSION_IMPL *, const char *[]),
   const char *cfg[], uint32_t open_flags)
{
	WT_COLGROUP *colgroup;
	WT_DECL_RET;
	WT_TABLE *table;
	const char *tablename;
	int i;

	tablename = uri;

	/* Get the btree handle(s) and call the underlying function. */
	if (WT_PREFIX_MATCH(uri, "file:")) {
		WT_RET(__wt_session_get_btree_ckpt(
		    session, uri, cfg, open_flags));
		ret = func(session, cfg);
		WT_TRET(__wt_session_release_btree(session));
	} else if (WT_PREFIX_MATCH(uri, "colgroup:") ||
	    WT_PREFIX_MATCH(uri, "index:")) {
		WT_RET(__wt_schema_get_btree(
		    session, uri, strlen(uri), cfg, open_flags));
		ret = func(session, cfg);
		WT_TRET(__wt_session_release_btree(session));
	} else if (WT_PREFIX_SKIP(tablename, "lsm:")) {
		WT_RET(__wt_lsm_tree_worker(
		    session, uri, func, cfg, open_flags));
	} else if (WT_PREFIX_SKIP(tablename, "table:")) {
		WT_RET(__wt_schema_get_table(session,
		    tablename, strlen(tablename), 0, &table));
		WT_ASSERT(session, session->btree == NULL);

		for (i = 0; i < WT_COLGROUPS(table); i++) {
			colgroup = table->cgroups[i];
			WT_RET(__wt_session_get_btree_ckpt(session,
			    colgroup->source, cfg, open_flags));
			ret = func(session, cfg);
			WT_TRET(__wt_session_release_btree(session));
			WT_RET(ret);
		}
	} else
		return (__wt_bad_object_type(session, uri));

	return (ret);
}
