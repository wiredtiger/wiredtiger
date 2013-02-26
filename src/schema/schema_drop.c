/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __drop_file --
 *	Drop a file.
 */
static int
__drop_file(
    WT_SESSION_IMPL *session, const char *uri, int force, const char *cfg[])
{
	WT_DECL_RET;
	int exist;
	const char *filename;

	filename = uri;
	if (!WT_PREFIX_SKIP(filename, "file:"))
		return (EINVAL);

	if (session->btree == NULL &&
	    (ret = __wt_session_get_btree(session, uri, NULL, cfg,
	    WT_BTREE_EXCLUSIVE | WT_BTREE_LOCK_ONLY)) != 0) {
		if (ret == WT_NOTFOUND || ret == ENOENT)
			ret = 0;
		return (ret);
	}

	/* Close all btree handles associated with this file. */
	WT_RET(__wt_conn_btree_close_all(session, uri));

	/* Remove the metadata entry (ignore missing items). */
	WT_TRET(__wt_metadata_remove(session, uri));
	if (force && ret == WT_NOTFOUND)
		ret = 0;

	/* Remove the underlying physical file. */
	exist = 0;
	WT_TRET(__wt_exist(session, filename, &exist));
	if (exist) {
		/*
		 * There is no point tracking this operation: there is no going
		 * back from here.
		 */
		WT_TRET(__wt_remove(session, filename));
	}

	return (ret);
}

/*
 * __drop_colgroup --
 *	WT_SESSION::drop for a colgroup.
 */
static int
__drop_colgroup(
    WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	WT_COLGROUP *colgroup;
	WT_DECL_RET;
	WT_TABLE *table;

	/* If we can get the colgroup, detach it from the table. */
	if ((ret = __wt_schema_get_colgroup(
	    session, uri, &table, &colgroup)) == 0) {
		table->cg_complete = 0;
		WT_TRET(__wt_schema_drop(session, colgroup->source, cfg));
	}

	WT_TRET(__wt_metadata_remove(session, uri));
	return (ret);
}

/*
 * __drop_index --
 *	WT_SESSION::drop for a colgroup.
 */
static int
__drop_index(
    WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	WT_INDEX *idx;
	WT_DECL_RET;
	WT_TABLE *table;

	/* If we can get the colgroup, detach it from the table. */
	if ((ret = __wt_schema_get_index(
	    session, uri, &table, &idx)) == 0) {
		table->idx_complete = 0;
		WT_TRET(__wt_schema_drop(session, idx->source, cfg));
	}

	WT_TRET(__wt_metadata_remove(session, uri));
	return (ret);
}

/*
 * __drop_table --
 *	WT_SESSION::drop for a table.
 */
static int
__drop_table(
    WT_SESSION_IMPL *session, const char *uri, int force, const char *cfg[])
{
	WT_COLGROUP *colgroup;
	WT_DECL_RET;
	WT_INDEX *idx;
	WT_TABLE *table;
	const char *name;
	u_int i;

	name = uri;
	(void)WT_PREFIX_SKIP(name, "table:");

	WT_ERR(__wt_schema_get_table(session, name, strlen(name), 1, &table));

	/* Drop the column groups. */
	for (i = 0; i < WT_COLGROUPS(table); i++) {
		if ((colgroup = table->cgroups[i]) == NULL)
			continue;
		WT_ERR(__wt_metadata_remove(session, colgroup->name));
		WT_ERR(__wt_schema_drop(session, colgroup->source, cfg));
	}

	/* Drop the indices. */
	WT_ERR(__wt_schema_open_indices(session, table));
	for (i = 0; i < table->nindices; i++) {
		if ((idx = table->indices[i]) == NULL)
			continue;
		WT_ERR(__wt_metadata_remove(session, idx->name));
		WT_ERR(__wt_schema_drop(session, idx->source, cfg));
	}

	WT_ERR(__wt_schema_remove_table(session, table));

	/* Remove the metadata entry (ignore missing items). */
	WT_ERR(__wt_metadata_remove(session, uri));

err:	if (force && ret == WT_NOTFOUND)
		ret = 0;
	return (ret);
}

int
__wt_schema_drop(WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_DATA_SOURCE *dsrc;
	WT_DECL_RET;
	int force;

	WT_RET(__wt_config_gets_defno(session, cfg, "force", &cval));
	force = (cval.val != 0);

	/* Disallow drops from the WiredTiger name space. */
	WT_RET(__wt_schema_name_check(session, uri));

	WT_RET(__wt_meta_track_on(session));

	/* Be careful to ignore any btree handle in our caller. */
	WT_CLEAR_BTREE_IN_SESSION(session);

	if (WT_PREFIX_MATCH(uri, "colgroup:"))
		ret = __drop_colgroup(session, uri, cfg);
	else if (WT_PREFIX_MATCH(uri, "file:"))
		ret = __drop_file(session, uri, force, cfg);
	else if (WT_PREFIX_MATCH(uri, "index:"))
		ret = __drop_index(session, uri, cfg);
	else if (WT_PREFIX_MATCH(uri, "table:"))
		ret = __drop_table(session, uri, force, cfg);
	else if ((ret = __wt_schema_get_source(session, uri, &dsrc)) == 0)
		ret = dsrc->drop(dsrc, &session->iface, uri, cfg);

	/*
	 * Map WT_NOTFOUND to ENOENT (or to 0 if "force" is set), based on the
	 * assumption WT_NOTFOUND means there was no metadata entry.  The
	 * underlying drop functions should handle this case (we passed them
	 * the "force" value), but better safe than sorry.
	 */
	if (ret == WT_NOTFOUND)
		ret = force ? 0 : ENOENT;

	/* Bump the schema generation so that stale data is ignored. */
	++S2C(session)->schema_gen;

	WT_TRET(__wt_meta_track_off(session, ret != 0));

	return (ret);
}
