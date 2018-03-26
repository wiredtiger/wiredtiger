/*-
 * Copyright (c) 2014-2018 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_alter --
 *	Alter an object
 */
static int
__wt_alter(WT_SESSION_IMPL *session,
    const char *uri, const char *newcfg[], const char *base_config)
{
	WT_DECL_RET;
	const char *cfg[4];
	char *config, *newconfig;

	newconfig = NULL;

	/* Find the URI */
	WT_RET(__wt_metadata_search(session, uri, &config));

	WT_ASSERT(session, newcfg[0] != NULL);

	/*
	 * Start with the base configuration because collapse is like
	 * a projection and if we are reading older metadata, it may not
	 * have all the components.
	 */
	cfg[0] = base_config;
	cfg[1] = config;
	cfg[2] = newcfg[0];
	cfg[3] = NULL;
	WT_ERR(__wt_config_collapse(session, cfg, &newconfig));
	/*
	 * Only rewrite if there are changes.
	 */
	if (strcmp(config, newconfig) != 0)
		WT_ERR(__wt_metadata_update(session, uri, newconfig));
	else
		WT_STAT_CONN_INCR(session, session_table_alter_skip);

err:	__wt_free(session, config);
	__wt_free(session, newconfig);
	/*
	 * Map WT_NOTFOUND to ENOENT, based on the assumption WT_NOTFOUND means
	 * there was no metadata entry.
	 */
	if (ret == WT_NOTFOUND)
		ret = ENOENT;

	return (ret);
}

/*
 * __wt_alter_file --
 *	Alter a file.
 */
static int
__wt_alter_file(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_DECL_RET;
	const char *filename, *uri;

	/*
	 * We know that we have exclusive access to the file.  So it will be
	 * closed after we're done with it and the next open will see the
	 * updated metadata.
	 */
	filename = uri = session->dhandle->name;
	if (!WT_PREFIX_SKIP(filename, "file:"))
		return (__wt_unexpected_object_type(session, uri, "file:"));

	WT_RET(__wt_meta_track_on(session));

	WT_ERR(__wt_alter(session,
	    uri, cfg, WT_CONFIG_BASE(session, file_meta)));

err:	WT_TRET(__wt_meta_track_off(session, true, ret != 0));
	return (ret);
}

/*
 * __alter_tree --
 *	Alter an index or colgroup reference.
 */
static int
__alter_tree(WT_SESSION_IMPL *session, const char *name, const char *cfg[])
{
	WT_CONFIG_ITEM cval;
	WT_DECL_ITEM(data_source);
	WT_DECL_RET;
	char *value;
	bool is_colgroup;

	value = NULL;

	is_colgroup = WT_PREFIX_MATCH(name, "colgroup:");
	if (!is_colgroup && !WT_PREFIX_MATCH(name, "index:"))
		WT_ERR_MSG(session, EINVAL,
		    "expected a 'colgroup:' or 'index:' source: '%s'", name);

	/* Read the schema value. */
	WT_ERR(__wt_metadata_search(session, name, &value));

	/* Get the data source URI. */
	if ((ret = __wt_config_getones(session, value, "source", &cval)) != 0)
		WT_ERR_MSG(session, EINVAL,
		    "index or column group has no data source: %s", value);

	WT_ERR(__wt_scr_alloc(session, 0, &data_source));
	WT_ERR(__wt_buf_fmt(session,
	    data_source, "%.*s", (int)cval.len, cval.str));

	/* Alter the data source */
	WT_ERR(__wt_schema_alter(session, data_source->data, cfg));

	/* Alter the index or colgroup */
	WT_ERR(__wt_schema_alter(session, name, cfg));

err:	__wt_scr_free(session, &data_source);
	__wt_free(session, value);
	return (ret);
}

/*
 * __alter_table --
 *	WT_SESSION::alter for a table.
 */
static int
__alter_table(
    WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	WT_COLGROUP *colgroup;
	WT_DECL_RET;
	WT_INDEX *idx;
	WT_TABLE *table;
	u_int i;
	const char *name;
	bool tracked;

	name = uri;
	WT_PREFIX_SKIP_REQUIRED(session, name, "table:");

	table = NULL;
	tracked = false;

	/*
	 * Open the table so we can alter its column groups and indexes, keeping
	 * the table locked exclusive across the alter.
	 */
	WT_ERR(__wt_schema_get_table_uri(session, uri, true,
	    WT_DHANDLE_EXCLUSIVE, &table));

	colgroup = NULL;
	/* Alter the column groups. */
	for (i = 0; i < WT_COLGROUPS(table); i++) {
		if ((colgroup = table->cgroups[i]) == NULL)
			continue;
		WT_ERR(__alter_tree(session, colgroup->name, cfg));
	}

	/* Alter the indices. */
	WT_ERR(__wt_schema_open_indices(session, table));
	for (i = 0; i < table->nindices; i++) {
		if ((idx = table->indices[i]) == NULL)
			continue;
		WT_ERR(__alter_tree(session, idx->name, cfg));
	}

	/* Alter the table */
	WT_ERR(__wt_alter(session,
	    uri, cfg, WT_CONFIG_BASE(session, table_meta)));

	/* Alter the underlying file */
	/*
	 * Note: Ideally we should call __wt_schema_alter() on the underlying
	 * "file:" uri.
	 */
	WT_ERR(__wt_schema_worker(session, uri, __wt_alter_file,
	    NULL, cfg, WT_BTREE_ALTER | WT_DHANDLE_EXCLUSIVE));

	if (WT_META_TRACKING(session)) {
		WT_WITH_DHANDLE(session, &table->iface,
		    ret = __wt_meta_track_handle_lock(session, false));
		WT_ERR(ret);
		tracked = true;
	}

err:	if (table != NULL && !tracked)
		WT_TRET(__wt_schema_release_table(session, table));
	return (ret);
}

/*
 * __wt_schema_alter --
 *	Alter an object.
 */
int
__wt_schema_alter(WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	if (WT_PREFIX_MATCH(uri, "file:"))
		return (__wt_schema_worker(session, uri, __wt_alter_file,
		    NULL, cfg, WT_BTREE_ALTER | WT_DHANDLE_EXCLUSIVE));
	else if (WT_PREFIX_MATCH(uri, "colgroup:"))
		return (__wt_alter(session,
		    uri, cfg, WT_CONFIG_BASE(session, colgroup_meta)));
	else if (WT_PREFIX_MATCH(uri, "index:"))
		return (__wt_alter(session,
		    uri, cfg, WT_CONFIG_BASE(session, index_meta)));
	else if (WT_PREFIX_MATCH(uri, "lsm:"))
		return (__wt_lsm_tree_worker(session, uri, __wt_alter_file,
		    NULL, cfg, WT_BTREE_ALTER | WT_DHANDLE_EXCLUSIVE));
	else if (WT_PREFIX_MATCH(uri, "table:"))
		return (__alter_table(session, uri, cfg));

	return (__wt_bad_object_type(session, uri));
}
