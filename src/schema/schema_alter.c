/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __alter_file --
 *	Alter a file.
 */
static int
__alter_file(
    WT_SESSION_IMPL *session, const char *uri, const char *newcfg[])
{
	WT_CONFIG_ITEM oldval, newval;
	WT_DECL_RET;
	bool change;
	const char *cfg[3], *filename;
	char *config, *newconfig;

	change = false;
	filename = uri;
	newconfig = NULL;
	if (!WT_PREFIX_SKIP(filename, "file:"))
		return (__wt_unexpected_object_type(session, uri, "file:"));

	/* Remove the metadata entry (ignore missing items). */
	WT_RET(__wt_metadata_search(session, uri, &config));

	cfg[0] = config;
	cfg[1] = NULL;

	WT_ASSERT(session, newcfg[0] != NULL);
	WT_ERR(__wt_config_gets(session, cfg,
	    "access_pattern_hint", &oldval));
	/*
	 * We're only using what is given to us by the user so it may not be
	 * a complete set of allowed options for alter.  So WT_NOTFOUND is
	 * an allowed error.  We don't use WT_ERR_NOTFOUND_OK because we
	 * don't want to lose the WT_NOTFOUND value.
	 */
	ret = __wt_config_gets(session, newcfg,
	    "access_pattern_hint", &newval);

	if (ret == 0 && !WT_STRING_MATCH(newval.str, oldval.str, oldval.len))
		change = true;
	else {
		WT_ERR_NOTFOUND_OK(ret);
		WT_ERR(__wt_config_gets(session, cfg,
		    "cache_resident", &oldval));
		ret = __wt_config_gets(session, newcfg,
		    "cache_resident", &newval);
		if (ret == 0 && oldval.val != newval.val)
			change = true;
		WT_ERR_NOTFOUND_OK(ret);

	}

	/*
	 * Only rewrite if there are changes.
	 */
	if (change) {
		/*
		 * Add in the new config and collapse.
		 */
		WT_ASSERT(session, newcfg[0] != NULL);
		cfg[1] = newcfg[0];
		cfg[2] = NULL;
		WT_ERR(__wt_config_collapse(session, cfg, &newconfig));
		WT_ERR(__wt_metadata_update(session, uri, newconfig));
	}

err:	__wt_free(session, config);
	__wt_free(session, newconfig);
	return (ret);
}

/*
 * __alter_colgroup --
 *	WT_SESSION::alter for a colgroup.
 */
static int
__alter_colgroup(
    WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	WT_COLGROUP *colgroup;
	WT_DECL_RET;

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_LOCKED_TABLE));

	/* If we can get the colgroup, perform any potential alterations. */
	if ((ret = __wt_schema_get_colgroup(
	    session, uri, false, NULL, &colgroup)) == 0)
		WT_TRET(__wt_schema_alter(session, colgroup->source, cfg));

	return (ret);
}

/*
 * __alter_index --
 *	WT_SESSION::alter for an index.
 */
static int
__alter_index(
    WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	WT_INDEX *idx;
	WT_DECL_RET;

	/* If we can get the index, perform any potential alterations. */
	if ((ret = __wt_schema_get_index(
	    session, uri, false, NULL, &idx)) == 0)
		WT_TRET(__wt_schema_alter(session, idx->source, cfg));

	return (ret);
}

/*
 * __alter_table --
 *	WT_SESSION::alter for a table.
 */
static int
__alter_table(WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	WT_COLGROUP *colgroup;
	WT_DECL_RET;
	WT_TABLE *table;
	const char *tblcfg[2], *name;
	u_int i;
	int ncolgroups;

	name = uri;
	(void)WT_PREFIX_SKIP(name, "table:");

	WT_RET(__wt_schema_get_table(
	    session, name, strlen(name), true, &table));

	/*
	 * Get the original configuration for the table to see if it is
	 * using the default column group.
	 */
	tblcfg[0] = table->config;
	tblcfg[1] = NULL;
	WT_ERR(__wt_table_colgroups_config(session, tblcfg, &ncolgroups));

	/*
	 * Alter the column groups only if we are using the default
	 * column group.  Otherwise the user should alter each
	 * index or column group explicitly.
	 */
	if (ncolgroups == 0)
		for (i = 0; i < WT_COLGROUPS(table); i++) {
			if ((colgroup = table->cgroups[i]) == NULL)
				continue;
			/*
			 * Alter the column group before updating the metadata
			 * to avoid the metadata for the table becoming
			 * inconsistent if we can't get exclusive access.
			 */
			WT_ERR(__wt_schema_alter(
			    session, colgroup->source, cfg));
		}
err:	__wt_schema_release_table(session, table);
	return (ret);
}

/*
 * __wt_schema_alter --
 *	Process a WT_SESSION::alter operation for all supported types.
 */
int
__wt_schema_alter(WT_SESSION_IMPL *session, const char *uri, const char *cfg[])
{
	WT_DECL_RET;

	WT_RET(__wt_meta_track_on(session));

	/* Paranoia: clear any handle from our caller. */
	session->dhandle = NULL;

	if (WT_PREFIX_MATCH(uri, "colgroup:"))
		ret = __alter_colgroup(session, uri, cfg);
	else if (WT_PREFIX_MATCH(uri, "file:"))
		ret = __alter_file(session, uri, cfg);
	else if (WT_PREFIX_MATCH(uri, "index:"))
		ret = __alter_index(session, uri, cfg);
	else if (WT_PREFIX_MATCH(uri, "lsm:"))
		ret = __wt_lsm_tree_alter(session, uri, cfg);
	else if (WT_PREFIX_MATCH(uri, "table:"))
		ret = __alter_table(session, uri, cfg);
	else
		ret = __wt_bad_object_type(session, uri);

	/*
	 * Map WT_NOTFOUND to ENOENT, based on the assumption WT_NOTFOUND means
	 * there was no metadata entry.
	 */
	if (ret == WT_NOTFOUND)
		ret = ENOENT;

	/* Bump the schema generation so that stale data is ignored. */
	++S2C(session)->schema_gen;

	WT_TRET(__wt_meta_track_off(session, true, ret != 0));

	return (ret);
}
