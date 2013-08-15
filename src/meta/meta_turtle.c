/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __metadata_config --
 *	Return the default configuration information for the metadata file.
 */
static int
__metadata_config(WT_SESSION_IMPL *session, const char **metaconfp)
{
	WT_DECL_RET;
	WT_ITEM *buf;
	const char *cfg[] = { WT_CONFIG_BASE(session, file_meta), NULL, NULL };
	const char *metaconf;

	*metaconfp = NULL;

	buf = NULL;
	metaconf = NULL;

	/* Create a turtle file with default values. */
	WT_RET(__wt_scr_alloc(session, 0, &buf));
	WT_ERR(__wt_buf_fmt(session, buf,
	    "key_format=S,value_format=S,version=(major=%d,minor=%d)",
	    WT_BTREE_MAJOR_VERSION_MAX, WT_BTREE_MINOR_VERSION_MAX));
	cfg[1] = buf->data;
	WT_ERR(__wt_config_collapse(session, cfg, &metaconf));

	*metaconfp = metaconf;

	if (0) {
err:		__wt_free(session, metaconf);
	}
	__wt_scr_free(&buf);
	return (ret);
}

/*
 * __metadata_init --
 *	Create the metadata file.
 */
static int
__metadata_init(WT_SESSION_IMPL *session)
{
	WT_DECL_RET;

	/*
	 * We're single-threaded, but acquire the schema lock regardless: the
	 * lower level code checks that it is appropriately synchronized.
	 */
	WT_WITH_SCHEMA_LOCK(session,
	    ret = __wt_schema_create(session, WT_METADATA_URI, NULL));

	return (ret);
}

/*
 * __metadata_load_hot_backup --
 *	Load the contents of any hot backup file.
 */
static int
__metadata_load_hot_backup(WT_SESSION_IMPL *session)
{
	FILE *fp;
	WT_DECL_ITEM(key);
	WT_DECL_ITEM(value);
	WT_DECL_RET;
	const char *path;

	fp = NULL;
	path = NULL;

	/* Look for a hot backup file: if we find it, load it. */
	WT_RET(__wt_filename(session, WT_METADATA_BACKUP, &path));
	fp = fopen(path, "r");
	__wt_free(session, path);
	if (fp == NULL)
		return (0);

	/* Read line pairs and load them into the metadata file. */
	WT_ERR(__wt_scr_alloc(session, 512, &key));
	WT_ERR(__wt_scr_alloc(session, 512, &value));
	for (;;) {
		WT_ERR(__wt_getline(session, key, fp));
		if (key->size == 0)
			break;
		WT_ERR(__wt_getline(session, value, fp));
		if (value->size == 0)
			WT_ERR(__wt_illegal_value(session, WT_METADATA_BACKUP));
		WT_ERR(__wt_metadata_update(session, key->data, value->data));
	}

err:	if (fp != NULL)
		WT_TRET(fclose(fp) == 0 ? 0 : __wt_errno());
	__wt_scr_free(&key);
	__wt_scr_free(&value);
	return (ret);
}

/*
 * __wt_turtle_init --
 *	Check the turtle file and create if necessary.
 */
int
__wt_turtle_init(WT_SESSION_IMPL *session)
{
	WT_DECL_RET;
	int exist;
	const char *metaconf;

	metaconf = NULL;

	/*
	 * Discard any turtle setup file left-over from previous runs.  This
	 * doesn't matter for correctness, it's just cleaning up random files.
	 */
	WT_RET(__wt_exist(session, WT_METADATA_TURTLE_SET, &exist));
	if (exist)
		WT_RET(__wt_remove(session, WT_METADATA_TURTLE_SET));

	/*
	 * We could die after creating the turtle file and before creating the
	 * metadata file, or worse, the metadata file might be in some random
	 * state.  Make sure that doesn't happen: if we don't find the turtle
	 * file, first create the metadata file, load any hot backup, and then
	 * create the turtle file.  No matter what happens, if metadata file
	 * creation doesn't fully complete, we won't have a turtle file and we
	 * will repeat the process until we succeed.
	 *
	 * If there's already a turtle file, we're done.
	 */
	WT_RET(__wt_exist(session, WT_METADATA_TURTLE, &exist));
	if (exist)
		return (0);

	/* Create the metadata file. */
	WT_RET(__metadata_init(session));

	/* Load any hot-backup information. */
	WT_RET(__metadata_load_hot_backup(session));

	/* Create the turtle file. */
	WT_RET(__metadata_config(session, &metaconf));
	WT_ERR(__wt_turtle_update(session, WT_METADATA_URI, metaconf));

	/* Remove the backup file if it exists, we'll never read it again. */
	WT_ERR(__wt_exist(session, WT_METADATA_BACKUP, &exist));
	if (exist)
		WT_ERR(__wt_remove(session, WT_METADATA_BACKUP));

err:	__wt_free(session, metaconf);
	return (ret);
}

/*
 * __wt_turtle_read --
 *	Read the turtle file.
 */
int
__wt_turtle_read(WT_SESSION_IMPL *session, const char *key, const char **valuep)
{
	FILE *fp;
	WT_DECL_ITEM(buf);
	WT_DECL_RET;
	int match;
	const char *path;

	*valuep = NULL;

	fp = NULL;
	path = NULL;

	/*
	 * Open the turtle file; there's one case where we won't find the turtle
	 * file, yet still succeed.  We create the metadata file before creating
	 * the turtle file, and that means returning the default configuration
	 * string for the metadata file.
	 */
	WT_RET(__wt_filename(session, WT_METADATA_TURTLE, &path));
	if ((fp = fopen(path, "r")) == NULL)
		ret = __wt_errno();
	__wt_free(session, path);
	if (fp == NULL)
		return (strcmp(key, WT_METADATA_URI) == 0 ?
		    __metadata_config(session, valuep) : ret);

	/* Search for the key. */
	WT_ERR(__wt_scr_alloc(session, 512, &buf));
	for (match = 0;;) {
		WT_ERR(__wt_getline(session, buf, fp));
		if (buf->size == 0)
			WT_ERR(WT_NOTFOUND);
		if (strcmp(key, buf->data) == 0)
			match = 1;

		/* Key matched: read the subsequent line for the value. */
		WT_ERR(__wt_getline(session, buf, fp));
		if (buf->size == 0)
			WT_ERR(__wt_illegal_value(session, WT_METADATA_TURTLE));
		if (match)
			break;
	}

	/* Copy the value for the caller. */
	WT_ERR(__wt_strdup(session, buf->data, valuep));

err:	if (fp != NULL)
		WT_TRET(fclose(fp) == 0 ? 0 : __wt_errno());
	__wt_scr_free(&buf);
	return (ret);
}

/*
 * __wt_turtle_update --
 *	Update the turtle file.
 */
int
__wt_turtle_update(
    WT_SESSION_IMPL *session, const char *key,  const char *value)
{
	FILE *fp;
	WT_DECL_RET;
	int vmajor, vminor, vpatch;
	const char *path, *version;

	fp = NULL;
	path = NULL;

	/*
	 * Create the turtle setup file: we currently re-write it from scratch
	 * every time.
	 */
	WT_RET(__wt_filename(session, WT_METADATA_TURTLE_SET, &path));
	if ((fp = fopen(path, "w")) == NULL)
		ret = __wt_errno();
	__wt_free(session, path);
	if (fp == NULL)
		return (ret);

	version = wiredtiger_version(&vmajor, &vminor, &vpatch);
	WT_ERR_TEST((fprintf(fp,
	    "%s\n%s\n%s\n" "major=%d,minor=%d,patch=%d\n%s\n%s\n",
	    WT_METADATA_VERSION_STR, version,
	    WT_METADATA_VERSION, vmajor, vminor, vpatch,
	    key, value) < 0), __wt_errno());

	ret = fclose(fp);
	fp = NULL;
	WT_ERR_TEST(ret == EOF, __wt_errno());

	WT_ERR(
	    __wt_rename(session, WT_METADATA_TURTLE_SET, WT_METADATA_TURTLE));

	if (0) {
err:		WT_TRET(__wt_remove(session, WT_METADATA_TURTLE_SET));
	}

	if  (fp != NULL)
		WT_TRET(fclose(fp) == 0 ? 0 : __wt_errno());
	return (ret);
}
