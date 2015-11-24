/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __metadata_turtle --
 *	Return if a key's value should be taken from the turtle file.
 */
static bool
__metadata_turtle(const char *key)
{
	switch (key[0]) {
	case 'f':
		if (strcmp(key, WT_METAFILE_URI) == 0)
			return (true);
		break;
	case 'W':
		if (strcmp(key, "WiredTiger version") == 0)
			return (true);
		if (strcmp(key, "WiredTiger version string") == 0)
			return (true);
		break;
	}
	return (false);
}

/*
 * __wt_metadata_open --
 *	Opens the metadata file, sets session->meta_dhandle.
 */
int
__wt_metadata_open(WT_SESSION_IMPL *session)
{
	WT_BTREE *btree;

	if (session->meta_dhandle != NULL)
		return (0);

	WT_RET(__wt_session_get_btree(session, WT_METAFILE_URI, NULL, NULL, 0));

	session->meta_dhandle = session->dhandle;
	WT_ASSERT(session, session->meta_dhandle != NULL);

	/* 
	 * Set special flags for the metadata file: eviction (the metadata file
	 * is in-memory and never evicted), logging (the metadata file is always
	 * logged if possible).
	 *
	 * Test flags before setting them so updates can't race in subsequent
	 * opens (the first update is safe because it's single-threaded from
	 * wiredtiger_open).
	 */
	btree = S2BT(session);
	if (!F_ISSET(btree, WT_BTREE_IN_MEMORY))
		F_SET(btree, WT_BTREE_IN_MEMORY);
	if (!F_ISSET(btree, WT_BTREE_NO_EVICTION))
		F_SET(btree, WT_BTREE_NO_EVICTION);
	if (F_ISSET(btree, WT_BTREE_NO_LOGGING))
		F_CLR(btree, WT_BTREE_NO_LOGGING);

	/* The metadata handle doesn't need to stay locked -- release it. */
	return (__wt_session_release_btree(session));
}

/*
 * __wt_metadata_cursor --
 *	Opens a cursor on the metadata.
 */
int
__wt_metadata_cursor(
    WT_SESSION_IMPL *session, const char *config, WT_CURSOR **cursorp)
{
	WT_DATA_HANDLE *saved_dhandle;
	WT_DECL_RET;
	bool is_dead;
	const char *cfg[] =
	    { WT_CONFIG_BASE(session, WT_SESSION_open_cursor), config, NULL };

	saved_dhandle = session->dhandle;
	WT_ERR(__wt_metadata_open(session));

	session->dhandle = session->meta_dhandle;

	/* 
	 * We use the metadata a lot, so we have a handle cached; lock it and
	 * increment the in-use counter once the cursor is open.
	 */
	WT_ERR(__wt_session_lock_dhandle(session, 0, &is_dead));

	/* The metadata should never be closed. */
	WT_ASSERT(session, !is_dead);

	WT_ERR(__wt_curfile_create(session, NULL, cfg, false, false, cursorp));
	__wt_cursor_dhandle_incr_use(session);

	/* Restore the caller's btree. */
err:	session->dhandle = saved_dhandle;
	return (ret);
}

/*
 * __wt_metadata_insert --
 *	Insert a row into the metadata.
 */
int
__wt_metadata_insert(
    WT_SESSION_IMPL *session, const char *key, const char *value)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;

	WT_RET(__wt_verbose(session, WT_VERB_METADATA,
	    "Insert: key: %s, value: %s, tracking: %s, %s" "turtle",
	    key, value, WT_META_TRACKING(session) ? "true" : "false",
	    __metadata_turtle(key) ? "" : "not "));

	if (__metadata_turtle(key))
		WT_RET_MSG(session, EINVAL,
		    "%s: insert not supported on the turtle file", key);

	WT_RET(__wt_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	WT_ERR(cursor->insert(cursor));
	if (WT_META_TRACKING(session))
		WT_ERR(__wt_meta_track_insert(session, key));

err:	WT_TRET(cursor->close(cursor));
	return (ret);
}

/*
 * __wt_metadata_update --
 *	Update a row in the metadata.
 */
int
__wt_metadata_update(
    WT_SESSION_IMPL *session, const char *key, const char *value)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;

	WT_RET(__wt_verbose(session, WT_VERB_METADATA,
	    "Update: key: %s, value: %s, tracking: %s, %s" "turtle",
	    key, value, WT_META_TRACKING(session) ? "true" : "false",
	    __metadata_turtle(key) ? "" : "not "));

	if (__metadata_turtle(key)) {
		WT_WITH_TURTLE_LOCK(session,
		    ret = __wt_turtle_update(session, key, value));
		return (ret);
	}

	if (WT_META_TRACKING(session))
		WT_RET(__wt_meta_track_update(session, key));

	WT_RET(__wt_metadata_cursor(session, "overwrite", &cursor));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	WT_ERR(cursor->insert(cursor));

err:	WT_TRET(cursor->close(cursor));
	return (ret);
}

/*
 * __wt_metadata_remove --
 *	Remove a row from the metadata.
 */
int
__wt_metadata_remove(WT_SESSION_IMPL *session, const char *key)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;

	WT_RET(__wt_verbose(session, WT_VERB_METADATA,
	    "Remove: key: %s, tracking: %s, %s" "turtle",
	    key, WT_META_TRACKING(session) ? "true" : "false",
	    __metadata_turtle(key) ? "" : "not "));

	if (__metadata_turtle(key))
		WT_RET_MSG(session, EINVAL,
		    "%s: remove not supported on the turtle file", key);

	WT_RET(__wt_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, key);
	WT_ERR(cursor->search(cursor));
	if (WT_META_TRACKING(session))
		WT_ERR(__wt_meta_track_update(session, key));
	WT_ERR(cursor->remove(cursor));

err:	WT_TRET(cursor->close(cursor));
	return (ret);
}

/*
 * __wt_metadata_search --
 *	Return a copied row from the metadata.
 *	The caller is responsible for freeing the allocated memory.
 */
int
__wt_metadata_search(
    WT_SESSION_IMPL *session, const char *key, char **valuep)
{
	WT_CURSOR *cursor;
	WT_DECL_RET;
	const char *value;

	*valuep = NULL;

	WT_RET(__wt_verbose(session, WT_VERB_METADATA,
	    "Search: key: %s, tracking: %s, %s" "turtle",
	    key, WT_META_TRACKING(session) ? "true" : "false",
	    __metadata_turtle(key) ? "" : "not "));

	if (__metadata_turtle(key))
		return (__wt_turtle_read(session, key, valuep));

	/*
	 * All metadata reads are at read-uncommitted isolation.  That's
	 * because once a schema-level operation completes, subsequent
	 * operations must see the current version of checkpoint metadata, or
	 * they may try to read blocks that may have been freed from a file.
	 * Metadata updates use non-transactional techniques (such as the
	 * schema and metadata locks) to protect access to in-flight updates.
	 */
	WT_RET(__wt_metadata_cursor(session, NULL, &cursor));
	cursor->set_key(cursor, key);
	WT_WITH_TXN_ISOLATION(session, WT_ISO_READ_UNCOMMITTED,
	    ret = cursor->search(cursor));
	WT_ERR(ret);

	WT_ERR(cursor->get_value(cursor, &value));
	WT_ERR(__wt_strdup(session, value, valuep));

err:	WT_TRET(cursor->close(cursor));
	return (ret);
}
