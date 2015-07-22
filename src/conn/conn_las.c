/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_las_drop --
 *	Discard the database's lookaside file.
 */
int
__wt_las_drop(WT_SESSION_IMPL *session)
{
	const char *drop_cfg[] = {
	    WT_CONFIG_BASE(session, WT_SESSION_drop), "force=true", NULL };

	return (__wt_session_drop(session, WT_LASFILE_URI, drop_cfg));
}

/*
 * __wt_las_create --
 *	Create the database's lookaside file.
 */
int
__wt_las_create(WT_SESSION_IMPL *session)
{
	/* Remove any previous lookaside file. */
	WT_RET(__wt_las_drop(session));

	/* Create a new lookaside file. */
	WT_RET(__wt_session_create(
	    session, WT_LASFILE_URI, "key_format=u,value_format=u"));

	return (0);
}

/*
 * __las_open --
 *	Open the lookaside file, sets session->las_dhandle.
 */
static int
__las_open(WT_SESSION_IMPL *session)
{
	WT_RET(__wt_session_get_btree(session, WT_LASFILE_URI, NULL, NULL, 0));

	session->las_dhandle = session->dhandle;
	WT_ASSERT(session, session->las_dhandle != NULL);

	/* The lookaside handle doesn't need to stay locked -- release it. */
	return (__wt_session_release_btree(session));
}

/*
 * __wt_las_cursor --
 *	Open a cursor on the lookaside file.
 */
int
__wt_las_cursor(WT_SESSION_IMPL *session, WT_CURSOR **cursorp, int *clearp)
{
	WT_DATA_HANDLE *saved_dhandle;
	WT_DECL_RET;
	int is_dead;
	const char *cfg[] = {
	    WT_CONFIG_BASE(session, WT_SESSION_open_cursor),
	    "overwrite=false", NULL };

	*clearp = F_ISSET(session, WT_SESSION_NO_CACHE_CHECK) ? 0 : 1;

	/* Open and cache a handle if we don't yet have one. */
	saved_dhandle = session->dhandle;
	if (session->las_dhandle == NULL)
		WT_RET(__las_open(session));
	session->dhandle = session->las_dhandle;

	/* 
	 * We have the lookaside handle cached; lock it and increment the in-use
	 * counter once the cursor is open.
	 */
	WT_ERR(__wt_session_lock_dhandle(session, 0, &is_dead));

	/* The lookaside table should never be closed. */
	WT_ASSERT(session, !is_dead);

	WT_ERR(__wt_curfile_create(session, NULL, cfg, 0, 0, cursorp));
	__wt_cursor_dhandle_incr_use(session);

	/*
	 * No cache checks.
	 * No lookaside records during reconciliation.
	 * No checkpoints or logging.
	 */
	F_SET(session, WT_SESSION_NO_CACHE_CHECK);
	F_SET(S2BT(session),
	    WT_BTREE_LAS_FILE | WT_BTREE_NO_CHECKPOINT | WT_BTREE_NO_LOGGING);

	/* Restore the caller's btree. */
err:	session->dhandle = saved_dhandle;
	return (ret);
}

/*
 * __wt_las_cursor_close --
 *	Close a cursor on the lookaside file.
 */
int
__wt_las_cursor_close(WT_SESSION_IMPL *session, WT_CURSOR **cursorp, int clear)
{
	WT_CURSOR *cursor;

	if (clear)
		F_CLR(session, WT_SESSION_NO_CACHE_CHECK);

	if ((cursor = *cursorp) == NULL)
		return (0);

	*cursorp = NULL;
	return (cursor->close(cursor));
}

/*
 * __wt_las_insert --
 *	Insert a record into the lookaside store.
 */
int
__wt_las_insert(WT_SESSION_IMPL *session, WT_ITEM *key, WT_ITEM *value)
{
	WT_CONNECTION_IMPL *conn;
	WT_CURSOR *cursor;
	WT_DECL_RET;
	int clear;

	conn = S2C(session);

	/*
	 * For performance reasons, we don't check the lookaside table when
	 * freeing backing blocks during reconciliation until the table is
	 * in use.
	 */
	if (conn->reconcile_las == 0) {
		conn->reconcile_las = 1;
		WT_WRITE_BARRIER();
	}

	WT_RET(__wt_las_cursor(session, &cursor, &clear));
	cursor->set_key(cursor, key);
	cursor->set_value(cursor, value);
	ret = cursor->insert(cursor);

	WT_TRET(__wt_las_cursor_close(session, &cursor, clear));
	return (ret);
}
