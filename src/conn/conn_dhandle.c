/*-
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __conn_dhandle_open_lock --
 *	Spin on the current data handle until either (a) it is open, read
 *	locked; or (b) it is closed, write locked.  If exclusive access is
 *	requested and cannot be granted immediately, fail with EBUSY.
 */
static int
__conn_dhandle_open_lock(
    WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, uint32_t flags)
{
	WT_BTREE *btree;
	WT_DECL_RET;

	btree = dhandle->handle;

	/*
	 * Check that the handle is open.  We've already incremented
	 * the reference count, so once the handle is open it won't be
	 * closed by another thread.
	 *
	 * If we can see the WT_DHANDLE_OPEN flag set while holding a
	 * lock on the handle, then it's really open and we can start
	 * using it.  Alternatively, if we can get an exclusive lock
	 * and WT_DHANDLE_OPEN is still not set, we need to do the open.
	 */
	for (;;) {
		if (!LF_ISSET(WT_DHANDLE_EXCLUSIVE) &&
		    F_ISSET(btree, WT_BTREE_SPECIAL_FLAGS))
			return (EBUSY);

		if (F_ISSET(dhandle, WT_DHANDLE_OPEN) &&
		    !LF_ISSET(WT_DHANDLE_EXCLUSIVE)) {
			WT_RET(__wt_readlock(session, dhandle->rwlock));
			if (F_ISSET(dhandle, WT_DHANDLE_OPEN))
				return (0);
			WT_RET(__wt_rwunlock(session, dhandle->rwlock));
		}

		/*
		 * It isn't open or we want it exclusive: try to get an
		 * exclusive lock.  There is some subtlety here: if we race
		 * with another thread that successfully opens the file, we
		 * don't want to block waiting to get exclusive access.
		 */
		if ((ret = __wt_try_writelock(session, dhandle->rwlock)) == 0) {
			/*
			 * If it was opened while we waited, drop the write
			 * lock and get a read lock instead.
			 */
			if (F_ISSET(dhandle, WT_DHANDLE_OPEN) &&
			    !LF_ISSET(WT_DHANDLE_EXCLUSIVE)) {
				WT_RET(__wt_rwunlock(session, dhandle->rwlock));
				continue;
			}

			/* We have an exclusive lock, we're done. */
			F_SET(dhandle, WT_DHANDLE_EXCLUSIVE);
			return (0);
		} else if (ret != EBUSY || LF_ISSET(WT_DHANDLE_EXCLUSIVE))
			return (EBUSY);

		/* Give other threads a chance to make progress. */
		__wt_yield();
	}
}

/*
 * __conn_dhandle_get --
 *	Find an open btree file handle, otherwise create a new one, lock it
 * exclusively, and return it linked into the connection's list.
 */
static int
__conn_dhandle_get(WT_SESSION_IMPL *session,
    const char *name, const char *ckpt, uint32_t flags)
{
	WT_BTREE *btree;
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	uint64_t hash;

	conn = S2C(session);

	/* We must be holding the schema lock at a higher level. */
	WT_ASSERT(session, F_ISSET(session, WT_SESSION_SCHEMA_LOCKED) &&
	    !LF_ISSET(WT_DHANDLE_HAVE_REF));

	/* Increment the reference count if we already have the btree open. */
	hash = __wt_hash_city64(name, strlen(name));
	SLIST_FOREACH(dhandle, &conn->dhlh, l)
		if ((hash == dhandle->name_hash &&
		     strcmp(name, dhandle->name) == 0) &&
		    ((ckpt == NULL && dhandle->checkpoint == NULL) ||
		    (ckpt != NULL && dhandle->checkpoint != NULL &&
		    strcmp(ckpt, dhandle->checkpoint) == 0))) {
			WT_RET(__conn_dhandle_open_lock(
			    session, dhandle, flags));
			(void)WT_ATOMIC_ADD(dhandle->session_ref, 1);
			session->dhandle = dhandle;
			return (0);
		}

	/*
	 * Allocate the data source handle and underlying btree handle, then
	 * initialize the data source handle.  Exclusively lock the data
	 * source handle before inserting it in the list.
	 */
	WT_RET(__wt_calloc_def(session, 1, &dhandle));

	WT_ERR(__wt_rwlock_alloc(session, "data handle", &dhandle->rwlock));
	dhandle->session_ref = 1;

	dhandle->name_hash = hash;
	WT_ERR(__wt_strdup(session, name, &dhandle->name));
	if (ckpt != NULL)
		WT_ERR(__wt_strdup(session, ckpt, &dhandle->checkpoint));

	WT_ERR(__wt_calloc_def(session, 1, &btree));
	dhandle->handle = btree;
	btree->dhandle = dhandle;

	WT_ERR(__wt_spin_init(
	    session, &dhandle->close_lock, "data handle close"));

	F_SET(dhandle, WT_DHANDLE_EXCLUSIVE);
	WT_ERR(__wt_writelock(session, dhandle->rwlock));

	/*
	 * Prepend the handle to the connection list, assuming we're likely to
	 * need new files again soon, until they are cached by all sessions.
	 *
	 * !!!
	 * We hold only the schema lock here, not the dhandle lock.  Eviction
	 * walks this list only holding the dhandle lock.  This works because
	 * we're inserting at the beginning of the list, and we're only
	 * publishing this one entry per lock acquisition.  Eviction either
	 * sees our newly added entry or the former head of the list, and it
	 * doesn't matter which (if eviction only sees a single element in the
	 * list because the insert races, it will return without finding enough
	 * candidates for eviction, and will then retry).
	 */
	SLIST_INSERT_HEAD(&conn->dhlh, dhandle, l);

	session->dhandle = dhandle;
	return (0);

err:	WT_TRET(__wt_rwlock_destroy(session, &dhandle->rwlock));
	__wt_free(session, dhandle->name);
	__wt_free(session, dhandle->checkpoint);
	__wt_free(session, dhandle->handle);		/* btree free */
	__wt_spin_destroy(session, &dhandle->close_lock);
	__wt_overwrite_and_free(session, dhandle);

	return (ret);
}

/*
 * __wt_conn_btree_sync_and_close --
 *	Sync and close the underlying btree handle.
 */
int
__wt_conn_btree_sync_and_close(WT_SESSION_IMPL *session)
{
	WT_BTREE *btree;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	int no_schema_lock;

	dhandle = session->dhandle;
	btree = S2BT(session);

	if (!F_ISSET(dhandle, WT_DHANDLE_OPEN))
		return (0);

	/*
	 * If we don't already have the schema lock, make it an error to try
	 * to acquire it.  The problem is that we are holding an exclusive
	 * lock on the handle, and if we attempt to acquire the schema lock
	 * we might deadlock with a thread that has the schema lock and wants
	 * a handle lock (specifically, checkpoint).
	 */
	no_schema_lock = 0;
	if (!F_ISSET(session, WT_SESSION_SCHEMA_LOCKED)) {
		no_schema_lock = 1;
		F_SET(session, WT_SESSION_NO_SCHEMA_LOCK);
	}

	/*
	 * We may not be holding the schema lock, and threads may be walking
	 * the list of open handles (for example, checkpoint).  Acquire the
	 * handle's close lock.
	 */
	__wt_spin_lock(session, &dhandle->close_lock);

	/*
	 * The close can fail if an update cannot be written, return the EBUSY
	 * error to our caller for eventual retry.
	 */
	if (!F_ISSET(btree,
	    WT_BTREE_SALVAGE | WT_BTREE_UPGRADE | WT_BTREE_VERIFY))
		WT_ERR(__wt_checkpoint_close(session));

	if (dhandle->checkpoint == NULL)
		--S2C(session)->open_btree_count;

	WT_TRET(__wt_btree_close(session));
	F_CLR(dhandle, WT_DHANDLE_OPEN);
	F_CLR(btree, WT_BTREE_SPECIAL_FLAGS);

err:	__wt_spin_unlock(session, &dhandle->close_lock);

	if (no_schema_lock)
		F_CLR(session, WT_SESSION_NO_SCHEMA_LOCK);

	return (ret);
}

/*
 * __conn_btree_config_clear --
 *	Clear the underlying object's configuration information.
 */
static void
__conn_btree_config_clear(WT_SESSION_IMPL *session)
{
	WT_DATA_HANDLE *dhandle;
	const char **a;

	dhandle = session->dhandle;

	if (dhandle->cfg == NULL)
		return;
	for (a = dhandle->cfg; *a != NULL; ++a)
		__wt_free(session, *a);
	__wt_free(session, dhandle->cfg);
}

/*
 * __conn_btree_config_set --
 *	Set up a btree handle's configuration information.
 */
static int
__conn_btree_config_set(WT_SESSION_IMPL *session)
{
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	const char *metaconf;

	dhandle = session->dhandle;

	/*
	 * Read the object's entry from the metadata file, we're done if we
	 * don't find one.
	 */
	if ((ret =
	    __wt_metadata_search(session, dhandle->name, &metaconf)) != 0) {
		if (ret == WT_NOTFOUND)
			ret = ENOENT;
		WT_RET(ret);
	}

	/*
	 * The defaults are included because underlying objects have persistent
	 * configuration information stored in the metadata file.  If defaults
	 * are included in the configuration, we can add new configuration
	 * strings without upgrading the metadata file or writing special code
	 * in case a configuration string isn't initialized, as long as the new
	 * configuration string has an appropriate default value.
	 *
	 * The error handling is a little odd, but be careful: we're holding a
	 * chunk of allocated memory in metaconf.  If we fail before we copy a
	 * reference to it into the object's configuration array, we must free
	 * it, after the copy, we don't want to free it.
	 */
	WT_ERR(__wt_calloc_def(session, 3, &dhandle->cfg));
	WT_ERR(__wt_strdup(
	    session, WT_CONFIG_BASE(session, file_meta), &dhandle->cfg[0]));
	dhandle->cfg[1] = metaconf;
	return (0);

err:	__wt_free(session, metaconf);
	return (ret);
}

/*
 * __conn_btree_open --
 *	Open the current btree handle.
 */
static int
__conn_btree_open(
	WT_SESSION_IMPL *session, const char *op_cfg[], uint32_t flags)
{
	WT_BTREE *btree;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;

	dhandle = session->dhandle;
	btree = S2BT(session);

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_SCHEMA_LOCKED) &&
	    F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE) &&
	    !LF_ISSET(WT_DHANDLE_LOCK_ONLY));

	/*
	 * If the handle is already open, it has to be closed so it can be
	 * reopened with a new configuration.  We don't need to check again:
	 * this function isn't called if the handle is already open in the
	 * required mode.
	 *
	 * This call can return EBUSY if there's an update in the object that's
	 * not yet globally visible.  That's not a problem because it can only
	 * happen when we're switching from a normal handle to a "special" one,
	 * so we're returning EBUSY to an attempt to verify or do other special
	 * operations.  The reverse won't happen because when the handle from a
	 * verify or other special operation is closed, there won't be updates
	 * in the tree that can block the close.
	 */
	if (F_ISSET(dhandle, WT_DHANDLE_OPEN))
		WT_RET(__wt_conn_btree_sync_and_close(session));

	/* Discard any previous configuration, set up the new configuration. */
	__conn_btree_config_clear(session);
	WT_RET(__conn_btree_config_set(session));

	/* Set any special flags on the handle. */
	F_SET(btree, LF_ISSET(WT_BTREE_SPECIAL_FLAGS));

	do {
		WT_ERR(__wt_btree_open(session, op_cfg));
		F_SET(dhandle, WT_DHANDLE_OPEN);
		/*
		 * Checkpoint handles are read only, so eviction calculations
		 * based on the number of btrees are better to ignore them.
		 */
		if (dhandle->checkpoint == NULL)
			++S2C(session)->open_btree_count;

		/* Drop back to a readlock if that is all that was needed. */
		if (!LF_ISSET(WT_DHANDLE_EXCLUSIVE)) {
			F_CLR(dhandle, WT_DHANDLE_EXCLUSIVE);
			WT_ERR(__wt_rwunlock(session, dhandle->rwlock));
			WT_ERR(
			    __conn_dhandle_open_lock(session, dhandle, flags));
		}
	} while (!F_ISSET(dhandle, WT_DHANDLE_OPEN));

	if (0) {
err:		F_CLR(btree, WT_BTREE_SPECIAL_FLAGS);
		/*
		 * If the open failed, close the handle.  If there was no
		 * reference to the handle in this session, we incremented the
		 * session reference count, so decrement it here.  Otherwise,
		 * just close the handle without decrementing.
		 */
		if (!LF_ISSET(WT_DHANDLE_HAVE_REF))
			__wt_conn_btree_close(session);
		else if (F_ISSET(dhandle, WT_DHANDLE_OPEN))
			WT_TRET(__wt_conn_btree_sync_and_close(session));
	}

	return (ret);
}

/*
 * __wt_conn_btree_get --
 *	Get an open btree file handle, otherwise open a new one.
 */
int
__wt_conn_btree_get(WT_SESSION_IMPL *session,
    const char *name, const char *ckpt, const char *op_cfg[], uint32_t flags)
{
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;

	if (LF_ISSET(WT_DHANDLE_HAVE_REF))
		WT_RET(
		    __conn_dhandle_open_lock(session, session->dhandle, flags));
	else
		WT_RET(__conn_dhandle_get(session, name, ckpt, flags));
	dhandle = session->dhandle;

	if (!LF_ISSET(WT_DHANDLE_LOCK_ONLY) &&
	    (!F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
	    LF_ISSET(WT_BTREE_SPECIAL_FLAGS)))
		if ((ret = __conn_btree_open(session, op_cfg, flags)) != 0) {
			F_CLR(dhandle, WT_DHANDLE_EXCLUSIVE);
			WT_TRET(__wt_rwunlock(session, dhandle->rwlock));
		}

	WT_ASSERT(session, ret != 0 ||
	    LF_ISSET(WT_DHANDLE_EXCLUSIVE) ==
	    F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE));

	return (ret);
}

/*
 * __wt_conn_btree_apply --
 *	Apply a function to all open btree handles apart from the metadata
 * file.
 */
int
__wt_conn_btree_apply(WT_SESSION_IMPL *session,
    int apply_checkpoints,
    int (*func)(WT_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;

	conn = S2C(session);

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_SCHEMA_LOCKED));

	SLIST_FOREACH(dhandle, &conn->dhlh, l)
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN) &&
		    WT_PREFIX_MATCH(dhandle->name, "file:") &&
		    (apply_checkpoints || dhandle->checkpoint == NULL) &&
		    !WT_IS_METADATA(dhandle)) {
			/*
			 * We need to pull the handle into the session handle
			 * cache and make sure it's referenced to stop other
			 * internal code dropping the handle (e.g in LSM when
			 * cleaning up obsolete chunks). Holding the metadata
			 * lock isn't enough.
			 */
			ret = __wt_session_get_btree(session,
			    dhandle->name, dhandle->checkpoint, NULL, 0);
			if (ret == 0) {
				ret = func(session, cfg);
				if (WT_META_TRACKING(session))
					WT_TRET(__wt_meta_track_handle_lock(
					    session, 0));
				else
					WT_TRET(__wt_session_release_btree(
					    session));
			} else if (ret == EBUSY)
				ret = __wt_conn_btree_apply_single(
				    session, dhandle->name,
				    dhandle->checkpoint, func, cfg);
			WT_RET(ret);
		}

	return (0);
}

/*
 * __wt_conn_btree_apply_single --
 *	Apply a function to a single btree handle that's being bulk-loaded.
 */
int
__wt_conn_btree_apply_single(WT_SESSION_IMPL *session,
    const char *uri, const char *checkpoint,
    int (*func)(WT_SESSION_IMPL *, const char *[]), const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle, *saved_dhandle;
	WT_DECL_RET;

	conn = S2C(session);
	saved_dhandle = session->dhandle;

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_SCHEMA_LOCKED));

	SLIST_FOREACH(dhandle, &conn->dhlh, l)
		if (strcmp(dhandle->name, uri) == 0 &&
		    ((dhandle->checkpoint == NULL && checkpoint == NULL) ||
		    (dhandle->checkpoint != NULL && checkpoint != NULL &&
		    strcmp(dhandle->checkpoint, checkpoint) == 0))) {
			/*
			 * We're holding the schema lock which locks out handle
			 * open (which might change the state of the underlying
			 * object).  However, closing a handle doesn't require
			 * the schema lock, lock out closing the handle and then
			 * confirm the handle is still open.
			 */
			__wt_spin_lock(session, &dhandle->close_lock);
			ret = 0;
			if (F_ISSET(dhandle, WT_DHANDLE_OPEN)) {
				session->dhandle = dhandle;
				ret = func(session, cfg);
			}
			__wt_spin_unlock(session, &dhandle->close_lock);
			WT_ERR(ret);
		}

err:	session->dhandle = saved_dhandle;
	return (ret);
}

/*
 * __wt_conn_btree_close --
 *	Discard a reference to an open btree file handle.
 */
void
__wt_conn_btree_close(WT_SESSION_IMPL *session)
{
	(void)WT_ATOMIC_SUB(session->dhandle->session_ref, 1);
}

/*
 * __wt_conn_dhandle_close_all --
 *	Close all data handles handles with matching name (including all
 *	checkpoint handles).
 */
int
__wt_conn_dhandle_close_all(WT_SESSION_IMPL *session, const char *name)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;

	conn = S2C(session);

	WT_ASSERT(session, F_ISSET(session, WT_SESSION_SCHEMA_LOCKED));
	WT_ASSERT(session, session->dhandle == NULL);

	SLIST_FOREACH(dhandle, &conn->dhlh, l) {
		if (strcmp(dhandle->name, name) != 0)
			continue;

		session->dhandle = dhandle;

		/* Lock the handle exclusively. */
		WT_ERR(__wt_session_get_btree(session,
		    dhandle->name, dhandle->checkpoint,
		    NULL, WT_DHANDLE_EXCLUSIVE | WT_DHANDLE_LOCK_ONLY));
		if (WT_META_TRACKING(session))
			WT_ERR(__wt_meta_track_handle_lock(session, 0));

		/*
		 * We have an exclusive lock, which means there are no cursors
		 * open at this point.  Close the handle, if necessary.
		 */
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN)) {
			ret = __wt_meta_track_sub_on(session);
			if (ret == 0)
				ret = __wt_conn_btree_sync_and_close(session);

			/*
			 * If the close succeeded, drop any locks it acquired.
			 * If there was a failure, this function will fail and
			 * the whole transaction will be rolled back.
			 */
			if (ret == 0)
				ret = __wt_meta_track_sub_off(session);
		}

		if (!WT_META_TRACKING(session))
			WT_TRET(__wt_session_release_btree(session));

		WT_ERR(ret);
	}

err:	session->dhandle = NULL;
	return (ret);
}

/*
 * __wt_conn_dhandle_discard_single --
 *	Close/discard a single data handle.
 */
int
__wt_conn_dhandle_discard_single(
    WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, int final)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *save_dhandle;
	WT_DECL_RET;

	conn = S2C(session);

	save_dhandle = session->dhandle;
	session->dhandle = dhandle;

	/*
	 * We're called from the periodic sweep function and the final close;
	 * the former wants to continue if the handle is suddenly found to be
	 * busy, the latter wants to shut things down.
	 */
	if (F_ISSET(dhandle, WT_DHANDLE_OPEN)) {
		if (!final)
			WT_ERR(EBUSY);
		WT_ERR(__wt_conn_btree_sync_and_close(session));
	}

	/* 
	 * Get the schema lock (required to remove entries from the data handle
	 * list), get the dhandle lock to block the eviction server from walking
	 * the list.
	 */
	F_SET(session, WT_SESSION_SCHEMA_LOCKED);
	__wt_spin_lock(session, &conn->schema_lock);
	__wt_spin_lock(session, &conn->dhandle_lock);

	/*
	 * Check if the handle was reacquired by a session while we waited;
	 * this should only happen when called from the periodic sweep code, of
	 * course.
	 */
	if (!final && dhandle->session_ref != 0)
		ret = EBUSY;
	else
		SLIST_REMOVE(&conn->dhlh, dhandle, __wt_data_handle, l);

	__wt_spin_unlock(session, &conn->dhandle_lock);
	__wt_spin_unlock(session, &conn->schema_lock);
	F_CLR(session, WT_SESSION_SCHEMA_LOCKED);

	/*
	 * After successfully removing the handle, clean it up.
	 */
	if (ret == 0) {
		WT_TRET(__wt_rwlock_destroy(session, &dhandle->rwlock));
		__wt_free(session, dhandle->name);
		__wt_free(session, dhandle->checkpoint);
		__conn_btree_config_clear(session);
		__wt_free(session, dhandle->handle);
		__wt_spin_destroy(session, &dhandle->close_lock);
		__wt_overwrite_and_free(session, dhandle);

		WT_CLEAR_BTREE_IN_SESSION(session);
	}

err:	session->dhandle = save_dhandle;
	WT_ASSERT(session, !final || ret == 0);
	return (ret);
}

/*
 * __wt_conn_dhandle_discard --
 *	Close/discard all data handles.
 */
int
__wt_conn_dhandle_discard(WT_CONNECTION_IMPL *conn)
{
	WT_DATA_HANDLE *dhandle;
	WT_DATA_HANDLE_CACHE *dhandle_cache;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	session = conn->default_session;

	/*
	 * Close open data handles: first, everything but the metadata file
	 * (as closing a normal file may open and write the metadata file),
	 * then the metadata file.  This function isn't called often, and I
	 * don't want to "know" anything about the metadata file's position on
	 * the list, so we do it the hard way.
	 */
restart:
	SLIST_FOREACH(dhandle, &conn->dhlh, l) {
		if (WT_IS_METADATA(dhandle))
			continue;

		WT_TRET(__wt_conn_dhandle_discard_single(session, dhandle, 1));
		goto restart;
	}

	/*
	 * Closing the files may have resulted in entries on our session's list
	 * of open data handles, specifically, we added the metadata file if
	 * any of the files were dirty.  Clean up that list before we shut down
	 * the metadata entry, for good.
	 */
	while ((dhandle_cache = SLIST_FIRST(&session->dhandles)) != NULL)
		__wt_session_discard_btree(session, dhandle_cache);

	/* Close the metadata file handle. */
	while ((dhandle = SLIST_FIRST(&conn->dhlh)) != NULL)
		WT_TRET(__wt_conn_dhandle_discard_single(session, dhandle, 1));

	return (ret);
}
