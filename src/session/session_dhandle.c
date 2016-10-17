/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __session_add_dhandle --
 *	Add a handle to the session's cache.
 */
static int
__session_add_dhandle(WT_SESSION_IMPL *session)
{
	WT_DATA_HANDLE_CACHE *dhandle_cache;
	uint64_t bucket;

	/* Allocate a handle cache entry. */
	WT_RET(__wt_calloc_one(session, &dhandle_cache));

	dhandle_cache->dhandle = session->dhandle;

	bucket = dhandle_cache->dhandle->name_hash % WT_HASH_ARRAY_SIZE;
	TAILQ_INSERT_HEAD(&session->dhandles, dhandle_cache, q);
	TAILQ_INSERT_HEAD(&session->dhhash[bucket], dhandle_cache, hashq);

	return (0);
}

/*
 * __session_discard_dhandle --
 *	Remove a data handle from the session cache.
 */
static void
__session_discard_dhandle(
    WT_SESSION_IMPL *session, WT_DATA_HANDLE_CACHE *dhandle_cache)
{
	uint64_t bucket;

	bucket = dhandle_cache->dhandle->name_hash % WT_HASH_ARRAY_SIZE;
	TAILQ_REMOVE(&session->dhandles, dhandle_cache, q);
	TAILQ_REMOVE(&session->dhhash[bucket], dhandle_cache, hashq);

	(void)__wt_atomic_sub32(&dhandle_cache->dhandle->session_ref, 1);

	__wt_overwrite_and_free(session, dhandle_cache);
}

/*
 * __session_find_dhandle --
 *	Search for a data handle in the session cache.
 */
static void
__session_find_dhandle(WT_SESSION_IMPL *session,
    const char *uri, const char *checkpoint,
    WT_DATA_HANDLE_CACHE **dhandle_cachep)
{
	WT_DATA_HANDLE *dhandle;
	WT_DATA_HANDLE_CACHE *dhandle_cache;
	uint64_t bucket;

	dhandle = NULL;

	bucket = __wt_hash_city64(uri, strlen(uri)) % WT_HASH_ARRAY_SIZE;
retry:	TAILQ_FOREACH(dhandle_cache, &session->dhhash[bucket], hashq) {
		dhandle = dhandle_cache->dhandle;
		if (WT_DHANDLE_INACTIVE(dhandle) &&
		    !WT_IS_METADATA(session, dhandle)) {
			__session_discard_dhandle(session, dhandle_cache);
			/* We deleted our entry, retry from the start. */
			goto retry;
		}

		if (strcmp(uri, dhandle->name) != 0)
			continue;
		if (checkpoint == NULL && dhandle->checkpoint == NULL)
			break;
		if (checkpoint != NULL && dhandle->checkpoint != NULL &&
		    strcmp(checkpoint, dhandle->checkpoint) == 0)
			break;
	}

	*dhandle_cachep = dhandle_cache;
}

/*
 * __wt_session_lock_dhandle --
 *	Return when the current data handle is either (a) open with the
 *	requested lock mode; or (b) closed and write locked.  If exclusive
 *	access is requested and cannot be granted immediately because the
 *	handle is in use, fail with EBUSY.
 *
 *	Here is a brief summary of how different operations synchronize using
 *	either the schema lock, handle locks or handle flags:
 *
 *	open -- one thread gets the handle exclusive, reverts to a shared
 *		handle lock once the handle is open;
 *	bulk load -- sets bulk and exclusive;
 *	salvage, truncate, update, verify -- hold the schema lock,
 *		get the handle exclusive, set a "special" flag;
 *	sweep -- gets a write lock on the handle, doesn't set exclusive
 *
 *	The principle is that some application operations can cause other
 *	application operations to fail (so attempting to open a cursor on a
 *	file while it is being bulk-loaded will fail), but internal or
 *	database-wide operations should not prevent application-initiated
 *	operations.  For example, attempting to verify a file should not fail
 *	because the sweep server happens to be in the process of closing that
 *	file.
 */
int
__wt_session_lock_dhandle(
    WT_SESSION_IMPL *session, uint32_t flags, bool *is_deadp)
{
	WT_BTREE *btree;
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	bool is_open, lock_busy, want_exclusive;

	*is_deadp = 0;

	dhandle = session->dhandle;
	btree = dhandle->handle;
	lock_busy = false;
	want_exclusive = LF_ISSET(WT_DHANDLE_EXCLUSIVE);

	/*
	 * If this session already has exclusive access to the handle, there is
	 * no point trying to lock it again.
	 *
	 * This should only happen if a checkpoint handle is locked multiple
	 * times during a checkpoint operation, or the handle is already open
	 * without any special flags.  In particular, it must fail if
	 * attempting to checkpoint a handle opened for a bulk load, even in
	 * the same session.
	 */
	if (dhandle->excl_session == session) {
		if (!LF_ISSET(WT_DHANDLE_LOCK_ONLY) &&
		    (!F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
		    F_ISSET(btree, WT_BTREE_SPECIAL_FLAGS)))
			return (EBUSY);
		++dhandle->excl_ref;
		return (0);
	}

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
		/* If the handle is dead, give up. */
		if (F_ISSET(dhandle, WT_DHANDLE_DEAD)) {
			*is_deadp = 1;
			return (0);
		}

		/*
		 * If the handle is already open for a special operation,
		 * give up.
		 */
		if (F_ISSET(btree, WT_BTREE_SPECIAL_FLAGS))
			return (EBUSY);

		/*
		 * If the handle is open, get a read lock and recheck.
		 *
		 * Wait for a read lock if we want exclusive access and failed
		 * to get it: the sweep server may be closing this handle, and
		 * we need to wait for it to release its lock.  If we want
		 * exclusive access and find the handle open once we get the
		 * read lock, give up: some other thread has it locked for real.
		 */
		if (F_ISSET(dhandle, WT_DHANDLE_OPEN) &&
		    (!want_exclusive || lock_busy)) {
			__wt_readlock(session, dhandle->rwlock);
			if (F_ISSET(dhandle, WT_DHANDLE_DEAD)) {
				*is_deadp = 1;
				__wt_readunlock(session, dhandle->rwlock);
				return (0);
			}

			is_open = F_ISSET(dhandle, WT_DHANDLE_OPEN);
			if (is_open && !want_exclusive)
				return (0);
			__wt_readunlock(session, dhandle->rwlock);
		} else
			is_open = false;

		/*
		 * It isn't open or we want it exclusive: try to get an
		 * exclusive lock.  There is some subtlety here: if we race
		 * with another thread that successfully opens the file, we
		 * don't want to block waiting to get exclusive access.
		 */
		if ((ret = __wt_try_writelock(session, dhandle->rwlock)) == 0) {
			if (F_ISSET(dhandle, WT_DHANDLE_DEAD)) {
				*is_deadp = 1;
				__wt_writeunlock(session, dhandle->rwlock);
				return (0);
			}

			/*
			 * If it was opened while we waited, drop the write
			 * lock and get a read lock instead.
			 */
			if (F_ISSET(dhandle, WT_DHANDLE_OPEN) &&
			    !want_exclusive) {
				lock_busy = false;
				__wt_writeunlock(session, dhandle->rwlock);
				continue;
			}

			/* We have an exclusive lock, we're done. */
			F_SET(dhandle, WT_DHANDLE_EXCLUSIVE);
			WT_ASSERT(session,
			    dhandle->excl_session == NULL &&
			    dhandle->excl_ref == 0);
			dhandle->excl_session = session;
			dhandle->excl_ref = 1;
			WT_ASSERT(session, !F_ISSET(dhandle, WT_DHANDLE_DEAD));
			return (0);
		}
		if (ret != EBUSY || (is_open && want_exclusive))
			return (ret);
		lock_busy = true;

		/* Give other threads a chance to make progress. */
		__wt_yield();
	}
}

/*
 * __wt_session_release_btree --
 *	Unlock a btree handle.
 */
int
__wt_session_release_btree(WT_SESSION_IMPL *session)
{
	WT_BTREE *btree;
	WT_DATA_HANDLE *dhandle;
	WT_DATA_HANDLE_CACHE *dhandle_cache;
	WT_DECL_RET;
	bool locked, write_locked;

	btree = S2BT(session);
	dhandle = session->dhandle;
	write_locked = F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE);
	locked = true;

	/*
	 * If we had special flags set, close the handle so that future access
	 * can get a handle without special flags.
	 */
	if (F_ISSET(dhandle, WT_DHANDLE_DISCARD | WT_DHANDLE_DISCARD_FORCE)) {
		__session_find_dhandle(session,
		    dhandle->name, dhandle->checkpoint, &dhandle_cache);
		if (dhandle_cache != NULL)
			__session_discard_dhandle(session, dhandle_cache);
	}

	if (F_ISSET(dhandle, WT_DHANDLE_DISCARD_FORCE)) {
		ret = __wt_conn_btree_sync_and_close(session, false, true);
		F_CLR(dhandle, WT_DHANDLE_DISCARD_FORCE);
	} else if (F_ISSET(dhandle, WT_DHANDLE_DISCARD) ||
	    F_ISSET(btree, WT_BTREE_SPECIAL_FLAGS)) {
		WT_ASSERT(session, F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE));
		ret = __wt_conn_btree_sync_and_close(session, false, false);
		F_CLR(dhandle, WT_DHANDLE_DISCARD);
	}

	if (session == dhandle->excl_session) {
		if (--dhandle->excl_ref == 0)
			dhandle->excl_session = NULL;
		else
			locked = false;
	}
	if (locked) {
		if (write_locked) {
			F_CLR(dhandle, WT_DHANDLE_EXCLUSIVE);
			__wt_writeunlock(session, dhandle->rwlock);
		} else
			__wt_readunlock(session, dhandle->rwlock);
	}

	session->dhandle = NULL;
	return (ret);
}

/*
 * __wt_session_get_btree_ckpt --
 *	Check the configuration strings for a checkpoint name, get a btree
 * handle for the given name, set session->dhandle.
 */
int
__wt_session_get_btree_ckpt(WT_SESSION_IMPL *session,
    const char *uri, const char *cfg[], uint32_t flags)
{
	WT_CONFIG_ITEM cval;
	WT_DECL_RET;
	bool last_ckpt;
	const char *checkpoint;

	last_ckpt = false;
	checkpoint = NULL;

	/*
	 * This function exists to handle checkpoint configuration.  Callers
	 * that never open a checkpoint call the underlying function directly.
	 */
	WT_RET_NOTFOUND_OK(
	    __wt_config_gets_def(session, cfg, "checkpoint", 0, &cval));
	if (cval.len != 0) {
		/*
		 * The internal checkpoint name is special, find the last
		 * unnamed checkpoint of the object.
		 */
		if (WT_STRING_MATCH(WT_CHECKPOINT, cval.str, cval.len)) {
			last_ckpt = true;
retry:			WT_RET(__wt_meta_checkpoint_last_name(
			    session, uri, &checkpoint));
		} else
			WT_RET(__wt_strndup(
			    session, cval.str, cval.len, &checkpoint));
	}

	ret = __wt_session_get_btree(session, uri, checkpoint, cfg, flags);
	__wt_free(session, checkpoint);

	/*
	 * There's a potential race: we get the name of the most recent unnamed
	 * checkpoint, but if it's discarded (or locked so it can be discarded)
	 * by the time we try to open it, we'll fail the open.  Retry in those
	 * cases, a new "last" checkpoint should surface, and we can't return an
	 * error, the application will be justifiably upset if we can't open the
	 * last checkpoint instance of an object.
	 *
	 * The check against WT_NOTFOUND is correct: if there was no checkpoint
	 * for the object (that is, the object has never been in a checkpoint),
	 * we returned immediately after the call to search for that name.
	 */
	if (last_ckpt && (ret == WT_NOTFOUND || ret == EBUSY))
		goto retry;
	return (ret);
}

/*
 * __wt_session_close_cache --
 *	Close any cached handles in a session.
 */
void
__wt_session_close_cache(WT_SESSION_IMPL *session)
{
	WT_DATA_HANDLE_CACHE *dhandle_cache;

	while ((dhandle_cache = TAILQ_FIRST(&session->dhandles)) != NULL)
		__session_discard_dhandle(session, dhandle_cache);
}

/*
 * __session_dhandle_sweep --
 *	Discard any session dhandles that are not open.
 */
static void
__session_dhandle_sweep(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DATA_HANDLE *dhandle;
	WT_DATA_HANDLE_CACHE *dhandle_cache, *dhandle_cache_next;
	time_t now;

	conn = S2C(session);

	/*
	 * Periodically sweep for dead handles; if we've swept recently, don't
	 * do it again.
	 */
	__wt_seconds(session, &now);
	if (difftime(now, session->last_sweep) < conn->sweep_interval)
		return;
	session->last_sweep = now;

	WT_STAT_CONN_INCR(session, dh_session_sweeps);

	dhandle_cache = TAILQ_FIRST(&session->dhandles);
	while (dhandle_cache != NULL) {
		dhandle_cache_next = TAILQ_NEXT(dhandle_cache, q);
		dhandle = dhandle_cache->dhandle;
		if (dhandle != session->dhandle &&
		    dhandle->session_inuse == 0 &&
		    (WT_DHANDLE_INACTIVE(dhandle) ||
		    (dhandle->timeofdeath != 0 &&
		    difftime(now, dhandle->timeofdeath) >
		    conn->sweep_idle_time))) {
			WT_STAT_CONN_INCR(session, dh_session_handles);
			WT_ASSERT(session, !WT_IS_METADATA(session, dhandle));
			__session_discard_dhandle(session, dhandle_cache);
		}
		dhandle_cache = dhandle_cache_next;
	}
}

/*
 * __session_find_shared_dhandle --
 *	Search for a data handle in the connection and add it to a session's
 *	cache.  Since the data handle isn't locked, this must be called holding
 *	the handle list lock, and we must increment the handle's reference
 *	count before releasing it.
 */
static int
__session_find_shared_dhandle(
    WT_SESSION_IMPL *session, const char *uri, const char *checkpoint)
{
	WT_RET(__wt_conn_dhandle_find(session, uri, checkpoint));
	(void)__wt_atomic_add32(&session->dhandle->session_ref, 1);
	return (0);
}

/*
 * __session_get_dhandle --
 *	Search for a data handle, first in the session cache, then in the
 *	connection.
 */
static int
__session_get_dhandle(
    WT_SESSION_IMPL *session, const char *uri, const char *checkpoint)
{
	WT_DATA_HANDLE_CACHE *dhandle_cache;
	WT_DECL_RET;

	__session_find_dhandle(session, uri, checkpoint, &dhandle_cache);
	if (dhandle_cache != NULL) {
		session->dhandle = dhandle_cache->dhandle;
		return (0);
	}

	/* Sweep the handle list to remove any dead handles. */
	__session_dhandle_sweep(session);

	/*
	 * We didn't find a match in the session cache, search the shared
	 * handle list and cache the handle we find.
	 */
	WT_WITH_HANDLE_LIST_LOCK(session,
	    ret = __session_find_shared_dhandle(session, uri, checkpoint));
	WT_RET(ret);

	/*
	 * Fixup the reference count on failure (we incremented the reference
	 * count while holding the handle-list lock).
	 */
	if ((ret = __session_add_dhandle(session)) != 0)
		(void)__wt_atomic_sub32(&session->dhandle->session_ref, 1);

	return (ret);
}

/*
 * __wt_session_get_btree --
 *	Get a btree handle for the given name, set session->dhandle.
 */
int
__wt_session_get_btree(WT_SESSION_IMPL *session,
    const char *uri, const char *checkpoint, const char *cfg[], uint32_t flags)
{
	WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	bool is_dead;

	WT_ASSERT(session, !F_ISSET(session, WT_SESSION_NO_DATA_HANDLES));

	for (;;) {
		WT_RET(__session_get_dhandle(session, uri, checkpoint));
		dhandle = session->dhandle;

		/* Try to lock the handle. */
		WT_RET(__wt_session_lock_dhandle(session, flags, &is_dead));
		if (is_dead)
			continue;

		/* If the handle is open in the mode we want, we're done. */
		if (LF_ISSET(WT_DHANDLE_LOCK_ONLY) ||
		    (F_ISSET(dhandle, WT_DHANDLE_OPEN) &&
		    !LF_ISSET(WT_BTREE_SPECIAL_FLAGS)))
			break;

		WT_ASSERT(session, F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE));

		/*
		 * For now, we need the schema lock and handle list locks to
		 * open a file for real.
		 *
		 * Code needing exclusive access (such as drop or verify)
		 * assumes that it can close all open handles, then open an
		 * exclusive handle on the active tree and no other threads can
		 * reopen handles in the meantime.  A combination of the schema
		 * and handle list locks are used to enforce this.
		 */
		if (!F_ISSET(session, WT_SESSION_LOCKED_SCHEMA) ||
		    !F_ISSET(session, WT_SESSION_LOCKED_HANDLE_LIST)) {
			dhandle->excl_session = NULL;
			dhandle->excl_ref = 0;
			F_CLR(dhandle, WT_DHANDLE_EXCLUSIVE);
			__wt_writeunlock(session, dhandle->rwlock);

			WT_WITH_SCHEMA_LOCK(session, ret,
			    WT_WITH_HANDLE_LIST_LOCK(session,
				ret = __wt_session_get_btree(
				session, uri, checkpoint, cfg, flags)));

			return (ret);
		}

		/* Open the handle. */
		if ((ret = __wt_conn_btree_open(session, cfg, flags)) == 0 &&
		    LF_ISSET(WT_DHANDLE_EXCLUSIVE))
			break;

		/*
		 * If we got the handle exclusive to open it but only want
		 * ordinary access, drop our lock and retry the open.
		 */
		dhandle->excl_session = NULL;
		dhandle->excl_ref = 0;
		F_CLR(dhandle, WT_DHANDLE_EXCLUSIVE);
		__wt_writeunlock(session, dhandle->rwlock);
		WT_RET(ret);
	}

	WT_ASSERT(session, !F_ISSET(dhandle, WT_DHANDLE_DEAD));
	WT_ASSERT(session, LF_ISSET(WT_DHANDLE_LOCK_ONLY) ||
	    F_ISSET(dhandle, WT_DHANDLE_OPEN));

	WT_ASSERT(session, LF_ISSET(WT_DHANDLE_EXCLUSIVE) ==
	    F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE) || dhandle->excl_ref > 1);

	return (0);
}

/*
 * __wt_session_lock_checkpoint --
 *	Lock the btree handle for the given checkpoint name.
 */
int
__wt_session_lock_checkpoint(WT_SESSION_IMPL *session, const char *checkpoint)
{
	WT_DATA_HANDLE *dhandle, *saved_dhandle;
	WT_DECL_RET;

	WT_ASSERT(session, WT_META_TRACKING(session));
	saved_dhandle = session->dhandle;

	/*
	 * Get the checkpoint handle exclusive, so no one else can access it
	 * while we are creating the new checkpoint.
	 */
	WT_ERR(__wt_session_get_btree(session, saved_dhandle->name,
	    checkpoint, NULL, WT_DHANDLE_EXCLUSIVE | WT_DHANDLE_LOCK_ONLY));

	/*
	 * Flush any pages in this checkpoint from the cache (we are about to
	 * re-write the checkpoint which will mean cached pages no longer have
	 * valid contents).  This is especially noticeable with memory mapped
	 * files, since changes to the underlying file are visible to the in
	 * memory pages.
	 */
	WT_ERR(__wt_cache_op(session, WT_SYNC_DISCARD));

	/*
	 * We lock checkpoint handles that we are overwriting, so the handle
	 * must be closed when we release it.
	 */
	dhandle = session->dhandle;
	F_SET(dhandle, WT_DHANDLE_DISCARD);

	WT_ERR(__wt_meta_track_handle_lock(session, false));

	/* Restore the original btree in the session. */
err:	session->dhandle = saved_dhandle;

	return (ret);
}
