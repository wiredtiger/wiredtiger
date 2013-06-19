/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_session_add_btree --
 *	Add a handle to the session's cache.
 */
int
__wt_session_add_btree(
    WT_SESSION_IMPL *session, WT_DATA_HANDLE_CACHE **dhandle_cachep)
{
	WT_DATA_HANDLE_CACHE *dhandle_cache;

	WT_RET(__wt_calloc_def(session, 1, &dhandle_cache));
	dhandle_cache->dhandle = session->dhandle;

	TAILQ_INSERT_HEAD(&session->dhandles, dhandle_cache, q);

	if (dhandle_cachep != NULL)
		*dhandle_cachep = dhandle_cache;

	return (0);
}

/*
 * __wt_session_lock_btree --
 *	Lock a btree handle.
 */
int
__wt_session_lock_btree(WT_SESSION_IMPL *session, uint32_t flags)
{
	WT_BTREE *btree;
	WT_DATA_HANDLE *dhandle;
	uint32_t special_flags;

	btree = S2BT(session);
	dhandle = session->dhandle;

	/*
	 * Special operation flags will cause the handle to be reopened.
	 * For example, a handle opened with WT_BTREE_BULK cannot use the same
	 * internal data structures as a handle opened for ordinary access.
	 */
	special_flags = LF_ISSET(WT_BTREE_SPECIAL_FLAGS);
	WT_ASSERT(session,
	    special_flags == 0 || LF_ISSET(WT_DHANDLE_EXCLUSIVE));

	if (LF_ISSET(WT_DHANDLE_EXCLUSIVE)) {
		/*
		 * Try to get an exclusive handle lock and fail immediately if
		 * it's unavailable.  We don't expect exclusive operations on
		 * trees to be mixed with ordinary cursor access, but if there
		 * is a use case in the future, we could make blocking here
		 * configurable.
		 *
		 * Special flags will cause the handle to be reopened, which
		 * will get the necessary lock, so don't bother here.
		 */
		if (LF_ISSET(WT_DHANDLE_LOCK_ONLY) || special_flags == 0) {
			WT_RET(__wt_try_writelock(session, dhandle->rwlock));
			F_SET(dhandle, WT_DHANDLE_EXCLUSIVE);
		}
	} else if (F_ISSET(btree, WT_BTREE_SPECIAL_FLAGS))
		return (EBUSY);
	else
		WT_RET(__wt_readlock(session, dhandle->rwlock));

	/*
	 * At this point, we have the requested lock -- if that is all that was
	 * required, we're done.  Otherwise, check that the handle is open and
	 * that no special flags are required.
	 */
	if (LF_ISSET(WT_DHANDLE_LOCK_ONLY) ||
	    (F_ISSET(dhandle, WT_DHANDLE_OPEN) && special_flags == 0))
		return (0);

	/*
	 * The handle needs to be opened.  If we locked the handle above,
	 * unlock it before returning.
	 */
	if (!LF_ISSET(WT_DHANDLE_EXCLUSIVE) || special_flags == 0) {
		F_CLR(dhandle, WT_DHANDLE_EXCLUSIVE);
		WT_RET(__wt_rwunlock(session, dhandle->rwlock));
	}

	/* Treat an unopened handle just like a non-existent handle. */
	return (WT_NOTFOUND);
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
	WT_DECL_RET;

	btree = S2BT(session);
	dhandle = session->dhandle;

	if (F_ISSET(dhandle, WT_DHANDLE_DISCARD_CLOSE)) {
		/*
		 * If configured to discard on last close, attempt to trade our
		 * read lock for an exclusive lock. If that succeeds, setup for
		 * discard. It is expected that the exclusive lock will fail
		 * sometimes since the handle may still be in use: in that case
		 * we've already unlocked, so we're done.
		 */
		WT_ERR(__wt_rwunlock(session, dhandle->rwlock));
		ret = __wt_try_writelock(session, dhandle->rwlock);
		if (ret != 0) {
			if (ret == EBUSY)
				ret = 0;
			goto err;
		}
		F_CLR(dhandle, WT_DHANDLE_DISCARD_CLOSE);
		F_SET(dhandle, WT_DHANDLE_DISCARD | WT_DHANDLE_EXCLUSIVE);
	}

	/*
	 * If we had special flags set, close the handle so that future access
	 * can get a handle without special flags.
	 */
	if (F_ISSET(dhandle, WT_DHANDLE_DISCARD) ||
	    F_ISSET(btree, WT_BTREE_SPECIAL_FLAGS)) {
		WT_ASSERT(session, F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE));
		F_CLR(dhandle, WT_DHANDLE_DISCARD);

		WT_TRET(__wt_conn_btree_sync_and_close(session));
	}

	if (F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE))
		F_CLR(dhandle, WT_DHANDLE_EXCLUSIVE);

	WT_TRET(__wt_rwunlock(session, dhandle->rwlock));

err:	session->dhandle = NULL;
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
	int last_ckpt;
	const char *checkpoint;

	last_ckpt = 0;
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
			last_ckpt = 1;
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
 * __wt_session_get_btree --
 *	Get a btree handle for the given name, set session->dhandle.
 */
int
__wt_session_get_btree(WT_SESSION_IMPL *session,
    const char *uri, const char *checkpoint, const char *cfg[], uint32_t flags)
{
	WT_DATA_HANDLE *dhandle;
	WT_DATA_HANDLE_CACHE *dhandle_cache;
	WT_DECL_RET;
	uint64_t hash;
	int candidate;

	dhandle = NULL;
	candidate = 0;

	hash = __wt_hash_city64(uri, strlen(uri));
	TAILQ_FOREACH(dhandle_cache, &session->dhandles, q) {
		dhandle = dhandle_cache->dhandle;
		if (hash != dhandle->name_hash ||
		    strcmp(uri, dhandle->name) != 0)
			continue;
		if (checkpoint == NULL && dhandle->checkpoint == NULL)
			break;
		if (checkpoint != NULL && dhandle->checkpoint != NULL &&
		    strcmp(checkpoint, dhandle->checkpoint) == 0)
			break;
	}

	if (dhandle_cache != NULL) {
		candidate = 1;
		session->dhandle = dhandle;

		/*
		 * Try to lock the file; if we succeed, our "exclusive"
		 * state must match.
		 */
		ret = __wt_session_lock_btree(session, flags);
		if (ret == WT_NOTFOUND)
			dhandle_cache = NULL;
		else
			WT_RET(ret);
	}

	if (dhandle_cache == NULL) {
		/*
		 * If we don't already hold the schema lock, get it now so that
		 * we can find and/or open the handle.
		 */
		WT_WITH_SCHEMA_LOCK_OPT(session, ret =
		    __wt_conn_btree_get(session, uri, checkpoint, cfg, flags));
		WT_RET(ret);

		if (!candidate)
			WT_RET(__wt_session_add_btree(session, NULL));
		WT_ASSERT(session, LF_ISSET(WT_DHANDLE_LOCK_ONLY) ||
		    F_ISSET(session->dhandle, WT_DHANDLE_OPEN));
	}

	WT_ASSERT(session, LF_ISSET(WT_DHANDLE_EXCLUSIVE) ==
	    F_ISSET(session->dhandle, WT_DHANDLE_EXCLUSIVE));
	F_SET(session->dhandle, LF_ISSET(WT_DHANDLE_DISCARD_CLOSE));

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

	saved_dhandle = session->dhandle;

	WT_ERR(__wt_session_get_btree(session, saved_dhandle->name,
	    checkpoint, NULL, WT_DHANDLE_EXCLUSIVE | WT_DHANDLE_LOCK_ONLY));

	/*
	 * We lock checkpoint handles that we are overwriting, so the handle
	 * must be closed when we release it.
	 */
	dhandle = session->dhandle;
	F_SET(dhandle, WT_DHANDLE_DISCARD);

	WT_ASSERT(session, WT_META_TRACKING(session));
	WT_ERR(__wt_meta_track_handle_lock(session, 0));

	/* Restore the original btree in the session. */
err:	session->dhandle = saved_dhandle;

	return (ret);
}

/*
 * __wt_session_discard_btree --
 *	Discard our reference to the btree.
 */
int
__wt_session_discard_btree(
    WT_SESSION_IMPL *session, WT_DATA_HANDLE_CACHE *dhandle_cache)
{
	WT_DATA_HANDLE *saved_dhandle;
	WT_DECL_RET;

	TAILQ_REMOVE(&session->dhandles, dhandle_cache, q);

	saved_dhandle = session->dhandle;
	session->dhandle = dhandle_cache->dhandle;

	__wt_overwrite_and_free(session, dhandle_cache);
	ret = __wt_conn_btree_close(session, 0);

	/* Restore the original handle in the session. */
	session->dhandle = saved_dhandle;
	return (ret);
}
