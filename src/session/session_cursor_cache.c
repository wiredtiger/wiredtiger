/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_session_not_idle --
 *	Session method is restricted when session is idle or cached.
 */
int
__wt_session_not_idle(WT_SESSION_IMPL *session)
{
	WT_RET_MSG(session, ENOTSUP, "Session is idle or cached");
}

/*
 * __wt_session_cursor_cache_lock --
 *	Lock the session so cached cursors can only be closed by the
 *	calling thread.
 */
void
__wt_session_cursor_cache_lock(WT_SESSION_IMPL *session,
    WT_SESSION_IMPL *locked_session)
{
	/* We are already holding an ownership lock for our own session. */
	if (session != locked_session)
		__wt_writelock(session, &locked_session->cursor_cache_lock);
}

/*
 * __wt_session_cursor_cache_try_lock --
 *	Try to lock the session so cached cursors can only be closed by the
 *	calling thread.
 */
int
__wt_session_cursor_cache_try_lock(WT_SESSION_IMPL *session,
    WT_SESSION_IMPL *locked_session)
{
	/* We are already holding an ownership lock for our own session. */
	if (session == locked_session)
		return (0);
	else
		return (__wt_try_writelock(session,
		    &locked_session->cursor_cache_lock));
}

/*
 * __wt_session_cursor_cache_unlock --
 *	Unlock the session for closing cached cursors.
 */
void
__wt_session_cursor_cache_unlock(WT_SESSION_IMPL *session,
    WT_SESSION_IMPL *locked_session)
{
	/* We are already holding an ownership lock for our own session. */
	if (session != locked_session)
		__wt_writeunlock(session, &locked_session->cursor_cache_lock);
}

/*
 * __cursor_cache_cleanup --
 *	Close cached cursors that are no longer globally referenced.
 *	Called while holding the session's cursor cache lock.
 */
static int
__cursor_cache_cleanup(WT_SESSION_IMPL *session,
    WT_SESSION_IMPL *target_session, uint64_t *closed_cnt)
{
	WT_BITMAP *hot;
	WT_CURSOR *cursor;
	bool closed;

	if (target_session->ncursors_cached == 0)
		return (0);

	hot = &S2C(session)->dhandle_hot;
	closed = false;
	F_CLR(target_session, WT_SESSION_CACHE_CURSORS);
again:	TAILQ_FOREACH(cursor, &target_session->cursors, q) {
		if (F_ISSET(cursor, WT_CURSTD_CACHED) &&
		    __wt_bitmap_test_all_bitmap(session, hot,
		    WT_CURSOR_DS_BITS(cursor))) {
			F_CLR(cursor, WT_CURSTD_CACHED);
			--target_session->ncursors_cached;
			closed = true;
			WT_RET(cursor->close(cursor));
			*closed_cnt += 1;

			/*
			 * This close may have closed subordinate
			 * cursors in the list.  There's no safe
			 * way to continue to traverse using the standard
			 * macros. (TODO: we could save the *previous* entry
			 * and back up, a stutter step).
			 */
			goto again;
		}
	}
	F_SET(target_session, WT_SESSION_CACHE_CURSORS);
	if (closed) {
		/*
		 * Rebuild the bitmap that collects what is in use.
		 * We cannot clear bits as we are closing because
		 * there may be multiple cursors that reference
		 * the same bits.
		 */
		__wt_bitmap_clear_all(&target_session->dhandle_inuse);
		TAILQ_FOREACH(cursor, &target_session->cursors, q) {
			WT_RET(__wt_bitmap_or_bitmap(
			    session, &session->dhandle_inuse,
			    WT_CURSOR_DS_BITS(cursor)));
		}
	}
	return (0);
}

/*
 * __wt_session_cursor_cache_open --
 *	Open a matching cursor from the cache.
 */
int
__wt_session_cursor_cache_open(WT_SESSION_IMPL *session, const char *uri,
    bool append, bool overwrite, WT_CURSOR **cursorp)
{
	WT_CURSOR *cursor;
	uint32_t flags;

	if (session->ncursors_cached == 0)
		return (WT_NOTFOUND);

	flags = 0;
	if (append)
		LF_SET(WT_CURSTD_APPEND);
	if (overwrite)
		LF_SET(WT_CURSTD_OVERWRITE);

	/*
	 * Walk through all cursors, if there is a cached
	 * cursor that matches uri and configuration, use it.
	 */
	TAILQ_FOREACH(cursor, &session->cursors, q) {
		if (F_ISSET(cursor, WT_CURSTD_CACHED) && cursor->uri != NULL &&
		    WT_STREQ(cursor->uri, uri) &&
		    F_MASK(cursor, WT_CURSTD_APPEND | WT_CURSTD_OVERWRITE) ==
		    flags) {
			F_CLR(cursor, WT_CURSTD_CACHED);
			WT_RET(__wt_bitmap_or_bitmap(session,
			    &session->dhandle_inuse,
			    WT_CURSOR_DS_BITS(cursor)));
			--session->ncursors_cached;
			*cursorp = cursor;
			return (0);
		}
	}
	return (WT_NOTFOUND);
}

/*
 * __wt_session_cursor_cache_server --
 *	The cursor cache server's entry point to collect usage
 *	information and close old cached cursors.
 */
int
__wt_session_cursor_cache_server(WT_SESSION_IMPL *session,
    WT_SESSION_IMPL *target_session, bool usage, bool close,
    WT_BITMAP *remove_reference, uint64_t *closed_cnt)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	time_t now;
	uint64_t ncached;
	bool locked;

	if (!usage) {
		WT_ORDERED_READ(ncached, target_session->ncursors_cached);
		if (ncached == 0)
			return (0);
	}

	conn = S2C(session);
	locked = false;
	if ((ret = __wt_session_cursor_cache_try_lock(
	    session, target_session)) == 0) {
		locked = true;
		/*
		 * We're done if this session isn't using any data handles
		 * matching a specific request to remove a reference.
		 */
		if (remove_reference != NULL && !__wt_bitmap_test_bitmap(
		    session, &target_session->dhandle_inuse, remove_reference))
			goto done;

		if (usage)
			__wt_bitmap_or_bitmap(
			    session, &conn->dhandle_history[0],
			    &target_session->dhandle_inuse);
		__wt_seconds(session, &now);
		if (close || now > session->last_cursor_cache_close) {
			WT_ERR(__cursor_cache_cleanup(session,
			    target_session, closed_cnt));
			session->last_cursor_cache_close = now;
		}
	}
	WT_ERR_BUSY_OK(ret);

done:
err:	if (locked)
		__wt_session_cursor_cache_unlock(session, target_session);
	return (ret);
}
