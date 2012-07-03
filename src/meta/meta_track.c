/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * WT_META_TRACK -- A tracked metadata operation: a non-transactional log,
 * maintained to make it easy to unroll simple metadata and filesystem
 * operations.
 */
typedef struct __wt_meta_track {
	enum {
		WT_ST_EMPTY,		/* Unused slot */
		WT_ST_CHECKPOINT,	/* Complete a checkpoint */
		WT_ST_FILEOP,		/* File operation */
		WT_ST_LOCK,		/* Lock a handle */
		WT_ST_REMOVE,		/* Remove a metadata entry */
		WT_ST_SET		/* Reset a metadata entry */
	} op;
	const char *a, *b;		/* Strings */
	WT_BTREE *btree;		/* Locked handle */
} WT_META_TRACK;

/*
 * __meta_track_next --
 *	Extend the list of operations we're tracking, as necessary, and
 *	optionally return the next slot.
 */
static int
__meta_track_next(WT_SESSION_IMPL *session, WT_META_TRACK **trkp)
{
	size_t offset, sub_off;

	if (!WT_META_TRACKING(session))
		session->meta_track_next = session->meta_track;

	offset = WT_PTRDIFF(session->meta_track_next, session->meta_track);
	sub_off = WT_PTRDIFF(session->meta_track_sub, session->meta_track);
	if (offset == session->meta_track_alloc) {
		WT_RET(__wt_realloc(session, &session->meta_track_alloc,
		    WT_MAX(2 * session->meta_track_alloc,
		    20 * sizeof(WT_META_TRACK)), &session->meta_track));

		/* Maintain positions in the new chunk of memory. */
		session->meta_track_next =
		    (uint8_t *)session->meta_track + offset;
		if (session->meta_track_sub != NULL)
			session->meta_track_sub =
			    (uint8_t *)session->meta_track + sub_off;
	}

	WT_ASSERT(session, session->meta_track_next != NULL);

	if (trkp != NULL) {
		*trkp = session->meta_track_next;
		session->meta_track_next = *trkp + 1;
	}

	return (0);
}

/*
 * __wt_meta_track_discard --
 *	Cleanup metadata tracking when closing a session.
 */
void
__wt_meta_track_discard(WT_SESSION_IMPL *session)
{
	__wt_free(session, session->meta_track);
	session->meta_track_next = NULL;
	session->meta_track_alloc = 0;
}

/*
 * __wt_meta_track_on --
 *	Turn on metadata operation tracking.
 */
int
__wt_meta_track_on(WT_SESSION_IMPL *session)
{
	return (__meta_track_next(session, NULL));
}

static int
__meta_track_apply(WT_SESSION_IMPL *session, WT_META_TRACK *trk, int unroll)
{
	WT_BTREE *saved_btree;
	WT_DECL_RET;
	int tret;

	/*
	 * Unlock handles and complete checkpoints regardless of whether we are
	 * unrolling.
	 */
	if (!unroll && trk->op != WT_ST_CHECKPOINT && trk->op != WT_ST_LOCK)
		goto free;

	switch (trk->op) {
	case WT_ST_EMPTY:	/* Unused slot */
		break;
	case WT_ST_CHECKPOINT:	/* Checkpoint, see above */
		saved_btree = session->btree;
		session->btree = trk->btree;
		if (!unroll)
			WT_TRET(__wt_bm_checkpoint_resolve(session));
		/* Release the checkpoint lock */
		__wt_rwunlock(session, session->btree->ckptlock);
		session->btree = saved_btree;
		break;
	case WT_ST_LOCK:	/* Handle lock, see above */
		saved_btree = session->btree;
		session->btree = trk->btree;
		if (session->created_btree == trk->btree)
			session->created_btree = NULL;
		WT_TRET(__wt_session_release_btree(session));
		session->btree = saved_btree;
		break;
	case WT_ST_FILEOP:	/* File operation */
		/*
		 * For renames, both a and b are set.
		 * For creates, a is NULL.
		 * For removes, b is NULL.
		 */
		if (trk->a != NULL && trk->b != NULL &&
		    (tret = __wt_rename(session,
		    trk->b + strlen("file:"),
		    trk->a + strlen("file:"))) != 0) {
			__wt_err(session, tret,
			    "metadata unroll rename %s to %s",
			    trk->b, trk->a);
			WT_TRET(tret);
		} else if (trk->a == NULL) {
			saved_btree = session->btree;
			if ((session->btree = session->created_btree) != NULL)
				WT_TRET(
				    __wt_conn_btree_sync_and_close(session));
			session->btree = saved_btree;
			if ((tret = __wt_remove(session,
			    trk->b + strlen("file:"))) != 0) {
				__wt_err(session, tret,
				    "metadata unroll create %s",
				    trk->b);
				WT_TRET(tret);
			}
		}
		/*
		 * We can't undo removes yet: that would imply
		 * some kind of temporary rename and remove in
		 * roll forward.
		 */
		break;
	case WT_ST_REMOVE:	/* Remove trk.a */
		if ((tret = __wt_metadata_remove(
		    session, trk->a)) != 0) {
			__wt_err(session, ret,
			    "metadata unroll remove: %s",
			    trk->a);
			WT_TRET(tret);
		}
		break;
	case WT_ST_SET:		/* Set trk.a to trk.b */
		if ((tret = __wt_metadata_update(
		    session, trk->a, trk->b)) != 0) {
			__wt_err(session, ret,
			    "metadata unroll update %s to %s",
			    trk->a, trk->b);
			WT_TRET(tret);
		}
		break;
	WT_ILLEGAL_VALUE(session);
	}

free:	trk->op = WT_ST_EMPTY;
	__wt_free(session, trk->a);
	__wt_free(session, trk->b);
	trk->btree = NULL;

	return (ret);
}

/*
 * __wt_meta_track_off --
 *	Turn off metadata operation tracking, unrolling on error.
 */
int
__wt_meta_track_off(WT_SESSION_IMPL *session, int unroll)
{
	WT_DECL_RET;
	WT_META_TRACK *trk, *trk_orig;

	if (!WT_META_TRACKING(session))
		return (0);

	trk_orig = session->meta_track;
	trk = session->meta_track_next;

	/* Turn off tracking for unroll. */
	session->meta_track_next = session->meta_track_sub = NULL;

	while (--trk >= trk_orig)
		WT_TRET(__meta_track_apply(session, trk, unroll));

	return (ret);
}

/*
 * __wt_meta_track_sub_on --
 *	Start a group of operations that can be committed independent of the
 *	main transaction.
 */
int
__wt_meta_track_sub_on(WT_SESSION_IMPL *session)
{
	WT_ASSERT(session, session->meta_track_sub == NULL);
	session->meta_track_sub = session->meta_track_next;
	return (0);
}

/*
 * __wt_meta_track_sub_off --
 *	Commit a group of operations independent of the main transaction.
 */
int
__wt_meta_track_sub_off(WT_SESSION_IMPL *session)
{
	WT_DECL_RET;
	WT_META_TRACK *trk, *trk_orig;

	if (!WT_META_TRACKING(session) || session->meta_track_sub == NULL)
		return (0);

	trk_orig = session->meta_track_sub;
	trk = session->meta_track_next;

	/* Turn off tracking for unroll. */
	session->meta_track_next = session->meta_track_sub = NULL;

	while (--trk >= trk_orig)
		WT_TRET(__meta_track_apply(session, trk, 0));

	session->meta_track_next = trk_orig;
	return (ret);
}

/*
 * __wt_meta_track_checkpoint --
 *	Track a handle involved in a checkpoint.
 */
int
__wt_meta_track_checkpoint(WT_SESSION_IMPL *session)
{
	WT_META_TRACK *trk;

	WT_ASSERT(session, session->btree != NULL);

	WT_RET(__meta_track_next(session, &trk));

	trk->op = WT_ST_CHECKPOINT;
	trk->btree = session->btree;
	return (0);
}
/*
 * __wt_meta_track_insert --
 *	Track an insert operation.
 */
int
__wt_meta_track_insert(WT_SESSION_IMPL *session, const char *key)
{
	WT_META_TRACK *trk;

	WT_RET(__meta_track_next(session, &trk));

	trk->op = WT_ST_REMOVE;
	WT_RET(__wt_strdup(session, key, &trk->a));

	return (0);
}

/*
 * __wt_meta_track_update --
 *	Track a metadata update operation.
 */
int
__wt_meta_track_update(WT_SESSION_IMPL *session, const char *key)
{
	WT_DECL_RET;
	WT_META_TRACK *trk;

	WT_RET(__meta_track_next(session, &trk));

	trk->op = WT_ST_SET;
	WT_RET(__wt_strdup(session, key, &trk->a));

	/*
	 * If there was a previous value, keep it around -- if not, then this
	 * "update" is really an insert.
	 */
	if ((ret = __wt_metadata_read(session, key, &trk->b)) == WT_NOTFOUND) {
		trk->op = WT_ST_REMOVE;
		ret = 0;
	}
	return (ret);
}

/*
 * __wt_meta_track_fs_rename --
 *	Track a filesystem rename operation.
 */
int
__wt_meta_track_fileop(
    WT_SESSION_IMPL *session, const char *olduri, const char *newuri)
{
	WT_META_TRACK *trk;

	WT_RET(__meta_track_next(session, &trk));

	trk->op = WT_ST_FILEOP;
	if (olduri != NULL)
		WT_RET(__wt_strdup(session, olduri, &trk->a));
	if (newuri != NULL)
		WT_RET(__wt_strdup(session, newuri, &trk->b));
	return (0);
}

/*
 * __wt_meta_track_handle_lock --
 *	Track a locked handle.
 */
int
__wt_meta_track_handle_lock(WT_SESSION_IMPL *session)
{
	WT_META_TRACK *trk;

	WT_ASSERT(session, session->btree != NULL);

	WT_RET(__meta_track_next(session, &trk));

	trk->op = WT_ST_LOCK;
	trk->btree = session->btree;
	return (0);
}
