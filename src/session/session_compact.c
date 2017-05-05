/*-
 * Copyright (c) 2014-2016 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Compaction is the place where the underlying block manager becomes visible
 * in the higher engine btree and API layers.  As there is currently only one
 * block manager, this code is written with it in mind: other block managers
 * may need changes to support compaction, and a smart block manager might need
 * far less support from the engine.
 *
 * First, the default block manager cannot entirely own compaction because it
 * has no way to find a block after it moves other than a request from the
 * btree layer with the new address.  In other words, if internal page X points
 * to leaf page Y, and page Y moves, the address of page Y has to be updated in
 * page X.  Generally, this is solved by building a translation layer in the
 * block manager so internal pages don't require updates to relocate blocks:
 * however, the translation table must be durable, has its own garbage
 * collection issues and might be slower, all of which have their own problems.
 *
 * Second, the btree layer cannot entirely own compaction because page
 * addresses are opaque, it cannot know where a page is in the file from the
 * address cookie.
 *
 * For these reasons, compaction is a cooperative process between the btree
 * layer and the block manager.  The btree layer walks files, and asks the
 * block manager if rewriting a particular block would reduce the file
 * footprint: if writing the page will help, the page is marked dirty so it
 * will eventually be written.  As pages are written, the original page
 * potentially becomes available for reuse and if enough pages at the end of
 * the file are available for reuse, the file can be truncated, and compaction
 * succeeds.
 *
 * However, writing a page is not by itself sufficient to make a page available
 * for reuse.  The original version of the page is still referenced by at least
 * the most recent checkpoint in the file.  To make a page available for reuse,
 * we have to checkpoint the file so we can discard the checkpoint referencing
 * the original version of the block; once no checkpoint references a block, it
 * becomes available for reuse.
 *
 * Compaction is not necessarily possible in WiredTiger, even in a file with
 * lots of available space.  If a block at the end of the file is referenced by
 * a named checkpoint, there is nothing we can do to compact the file, no
 * matter how many times we rewrite the block, the named checkpoint can't be
 * discarded and so the reference count on the original block will never go to
 * zero. What's worse, because the block manager doesn't reference count
 * blocks, it can't easily know this is the case, and so we'll waste a lot of
 * effort trying to compact files that can't be compacted.
 *
 * Finally, compaction checkpoints are database-wide, otherwise we can corrupt
 * file relationships, for example, an index checkpointed by compaction could
 * be out of sync with the primary after a crash.
 *
 * Now, to the actual process.  First, we checkpoint the database: there are
 * potentially many dirty blocks in the cache, and we want to write them out
 * and then discard previous checkpoints so we have as many blocks as possible
 * on the file's "available for reuse" list when we start compaction.
 *
 * Then, we compact the high-level object.
 *
 * Compacting the object is done 10% at a time, that is, we try and move blocks
 * from the last 10% of the file into the beginning of the file (the 10% is
 * hard coded in the block manager).  The reason for this is because we are
 * walking the file in logical order, not block offset order, and we can fail
 * to compact a file if we write the wrong blocks first.
 *
 * For example, imagine a file with 10 blocks in the first 10% of a file, 1,000
 * blocks in the 3rd quartile of the file, and 10 blocks in the last 10% of the
 * file.  If we were to rewrite blocks from more than the last 10% of the file,
 * and found the 1,000 blocks in the 3rd quartile of the file first, we'd copy
 * 10 of them without ever rewriting the blocks from the end of the file which
 * would allow us to compact the file.  So, we compact the last 10% of the
 * file, and if that works, we compact the last 10% of the file again, and so
 * on.  Note the block manager uses a first-fit block selection algorithm
 * during compaction to maximize block movement.
 *
 * After each 10% compaction, we checkpoint two more times (seriously, twice).
 * The second and third checkpoints are because the block manager checkpoints
 * in two steps: blocks made available for reuse during a checkpoint are put on
 * a special checkpoint-available list and only moved to the real available
 * list after the metadata has been updated with the new checkpoint's
 * information.  (Otherwise it is possible to allocate a rewritten block, crash
 * before the metadata is updated, and see corruption.)  For this reason,
 * blocks allocated to write the checkpoint itself cannot be taken from the
 * blocks made available by the checkpoint.
 *
 * To say it another way, the second checkpoint puts the blocks from the end of
 * the file that were made available by compaction onto the checkpoint-available
 * list, but then potentially writes the checkpoint itself at the end of the
 * file, which would prevent any file truncation.  When the metadata is updated
 * for the second checkpoint, the blocks freed by compaction become available
 * for the third checkpoint, so the third checkpoint's blocks are written
 * towards the beginning of the file, and then the file can be truncated.
 */

/*
 * __compact_start --
 *	Start object compaction.
 */
static int
__compact_start(WT_SESSION_IMPL *session)
{
	WT_BM *bm;

	bm = S2BT(session)->bm;
	return (bm->compact_start(bm, session));
}

/*
 * __compact_end --
 *	End object compaction.
 */
static int
__compact_end(WT_SESSION_IMPL *session)
{
	WT_BM *bm;

	bm = S2BT(session)->bm;
	return (bm->compact_end(bm, session));
}

/*
 * __compact_uri_analyze --
 *	Extract information relevant to deciding what work compact needs to
 *	do from a URI that is part of a table schema.
 *	Called via the schema_worker function.
 */
static int
__compact_uri_analyze(WT_SESSION_IMPL *session, const char *uri, bool *skipp)
{
	/*
	 * Add references to schema URI objects to the list of objects to be
	 * compacted.  Skip over LSM trees or we will get false positives on
	 * the "file:" URIs for the chunks.
	 */
	if (WT_PREFIX_MATCH(uri, "lsm:")) {
		session->compact->lsm_count++;
		*skipp = true;
	} else if (WT_PREFIX_MATCH(uri, "file:"))
		session->compact->file_count++;

	return (0);
}

/*
 * __compact_handle_append --
 *	Gather a file handle to be compacted.
 *	Called via the schema_worker function.
 */
static int
__compact_handle_append(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_DECL_RET;

	WT_UNUSED(cfg);

	WT_RET(__wt_session_get_btree(
	    session, session->dhandle->name, NULL, NULL, 0));

	/* Set compact active on the handle. */
	if ((ret = __compact_start(session)) != 0) {
		WT_TRET(__wt_session_release_btree(session));
		return (ret);
	}

	/* Make sure there is space for the next entry. */
	WT_RET(__wt_realloc_def(session, &session->op_handle_allocated,
	    session->op_handle_next + 1, &session->op_handle));

	session->op_handle[session->op_handle_next++] = session->dhandle;
	return (0);
}

/*
 * __wt_session_compact_check_timeout --
 *	Check if the timeout has been exceeded.
 */
int
__wt_session_compact_check_timeout(WT_SESSION_IMPL *session)
{
	struct timespec end;

	if (session->compact->max_time == 0)
		return (0);

	__wt_epoch(session, &end);
	return (session->compact->max_time >
	    WT_TIMEDIFF_SEC(end, session->compact->begin) ? 0 : ETIMEDOUT);
}

/*
 * __compact_checkpoint --
 *     Perform a checkpoint for compaction.
 */
static int
__compact_checkpoint(WT_SESSION_IMPL *session)
{
	WT_TXN_GLOBAL *txn_global;
	WT_DECL_RET;
	uint64_t txn_gen;

	/*
	 * Force compaction checkpoints: we don't want to skip it because the
	 * work we need to have done is done in the underlying block manager.
	 */
	const char *checkpoint_cfg[] = {
	    WT_CONFIG_BASE(session, WT_SESSION_checkpoint), "force=1", NULL };

	/* Checkpoints take a lot of time, check if we've run out. */
	WT_RET(__wt_session_compact_check_timeout(session));

	if ((ret = __wt_txn_checkpoint(session, checkpoint_cfg, false)) == 0)
		return (0);
	WT_RET_BUSY_OK(ret);

	/*
	 * If there's a checkpoint running, wait for it to complete, checking if
	 * we're out of time. If there's no checkpoint running or the checkpoint
	 * generation number changes, the checkpoint blocking us has completed.
	 */
	txn_global = &S2C(session)->txn_global;
	for (txn_gen = __wt_gen(session, WT_GEN_CHECKPOINT);;) {
		WT_READ_BARRIER();
		if (!txn_global->checkpoint_running ||
		    txn_gen != __wt_gen(session, WT_GEN_CHECKPOINT))
			break;

		WT_RET(__wt_session_compact_check_timeout(session));
		__wt_sleep(2, 0);
	}

	return (0);
}

/*
 * __compact_worker --
 *	Function to alternate between checkpoints and compaction calls.
 */
static int
__compact_worker(WT_SESSION_IMPL *session)
{
	WT_DECL_RET;
	u_int i, loop;
	bool didwork;

	/*
	 * Reset the handles' compaction skip flag (we don't bother setting
	 * or resetting it when we finish compaction, it's simpler to do it
	 * once, here).
	 */
	for (i = 0; i < session->op_handle_next; ++i)
		session->op_handle[i]->compact_skip = false;

	/*
	 * Perform an initial checkpoint (see this file's leading comment for
	 * details).
	 */
	WT_ERR(__compact_checkpoint(session));

	/*
	 * We compact 10% of a file on each pass (but the overall size of the
	 * file is decreasing each time, so we're not compacting 10% of the
	 * original file each time). Try 100 times (which is clearly more than
	 * we need); quit if we make no progress.
	 */
	for (loop = 0; loop < 100; ++loop) {
		/* Step through the list of files being compacted. */
		for (didwork = false, i = 0; i < session->op_handle_next; ++i) {
			/* Skip objects where there's no more work. */
			if (session->op_handle[i]->compact_skip)
				continue;

			session->compact_state = WT_COMPACT_RUNNING;
			WT_WITH_DHANDLE(session,
			    session->op_handle[i], ret = __wt_compact(session));
			WT_ERR(ret);

			/* If we did no work, skip this file in the future. */
			if (session->compact_state == WT_COMPACT_SUCCESS)
				didwork = true;
			else
				session->op_handle[i]->compact_skip = true;
		}
		if (!didwork)
			break;

		/*
		 * Perform two checkpoints (see this file's leading comment for
		 * details).
		 */
		WT_ERR(__compact_checkpoint(session));
		WT_ERR(__compact_checkpoint(session));
	}

err:	session->compact_state = WT_COMPACT_NONE;

	return (ret);
}

/*
 * __wt_session_compact --
 *	WT_SESSION.compact method.
 */
int
__wt_session_compact(
    WT_SESSION *wt_session, const char *uri, const char *config)
{
	WT_COMPACT_STATE compact;
	WT_CONFIG_ITEM cval;
	WT_DATA_SOURCE *dsrc;
	WT_DECL_RET;
	WT_SESSION_IMPL *session;
	WT_TXN *txn;
	u_int i;

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL(session, compact, config, cfg);

	/* In-memory is already as compact as it's going to get. */
	if (F_ISSET(S2C(session), WT_CONN_IN_MEMORY))
		goto err;

	/*
	 * Non-LSM object compaction requires checkpoints, which are impossible
	 * in transactional contexts. Disallow in all contexts (there's no
	 * reason for LSM to allow this, possible or not), and check now so the
	 * error message isn't confusing.
	 */
	txn = &session->txn;
	if (F_ISSET(txn, WT_TXN_RUNNING))
		WT_ERR_MSG(session, EINVAL,
		    "compaction not permitted in a transaction");

	/* Disallow objects in the WiredTiger name space. */
	WT_ERR(__wt_str_name_check(session, uri));

	if (!WT_PREFIX_MATCH(uri, "colgroup:") &&
	    !WT_PREFIX_MATCH(uri, "file:") &&
	    !WT_PREFIX_MATCH(uri, "index:") &&
	    !WT_PREFIX_MATCH(uri, "lsm:") &&
	    !WT_PREFIX_MATCH(uri, "table:")) {
		if ((dsrc = __wt_schema_get_source(session, uri)) != NULL)
			ret = dsrc->compact == NULL ?
			    __wt_object_unsupported(session, uri) :
			    dsrc->compact(
			    dsrc, wt_session, uri, (WT_CONFIG_ARG *)cfg);
		else
			ret = __wt_bad_object_type(session, uri);
		goto err;
	}

	/* Setup the session handle's compaction state structure. */
	memset(&compact, 0, sizeof(WT_COMPACT_STATE));
	session->compact = &compact;

	/* Compaction can be time-limited. */
	WT_ERR(__wt_config_gets(session, cfg, "timeout", &cval));
	session->compact->max_time = (uint64_t)cval.val;
	__wt_epoch(session, &session->compact->begin);

	/* Find the types of data sources being compacted. */
	WT_WITH_SCHEMA_LOCK(session,
	    ret = __wt_schema_worker(session, uri,
	    __compact_handle_append, __compact_uri_analyze, cfg, 0));
	WT_ERR(ret);

	if (session->compact->lsm_count != 0)
		WT_ERR(__wt_schema_worker(
		    session, uri, NULL, __wt_lsm_compact, cfg, 0));
	if (session->compact->file_count != 0)
		WT_ERR(__compact_worker(session));

err:	session->compact = NULL;

	for (i = 0; i < session->op_handle_next; ++i) {
		WT_WITH_DHANDLE(session, session->op_handle[i],
		    WT_TRET(__compact_end(session)));
		WT_WITH_DHANDLE(session, session->op_handle[i],
		    WT_TRET(__wt_session_release_btree(session)));
	}

	__wt_free(session, session->op_handle);
	session->op_handle_allocated = session->op_handle_next = 0;

	/*
	 * Release common session resources (for example, checkpoint may acquire
	 * significant reconciliation structures/memory).
	 */
	WT_TRET(__wt_session_release_resources(session));

	if (ret != 0)
		WT_STAT_CONN_INCR(session, session_table_compact_fail);
	else
		WT_STAT_CONN_INCR(session, session_table_compact_success);
	API_END_RET_NOTFOUND_MAP(session, ret);
}

/*
 * __wt_session_compact_readonly --
 *	WT_SESSION.compact method; readonly version.
 */
int
__wt_session_compact_readonly(
    WT_SESSION *wt_session, const char *uri, const char *config)
{
	WT_DECL_RET;
	WT_SESSION_IMPL *session;

	WT_UNUSED(uri);
	WT_UNUSED(config);

	session = (WT_SESSION_IMPL *)wt_session;
	SESSION_API_CALL_NOCONF(session, compact);

	WT_STAT_CONN_INCR(session, session_table_compact_fail);
	ret = __wt_session_notsup(session);
err:	API_END_RET(session, ret);
}
