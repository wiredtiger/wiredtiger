/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __lsm_bloom_create(
    WT_SESSION_IMPL *, WT_LSM_TREE *, WT_LSM_CHUNK *);
static int __lsm_free_chunks(WT_SESSION_IMPL *, WT_LSM_TREE *);

/*
 * __wt_lsm_worker --
 *	The worker thread for an LSM tree, responsible for writing in-memory
 *	trees to disk and merging on-disk trees.
 */
void *
__wt_lsm_worker(void *arg)
{
	WT_LSM_TREE *lsm_tree;
	WT_SESSION_IMPL *session;
	int progress, stalls;

	lsm_tree = arg;
	session = lsm_tree->worker_session;
	stalls = 0;

	while (F_ISSET(lsm_tree, WT_LSM_TREE_WORKING)) {
		progress = 0;

		/* Clear any state from previous worker thread iterations. */
		session->btree = NULL;

		/* Report stalls to merge in seconds. */
		if (__wt_lsm_merge(session, lsm_tree, stalls / 1000) == 0)
			progress = 1;

		/* Clear any state from previous worker thread iterations. */
		session->btree = NULL;

		if (lsm_tree->nold_chunks != lsm_tree->old_avail &&
		    __lsm_free_chunks(session, lsm_tree) == 0)
			progress = 1;

		if (progress)
			stalls = 0;
		else {
			__wt_sleep(0, 1000);
			++stalls;
		}
	}

	return (NULL);
}

/*
 * __wt_lsm_checkpoint_worker --
 *	A worker thread for an LSM tree, responsible for checkpointing chunks
 *	once they become read only.
 */
void *
__wt_lsm_checkpoint_worker(void *arg)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	WT_LSM_TREE *lsm_tree;
	WT_LSM_WORKER_COOKIE cookie;
	WT_SESSION_IMPL *session;
	const char *cfg[] = { "name=,drop=", NULL };
	int i, j;

	lsm_tree = arg;
	session = lsm_tree->ckpt_session;

	WT_CLEAR(cookie);

	while (F_ISSET(lsm_tree, WT_LSM_TREE_WORKING)) {
		WT_ERR(__wt_lsm_copy_chunks(session, lsm_tree, &cookie));

		/* Write checkpoints in all completed files. */
		for (i = 0, j = 0; i < cookie.nchunks; i++) {
			chunk = cookie.chunk_array[i];
			if (F_ISSET(chunk, WT_LSM_CHUNK_ONDISK))
				continue;
			/* Stop if a thread is still active in the chunk. */
			if (chunk->ncursor != 0)
				break;

			WT_ERR(__lsm_bloom_create(
			    session, lsm_tree, chunk));
			/*
			 * NOTE: we pass a non-NULL config, because otherwise
			 * __wt_checkpoint thinks we're closing the file.
			 */
			WT_WITH_SCHEMA_LOCK(session,
			    ret = __wt_schema_worker(session, chunk->uri,
			    __wt_checkpoint, cfg, 0));
			if (ret == 0) {
				++j;
				__wt_spin_lock(session, &lsm_tree->lock);
				F_SET(chunk, WT_LSM_CHUNK_ONDISK);
				lsm_tree->dsk_gen++;
				__wt_spin_unlock(session, &lsm_tree->lock);
				WT_VERBOSE_ERR(session, lsm,
				     "LSM worker checkpointed %d.", i);
			}
		}
		if (j == 0)
			__wt_sleep(0, 10);
	}
err:	__wt_free(session, cookie.chunk_array);

	return (NULL);
}

/*
 * __wt_lsm_copy_chunks --
 *	 Take a copy of part of the LSM tree chunk array so that we can work on
 *	 the contents without holding the LSM tree handle lock long term.
 */
int
__wt_lsm_copy_chunks(WT_SESSION_IMPL *session,
    WT_LSM_TREE *lsm_tree, WT_LSM_WORKER_COOKIE *cookie)
{
	WT_DECL_RET;
	int nchunks;

	/* Always return zero chunks on error. */
	cookie->nchunks = 0;

	__wt_spin_lock(session, &lsm_tree->lock);
	if (!F_ISSET(lsm_tree, WT_LSM_TREE_WORKING)) {
		__wt_spin_unlock(session, &lsm_tree->lock);
		/* The actual error value is ignored. */
		return (WT_ERROR);
	}
	/*
	 * Take a copy of the current state of the LSM tree. Skip
	 * the last chunk - since it is the active one and not relevant
	 * to merge operations.
	 */
	nchunks = lsm_tree->nchunks - 1;

	/*
	 * If the tree array of active chunks is larger than our current buffer,
	 * increase the size of our current buffer to match.
	 */
	if (cookie->chunk_alloc < lsm_tree->chunk_alloc)
		ret = __wt_realloc(session,
		    &cookie->chunk_alloc, lsm_tree->chunk_alloc,
		    &cookie->chunk_array);
	if (ret == 0 && nchunks > 0)
		memcpy(cookie->chunk_array, lsm_tree->chunk,
		    nchunks * sizeof(*lsm_tree->chunk));
	__wt_spin_unlock(session, &lsm_tree->lock);

	if (ret == 0)
		cookie->nchunks = nchunks;
	return (ret);
}

/*
 * Create a bloom filter for a chunk of the LSM tree that has not yet been
 * merged. Uses a cursor on the yet to be checkpointed in-memory chunk, so
 * the cache should not be excessively churned.
 */
static int
__lsm_bloom_create(WT_SESSION_IMPL *session,
    WT_LSM_TREE *lsm_tree, WT_LSM_CHUNK *chunk)
{
	WT_BLOOM *bloom;
	WT_CURSOR *src;
	WT_DECL_RET;
	WT_ITEM key;
	const char *cur_cfg[] = API_CONF_DEFAULTS(session, open_cursor, "raw");
	uint64_t insert_count;

	if (!FLD_ISSET(lsm_tree->bloom, WT_LSM_BLOOM_NEWEST) ||
	    chunk->count == 0)
		return (0);

	WT_ASSERT(session, chunk->bloom_uri != NULL);

	bloom = NULL;

	WT_ERR(__wt_bloom_create(session, chunk->bloom_uri, NULL, chunk->count,
	    lsm_tree->bloom_bit_count, lsm_tree->bloom_hash_count, &bloom));

	WT_ERR(__wt_open_cursor(session, chunk->uri, NULL, cur_cfg, &src));

	for (insert_count = 0; (ret = src->next(src)) == 0; insert_count++) {
		WT_ERR(src->get_key(src, &key));
		WT_ERR(__wt_bloom_insert(bloom, &key));
	}
	WT_ERR_NOTFOUND_OK(ret);
	WT_TRET(src->close(src));

	WT_TRET(__wt_bloom_finalize(bloom));
	WT_ERR(ret);

	WT_VERBOSE_ERR(session, lsm,
	    "LSM checkpoint worker created bloom filter. "
	    "Expected %" PRIu64 " items, got %" PRIu64,
	    chunk->count, insert_count);

	F_SET(chunk, WT_LSM_CHUNK_BLOOM);
err:	if (bloom != NULL)
		WT_TRET(__wt_bloom_close(bloom));
	return (ret);
}

static int
__lsm_free_chunks(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
{
	WT_DECL_RET;
	WT_LSM_CHUNK *chunk;
	const char *drop_cfg[] = { NULL };
	int locked, progress, i;

	locked = progress = 0;
	for (i = 0; i < lsm_tree->nold_chunks; i++) {
		if ((chunk = lsm_tree->old_chunks[i]) == NULL)
			continue;
		if (!locked) {
			locked = 1;
			/* TODO: Do we need the lsm_tree lock for all drops? */
			__wt_spin_lock(session, &lsm_tree->lock);
		}
		if (F_ISSET(chunk, WT_LSM_CHUNK_BLOOM)) {
			WT_WITH_SCHEMA_LOCK(session, ret = __wt_schema_drop(
			    session, chunk->bloom_uri, drop_cfg));
			/*
			 * An EBUSY return is acceptable - a cursor may still
			 * be positioned on this old chunk.
			 */
			if (ret == 0) {
				progress = 1;
				F_CLR(chunk, WT_LSM_CHUNK_BLOOM);
				__wt_free(session, chunk->bloom_uri);
				chunk->bloom_uri = NULL;
			} else if (ret != EBUSY)
				goto err;
			if (ret == EBUSY)
				WT_VERBOSE_ERR(session, lsm,
				    "LSM worker bloom drop busy: %s.",
				    chunk->bloom_uri);
		}
		if (chunk->uri != NULL) {
			WT_WITH_SCHEMA_LOCK(session, ret =
			    __wt_schema_drop(session, chunk->uri, drop_cfg));
			/*
			 * An EBUSY return is acceptable - a cursor may still
			 * be positioned on this old chunk.
			 */
			if (ret == 0) {
				progress = 1;
				__wt_free(session, chunk->uri);
				chunk->uri = NULL;
			} else if (ret != EBUSY)
				goto err;
		}

		if (chunk->uri == NULL &&
		    !F_ISSET(chunk, WT_LSM_CHUNK_BLOOM)) {
			__wt_free(session, lsm_tree->old_chunks[i]);
			++lsm_tree->old_avail;
		}
	}
	if (locked) {
err:		WT_TRET(__wt_lsm_meta_write(session, lsm_tree));
		__wt_spin_unlock(session, &lsm_tree->lock);
	}

	/* Returning non-zero means there is no work to do. */
	if (!progress)
		WT_TRET(WT_NOTFOUND);

	return (ret);
}
