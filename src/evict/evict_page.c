/*-
 * Copyright (c) 2014-2017 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __evict_page_clean_update(WT_SESSION_IMPL *, WT_REF *, bool);
static int __evict_page_dirty_update(WT_SESSION_IMPL *, WT_REF *, bool);
static int __evict_review(WT_SESSION_IMPL *, WT_REF *, bool, bool *);

/*
 * __evict_exclusive_clear --
 *	Release exclusive access to a page.
 */
static inline void
__evict_exclusive_clear(WT_SESSION_IMPL *session, WT_REF *ref)
{
	WT_ASSERT(session, ref->state == WT_REF_LOCKED && ref->page != NULL);

	ref->state = WT_REF_MEM;
}

/*
 * __evict_exclusive --
 *	Acquire exclusive access to a page.
 */
static inline int
__evict_exclusive(WT_SESSION_IMPL *session, WT_REF *ref)
{
	WT_ASSERT(session, ref->state == WT_REF_LOCKED);

	/*
	 * Check for a hazard pointer indicating another thread is using the
	 * page, meaning the page cannot be evicted.
	 */
	if (__wt_hazard_check(session, ref) == NULL)
		return (0);

	WT_STAT_DATA_INCR(session, cache_eviction_hazard);
	WT_STAT_CONN_INCR(session, cache_eviction_hazard);
	return (EBUSY);
}

/*
 * __wt_page_release_evict --
 *	Release a reference to a page, and attempt to immediately evict it.
 */
int
__wt_page_release_evict(WT_SESSION_IMPL *session, WT_REF *ref)
{
	struct timespec start, stop;
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE *page;
	bool locked, too_big;

	btree = S2BT(session);
	page = ref->page;
	__wt_epoch(session, &start);

	/*
	 * Take some care with order of operations: if we release the hazard
	 * reference without first locking the page, it could be evicted in
	 * between.
	 */
	locked = __wt_atomic_casv32(&ref->state, WT_REF_MEM, WT_REF_LOCKED);
	if ((ret = __wt_hazard_clear(session, ref)) != 0 || !locked) {
		if (locked)
			ref->state = WT_REF_MEM;
		return (ret == 0 ? EBUSY : ret);
	}

	(void)__wt_atomic_addv32(&btree->evict_busy, 1);

	too_big = page->memory_footprint >= btree->splitmempage;

	/*
	 * Track how long the call to evict took. If eviction is successful then
	 * we have one of two pairs of stats to increment.
	 */
	ret = __wt_evict(session, ref, false);
	__wt_epoch(session, &stop);
	if (ret == 0) {
		if (too_big) {
			WT_STAT_CONN_INCR(session, cache_eviction_force);
			WT_STAT_CONN_INCRV(session, cache_eviction_force_time,
			    WT_TIMEDIFF_US(stop, start));
		} else {
			/*
			 * If the page isn't too big, we are evicting it because
			 * it had a chain of deleted entries that make traversal
			 * expensive.
			 */
			WT_STAT_CONN_INCR(session, cache_eviction_force_delete);
			WT_STAT_CONN_INCRV(session,
			    cache_eviction_force_delete_time,
			    WT_TIMEDIFF_US(stop, start));
		}
	} else {
		WT_STAT_CONN_INCR(session, cache_eviction_force_fail);
		WT_STAT_CONN_INCRV(session, cache_eviction_force_fail_time,
		    WT_TIMEDIFF_US(stop, start));
	}

	(void)__wt_atomic_subv32(&btree->evict_busy, 1);

	return (ret);
}

/*
 * __wt_evict --
 *	Evict a page.
 */
int
__wt_evict(WT_SESSION_IMPL *session, WT_REF *ref, bool closing)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_PAGE *page;
	WT_PAGE_MODIFY *mod;
	bool clean_page, inmem_split, tree_dead;

	conn = S2C(session);

	/* Enter the eviction generation. */
	__wt_session_gen_enter(session, WT_GEN_EVICT);

	page = ref->page;
	tree_dead = F_ISSET(session->dhandle, WT_DHANDLE_DEAD);

	__wt_verbose(session, WT_VERB_EVICT,
	    "page %p (%s)", (void *)page, __wt_page_type_string(page->type));

	/*
	 * Get exclusive access to the page and review it for conditions that
	 * would block our eviction of the page.  If the check fails (for
	 * example, we find a page with active children), we're done.  We have
	 * to make this check for clean pages, too: while unlikely eviction
	 * would choose an internal page with children, it's not disallowed.
	 */
	WT_ERR(__evict_review(session, ref, closing, &inmem_split));

	/*
	 * If there was an in-memory split, the tree has been left in the state
	 * we want: there is nothing more to do.
	 */
	if (inmem_split)
		goto done;

	/* Count evictions of internal pages during normal operation. */
	if (!closing && WT_PAGE_IS_INTERNAL(page)) {
		WT_STAT_CONN_INCR(session, cache_eviction_internal);
		WT_STAT_DATA_INCR(session, cache_eviction_internal);
	}

	/*
	 * Track the largest page size seen at eviction, it tells us something
	 * about our ability to force pages out before they're larger than the
	 * cache.
	 */
	if (page->memory_footprint > conn->cache->evict_max_page_size)
		conn->cache->evict_max_page_size = page->memory_footprint;

	/* Figure out whether reconciliation was done on the page */
	mod = page->modify;
	clean_page = mod == NULL || mod->rec_result == 0;

	/* Update the reference and discard the page. */
	if (__wt_ref_is_root(ref))
		__wt_ref_out(session, ref);
	else if ((clean_page && !F_ISSET(conn, WT_CONN_IN_MEMORY)) || tree_dead)
		/*
		 * Pages that belong to dead trees never write back to disk
		 * and can't support page splits.
		 */
		WT_ERR(__evict_page_clean_update(
		    session, ref, tree_dead || closing));
	else
		WT_ERR(__evict_page_dirty_update(session, ref, closing));

	if (clean_page) {
		WT_STAT_CONN_INCR(session, cache_eviction_clean);
		WT_STAT_DATA_INCR(session, cache_eviction_clean);
	} else {
		WT_STAT_CONN_INCR(session, cache_eviction_dirty);
		WT_STAT_DATA_INCR(session, cache_eviction_dirty);
	}

	if (0) {
err:		if (!closing)
			__evict_exclusive_clear(session, ref);

		WT_STAT_CONN_INCR(session, cache_eviction_fail);
		WT_STAT_DATA_INCR(session, cache_eviction_fail);
	}

done:	/* Leave the eviction generation. */
	__wt_session_gen_leave(session, WT_GEN_EVICT);

	return (ret);
}

/*
 * __evict_delete_ref --
 *	Mark a page reference deleted and check if the parent can reverse
 *	split.
 */
static int
__evict_delete_ref(WT_SESSION_IMPL *session, WT_REF *ref, bool closing)
{
	WT_DECL_RET;
	WT_PAGE *parent;
	WT_PAGE_INDEX *pindex;
	uint32_t ndeleted;

	if (__wt_ref_is_root(ref))
		return (0);

	/*
	 * Avoid doing reverse splits when closing the file, it is wasted work
	 * and some structures may have already been freed.
	 */
	if (!closing) {
		parent = ref->home;
		WT_INTL_INDEX_GET(session, parent, pindex);
		ndeleted = __wt_atomic_addv32(&pindex->deleted_entries, 1);

		/*
		 * If more than 10% of the parent references are deleted, try a
		 * reverse split.  Don't bother if there is a single deleted
		 * reference: the internal page is empty and we have to wait
		 * for eviction to notice.
		 *
		 * This will consume the deleted ref (and eventually free it).
		 * If the reverse split can't get the access it needs because
		 * something is busy, be sure that the page still ends up
		 * marked deleted.
		 */
		if (ndeleted > pindex->entries / 10 && pindex->entries > 1) {
			if ((ret = __wt_split_reverse(session, ref)) == 0)
				return (0);
			WT_RET_BUSY_OK(ret);

			/*
			 * The child must be locked after a failed reverse
			 * split.
			 */
			WT_ASSERT(session, ref->state == WT_REF_LOCKED);
		}
	}

	WT_PUBLISH(ref->state, WT_REF_DELETED);
	return (0);
}

/*
 * __evict_page_clean_update --
 *	Update a clean page's reference on eviction.
 */
static int
__evict_page_clean_update(WT_SESSION_IMPL *session, WT_REF *ref, bool closing)
{
	WT_DECL_RET;

	/*
	 * Discard the page and update the reference structure; if the page has
	 * an address, it's a disk page; if it has no address, it's a deleted
	 * page re-instantiated (for example, by searching) and never written.
	 */
	__wt_ref_out(session, ref);
	if (ref->addr == NULL) {
		WT_WITH_PAGE_INDEX(session,
		    ret = __evict_delete_ref(session, ref, closing));
		WT_RET_BUSY_OK(ret);
	} else
		WT_PUBLISH(ref->state, WT_REF_DISK);

	return (0);
}

/*
 * __evict_page_dirty_update --
 *	Update a dirty page's reference on eviction.
 */
static int
__evict_page_dirty_update(WT_SESSION_IMPL *session, WT_REF *ref, bool closing)
{
	WT_ADDR *addr;
	WT_DECL_RET;
	WT_MULTI multi;
	WT_PAGE_MODIFY *mod;

	mod = ref->page->modify;

	WT_ASSERT(session, ref->addr == NULL);

	switch (mod->rec_result) {
	case WT_PM_REC_EMPTY:				/* Page is empty */
		/*
		 * Update the parent to reference a deleted page.  The fact that
		 * reconciliation left the page "empty" means there's no older
		 * transaction in the system that might need to see an earlier
		 * version of the page.  For that reason, we clear the address
		 * of the page, if we're forced to "read" into that namespace,
		 * we'll instantiate a new page instead of trying to read from
		 * the backing store.
		 */
		__wt_ref_out(session, ref);
		ref->addr = NULL;
		WT_WITH_PAGE_INDEX(session,
		    ret = __evict_delete_ref(session, ref, closing));
		WT_RET_BUSY_OK(ret);
		break;
	case WT_PM_REC_MULTIBLOCK:			/* Multiple blocks */
		/*
		 * Either a split where we reconciled a page and it turned into
		 * a lot of pages or an in-memory page that got too large, we
		 * forcibly evicted it, and there wasn't anything to write.
		 *
		 * The latter is a special case of forced eviction. Imagine a
		 * thread updating a small set keys on a leaf page. The page
		 * is too large or has too many deleted items, so we try and
		 * evict it, but after reconciliation there's only a small
		 * amount of live data (so it's a single page we can't split),
		 * and if there's an older reader somewhere, there's data on
		 * the page we can't write (so the page can't be evicted). In
		 * that case, we end up here with a single block that we can't
		 * write. Take advantage of the fact we have exclusive access
		 * to the page and rewrite it in memory.
		 */
		if (mod->mod_multi_entries == 1) {
			WT_ASSERT(session, closing == false);
			WT_RET(__wt_split_rewrite(
			    session, ref, &mod->mod_multi[0]));
		} else
			WT_RET(__wt_split_multi(session, ref, closing));
		break;
	case WT_PM_REC_REPLACE: 			/* 1-for-1 page swap */
		/*
		 * Update the parent to reference the replacement page.
		 *
		 * A page evicted with lookaside entries may not have an
		 * address, if no updates were visible to reconciliation.
		 *
		 * Publish: a barrier to ensure the structure fields are set
		 * before the state change makes the page available to readers.
		 */
		if (mod->mod_replace.addr == NULL)
			ref->addr = NULL;
		else {
			WT_RET(__wt_calloc_one(session, &addr));
			*addr = mod->mod_replace;
			mod->mod_replace.addr = NULL;
			mod->mod_replace.size = 0;
			ref->addr = addr;
		}

		/*
		 * Eviction wants to keep this page if we have a disk image,
		 * re-instantiate the page in memory, else discard the page.
		 */
		if (mod->mod_disk_image == NULL) {
			if (mod->mod_replace_las_pageid != 0) {
				WT_RET(
				    __wt_calloc_one(session, &ref->page_las));
				ref->page_las->las_pageid =
				    mod->mod_replace_las_pageid;
#ifdef HAVE_TIMESTAMPS
				__wt_timestamp_set(
				    &ref->page_las->min_timestamp,
				    &mod->mod_replace_las_min_timestamp);
#endif
				__wt_ref_out(session, ref);
				WT_PUBLISH(ref->state, WT_REF_LOOKASIDE);
			} else {
				__wt_ref_out(session, ref);
				WT_PUBLISH(ref->state, WT_REF_DISK);
			}
		} else {
			/*
			 * The split code works with WT_MULTI structures, build
			 * one for the disk image.
			 */
			memset(&multi, 0, sizeof(multi));
			multi.disk_image = mod->mod_disk_image;

			WT_RET(__wt_split_rewrite(session, ref, &multi));
		}

		break;
	WT_ILLEGAL_VALUE(session);
	}

	return (0);
}

/*
 * __evict_child_check --
 *	Review an internal page for active children.
 */
static int
__evict_child_check(WT_SESSION_IMPL *session, WT_REF *parent)
{
	WT_REF *child;

	WT_INTL_FOREACH_BEGIN(session, parent->page, child) {
		switch (child->state) {
		case WT_REF_DISK:		/* On-disk */
		case WT_REF_DELETED:		/* On-disk, deleted */
			break;
		default:
			return (EBUSY);
		}
	} WT_INTL_FOREACH_END;

	return (0);
}

/*
 * __evict_review --
 *	Get exclusive access to the page and review the page and its subtree
 *	for conditions that would block its eviction.
 */
static int
__evict_review(
    WT_SESSION_IMPL *session, WT_REF *ref, bool closing, bool *inmem_splitp)
{
	WT_CACHE *cache;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_PAGE *page;
	uint32_t flags;
	bool lookaside_retry, *lookaside_retryp, modified;

	*inmem_splitp = false;

	conn = S2C(session);
	flags = WT_REC_EVICT;
	if (!WT_SESSION_IS_CHECKPOINT(session))
		LF_SET(WT_REC_VISIBLE_ALL);

	/*
	 * Get exclusive access to the page if our caller doesn't have the tree
	 * locked down.
	 */
	if (!closing) {
		WT_RET(__evict_exclusive(session, ref));

		/*
		 * Now the page is locked, remove it from the LRU eviction
		 * queue.  We have to do this before freeing the page memory or
		 * otherwise touching the reference because eviction paths
		 * assume a non-NULL reference on the queue is pointing at
		 * valid memory.
		 */
		__wt_evict_list_clear_page(session, ref);
	}

	/* Now that we have exclusive access, review the page. */
	page = ref->page;

	/*
	 * Fail if an internal has active children, the children must be evicted
	 * first. The test is necessary but shouldn't fire much: the eviction
	 * code is biased for leaf pages, an internal page shouldn't be selected
	 * for eviction until all children have been evicted.
	 */
	if (WT_PAGE_IS_INTERNAL(page)) {
		WT_WITH_PAGE_INDEX(session,
		    ret = __evict_child_check(session, ref));
		WT_RET(ret);
	}

	/*
	 * It is always OK to evict pages from dead trees if they don't have
	 * children.
	 */
	if (F_ISSET(session->dhandle, WT_DHANDLE_DEAD))
		return (0);

	/*
	 * Retrieve the modified state of the page. This must happen after the
	 * check for evictable internal pages otherwise there is a race where a
	 * page could be marked modified due to a child being transitioned to
	 * WT_REF_DISK after the modified check and before we visited the ref
	 * while walking the parent index.
	 */
	modified = __wt_page_is_modified(page);

	/*
	 * Clean pages can't be evicted when running in memory only. This
	 * should be uncommon - we don't add clean pages to the queue.
	 */
	if (F_ISSET(conn, WT_CONN_IN_MEMORY) && !modified && !closing)
		return (EBUSY);

	/* Check if the page can be evicted. */
	if (!closing) {
		/*
		 * Update the oldest ID to avoid wasted effort should it have
		 * fallen behind current.
		 */
		if (modified)
			WT_RET(__wt_txn_update_oldest(
			    session, WT_TXN_OLDEST_STRICT));

		if (!__wt_page_can_evict(session, ref, inmem_splitp))
			return (EBUSY);

		/*
		 * Check for an append-only workload needing an in-memory
		 * split; we can't do this earlier because in-memory splits
		 * require exclusive access. If an in-memory split completes,
		 * the page stays in memory and the tree is left in the desired
		 * state: avoid the usual cleanup.
		 */
		if (*inmem_splitp)
			return (__wt_split_insert(session, ref));
	}

	/* If the page is clean, we're done and we can evict. */
	if (!modified)
		return (0);

	/*
	 * If the page is dirty, reconcile it to decide if we can evict it.
	 *
	 * If we have an exclusive lock (we're discarding the tree), assert
	 * there are no updates we cannot read.
	 *
	 * Don't set any other flags for internal pages: there are no update
	 * lists to be saved and restored, changes can't be written into the
	 * lookaside table, nor can we re-create internal pages in memory.
	 *
	 * For leaf pages:
	 *
	 * In-memory pages are a known configuration.
	 *
	 * Set the update/restore flag, so reconciliation will write blocks it
	 * can write and create a list of skipped updates for blocks it cannot
	 * write, along with disk images. This is how eviction of active, huge
	 * pages works: we take a big page and reconcile it into blocks, some of
	 * which we write and discard, the rest of which we re-create as smaller
	 * in-memory pages, (restoring the updates that stopped us from writing
	 * the block), and inserting the whole mess into the page's parent. Set
	 * the flag in all cases because the incremental cost of update/restore
	 * in reconciliation is minimal, eviction shouldn't have picked a page
	 * where update/restore is necessary, absent some cache pressure. It's
	 * possible updates occurred after we selected this page for eviction,
	 * but it's unlikely and we don't try and manage that risk.
	 *
	 * Additionally, if we aren't trying to free space in the cache, scrub
	 * the page and keep it in memory.
	 */
	cache = conn->cache;
	lookaside_retry = false;
	lookaside_retryp = NULL;

	if (closing)
		LF_SET(WT_REC_VISIBILITY_ERR);
	else if (!WT_PAGE_IS_INTERNAL(page) &&
	    !F_ISSET(S2BT(session), WT_BTREE_LOOKASIDE)) {
		if (F_ISSET(conn, WT_CONN_IN_MEMORY))
			LF_SET(WT_REC_IN_MEMORY |
			    WT_REC_SCRUB | WT_REC_UPDATE_RESTORE);
		else {
			if (!WT_SESSION_IS_CHECKPOINT(session)) {
				LF_SET(WT_REC_UPDATE_RESTORE);

				if (F_ISSET(cache, WT_CACHE_EVICT_SCRUB))
					LF_SET(WT_REC_SCRUB);
			}

			/*
			 * If the cache is nearly stuck, check if
			 * reconciliation suggests trying the lookaside table
			 * unless lookaside eviction is disabled globally.
			 *
			 * We don't wait until the cache is completely stuck:
			 * for workloads where lookaside eviction is necessary
			 * to make progress, we don't want a single successful
			 * page eviction to make the cache "unstuck" so we have
			 * to wait again before evicting the next page.
			 */
			if (__wt_cache_nearly_stuck(session) &&
			    !F_ISSET(conn, WT_CONN_EVICTION_NO_LOOKASIDE))
				lookaside_retryp = &lookaside_retry;
		}
	}

	/* Reconcile the page. */
	ret = __wt_reconcile(session, ref, NULL, flags, lookaside_retryp);

	/*
	 * If reconciliation fails, eviction is stuck and reconciliation
	 * reports it might succeed if we use the lookaside table, then
	 * configure reconciliation to write those updates to the lookaside
	 * table, allowing the eviction of pages we'd otherwise have to retain
	 * in cache to support older readers.
	 */
	if (ret == EBUSY && lookaside_retry) {
		LF_CLR(WT_REC_SCRUB | WT_REC_UPDATE_RESTORE);
		LF_SET(WT_REC_LOOKASIDE);
		ret = __wt_reconcile(session, ref, NULL, flags, NULL);
	}

	WT_RET(ret);

	/*
	 * If attempting eviction in service of a checkpoint, we may
	 * successfully reconcile but then find that there are updates on the
	 * page too new to evict.  Give up in that case: checkpoint will
	 * reconcile the page normally.
	 */
	if (WT_SESSION_IS_CHECKPOINT(session) && !__wt_page_is_modified(page) &&
	    !LF_ISSET(WT_REC_LOOKASIDE) &&
	    !__wt_txn_visible_all(session, page->modify->rec_max_txn,
	    WT_TIMESTAMP_NULL(&page->modify->rec_max_timestamp)))
		return (EBUSY);

	/*
	 * Success: assert the page is clean or reconciliation was configured
	 * for update/restore. If the page is clean, assert that reconciliation
	 * was configured for a lookaside table, or it's not a durable object
	 * (currently the lookaside table), or all page updates were globally
	 * visible.
	 */
	WT_ASSERT(session,
	    !__wt_page_is_modified(page) || LF_ISSET(WT_REC_UPDATE_RESTORE));
	WT_ASSERT(session,
	    __wt_page_is_modified(page) ||
	    LF_ISSET(WT_REC_LOOKASIDE) ||
	    __wt_txn_visible_all(session, page->modify->rec_max_txn,
	    WT_TIMESTAMP_NULL(&page->modify->rec_max_timestamp)));

	return (0);
}
