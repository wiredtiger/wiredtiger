/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __tree_walk_delete_rollback --
 *	Abort pages that were deleted without being instantiated.
 */
void
__wt_tree_walk_delete_rollback(WT_REF *ref)
{
	WT_PAGE *page;
	WT_ROW *rip;
	WT_UPDATE *upd;
	uint32_t i;

	/*
	 * If the page is still marked deleted, it's as we left it, reset the
	 * state to on-disk and we're done.
	 */
	if (WT_ATOMIC_CAS(ref->state, WT_REF_DELETED, WT_REF_DISK))
		return;

	/*
	 * The page is either instantiated or being instantiated -- wait for
	 * the page to settle down, as needed, and then clean up the update
	 * structures.  We don't need a hazard pointer or anything on the
	 * page because there are unresolved transactions, the page can't go
	 * anywhere.
	 */
	while (ref->state != WT_REF_MEM)
		__wt_yield();
	page = ref->page;
	WT_ROW_FOREACH(page, rip, i)
		for (upd =
		    WT_ROW_UPDATE(page, rip); upd != NULL; upd = upd->next)
			if (upd->txnid == ref->txnid)
				upd->txnid = WT_TXN_ABORTED;
}

/*
 * __tree_walk_delete --
 *	If deleting a range, try to delete the page without instantiating it.
 */
static inline int
__tree_walk_delete(
    WT_SESSION_IMPL *session, WT_PAGE *page, WT_REF *ref, int *skipp)
{
	WT_CELL_UNPACK unpack;
	WT_DECL_RET;

	*skipp = 0;

	/*
	 * If the page is already instantiated in-memory, other threads may be
	 * using it, no fast delete.
	 *
	 * Atomically switch the page's state to lock it.  If the page state
	 * changes underneath us, no fast delete.
	 *
	 * Possible optimization: if the page is already deleted and the delete
	 * is visible to us (the delete has been committed), we could skip the
	 * page instead of instantiating it and figuring out there are no rows
	 * in the page.  While that's a huge amount of work to no purpose, it's
	 * unclear optimizing for overlapping range deletes is worth the effort.
	 */
	if (ref->state != WT_REF_DISK ||
	    !WT_ATOMIC_CAS(ref->state, WT_REF_DISK, WT_REF_LOCKED))
		return (0);

	/*
	 * We may be in a reconciliation-built internal page if the page split.
	 * In that case, the reference address doesn't point to a cell.  While
	 * we could probably still fast-delete the page, I doubt it's a common
	 * enough case to make it worth the effort.  Skip fast deletes inside
	 * split merge pages.
	 */
	if (__wt_off_page(page, ref->addr))
		goto err;

	/*
	 * If the page references overflow items, we have to clean it up during
	 * reconciliation, no fast delete.   Check this after we have the page
	 * locked down, instantiating the page in memory and modifying it could
	 * theoretically point the address somewhere away from the on-page cell.
	 */
	__wt_cell_unpack(ref->addr, &unpack);
	if (unpack.raw != WT_CELL_ADDR_LNO)
		goto err;

	/*
	 * Record the change in the transaction structure and set the change's
	 * transaction ID.
	 */
	WT_ERR(__wt_txn_modify_ref(session, ref));

	/*
	 * This action dirties the parent page: mark it dirty now, there's no
	 * future reconciliation of the child leaf page that will dirty it as
	 * we write the tree.
	 */
	WT_ERR(__wt_page_modify_init(session, page));
	__wt_page_modify_set(session, page);

	*skipp = 1;

	/* Delete the page. */
	WT_PUBLISH(ref->state, WT_REF_DELETED);
	return (0);

err:	/*
	 * Restore the page to on-disk status, we'll have to instantiate it.
	 * We're don't have to back out adding this node to the transaction
	 * modify list, that's OK because the rollback function ignores nodes
	 * that aren't set to WT_REF_DELETED.
	 */
	WT_PUBLISH(ref->state, WT_REF_DISK);
	return (ret);
}

/*
 * __tree_walk_read --
 *	If iterating a cursor, skip deleted pages that are visible to us.
 */
static inline int
__tree_walk_read(WT_SESSION_IMPL *session, WT_REF *ref, int *skipp)
{
	*skipp = 0;

	/*
	 * Do a simple test first, avoid the atomic operation unless it's
	 * demonstrably necessary.
	 */
	if (ref->state != WT_REF_DELETED)
		return (0);

	/*
	 * It's possible the state is changing underneath us, we could race
	 * between checking for a deleted state and looking at the stored
	 * transaction ID to see if the delete is visible to us.  Lock down
	 * the structure.
	 */
	if (!WT_ATOMIC_CAS(ref->state, WT_REF_DELETED, WT_REF_LOCKED))
		return (0);

	*skipp = __wt_txn_visible(session, ref->txnid) ? 1 : 0;

	WT_PUBLISH(ref->state, WT_REF_DELETED);
	return (0);
}

/*
 * __wt_tree_walk --
 *	Move to the next/previous page in the tree.
 */
int
__wt_tree_walk(WT_SESSION_IMPL *session, WT_PAGE **pagep, uint32_t flags)
{
	WT_BTREE *btree;
	WT_PAGE *page, *t;
	WT_REF *ref;
	uint32_t slot;
	int cache, compact, discard, eviction, prev, set_read_gen;
	int skip, skip_intl, skip_leaf;

	btree = session->btree;

	/* Fast-discard currently only works on row-store trees. */
	discard = LF_ISSET(WT_TREE_DISCARD) && btree->type == BTREE_ROW ? 1 : 0;

	compact = LF_ISSET(WT_TREE_COMPACT) ? 1 : 0;
	eviction = LF_ISSET(WT_TREE_EVICT) ? 1 : 0;
	cache = LF_ISSET(WT_TREE_CACHE) ? 1 : 0;
	prev = LF_ISSET(WT_TREE_PREV) ? 1 : 0;
	skip_intl = LF_ISSET(WT_TREE_SKIP_INTL) ? 1 : 0;
	skip_leaf = LF_ISSET(WT_TREE_SKIP_LEAF) ? 1 : 0;

	/*
	 * Take a copy of any returned page; we have a hazard pointer on the
	 * page, by definition.
	 */
	page = *pagep;
	*pagep = NULL;

	/* If no page is active, begin a walk from the start of the tree. */
	if (page == NULL) {
		if ((page = btree->root_page) == NULL)
			return (0);
		slot = prev ? page->entries - 1 : 0;
		goto descend;
	}

ascend:	/* If the active page was the root, we've reached the walk's end. */
	if (WT_PAGE_IS_ROOT(page))
		return (0);

	/* Figure out the current slot in the parent page's WT_REF array. */
	t = page->parent;
	slot = (uint32_t)(page->ref - t->u.intl.t);

	/* If not the eviction thread, release the page's hazard pointer. */
	if (eviction) {
		if (page->ref->state == WT_REF_EVICT_WALK)
			page->ref->state = WT_REF_MEM;
	} else
		WT_RET(__wt_page_release(session, page));

	/* Switch to the parent. */
	page = t;

	/*
	 * If we're at the last/first slot on the page, return this page in
	 * post-order traversal.  Otherwise we move to the next/prev slot
	 * and left/right-most element in its subtree.
	 */
	for (;;) {
		if ((prev && slot == 0) ||
		    (!prev && slot == page->entries - 1)) {
			if (skip_intl)
				goto ascend;
			*pagep = page;
			return (0);
		}
		if (prev)
			--slot;
		else
			++slot;

descend:	for (;;) {
			if (page->type == WT_PAGE_ROW_INT ||
			    page->type == WT_PAGE_COL_INT)
				ref = &page->u.intl.t[slot];
			else if (skip_leaf)
				goto ascend;
			else {
				*pagep = page;
				return (0);
			}

			/*
			 * There are several reasons to walk an in-memory tree:
			 *
			 * (1) to write all dirty leaf nodes;
			 * (2) to find pages to evict;
			 * (3) to find pages for compaction; and
			 * (4) to write internal nodes (creating a checkpoint);
			 * (5) to close a file, discarding pages;
			 * (6) to perform cursor scans.
			 *
			 * For cases (2)-(4), we want all ordinary in-memory
			 * pages, and we swap the state to WT_REF_EVICT_WALK
			 * temporarily to avoid the page being evicted by
			 * another thread while it is being evaluated.  The
			 * other cases use __wt_page_in to get hazard pointers
			 * and protect the page from eviction that way.
			 */
			set_read_gen = 0;
			if (eviction) {
retry:				if (!WT_ATOMIC_CAS(ref->state,
				    WT_REF_MEM, WT_REF_EVICT_WALK)) {
					if (!LF_ISSET(WT_TREE_WAIT) ||
					    ref->state == WT_REF_DELETED ||
					    ref->state == WT_REF_DISK)
						break;

					/*
					 * A walk to checkpoint the file may
					 * collide with the current LRU
					 * eviction walk.
					 *
					 * If so, clear the LRU walk, or the
					 * checkpoint will be blocked until the
					 * eviction thread next wakes up and
					 * decides to search for some more
					 * pages to evict.
					 */
					__wt_evict_clear_tree_walk(
					    session, NULL);
					__wt_yield();
					goto retry;
				}
			} else if (cache) {
				/*
				 * Only look at unlocked pages in memory.
				 * There is a race here, but worse case is
				 * that the page will be read back in to cache.
				 */
				while (LF_ISSET(WT_TREE_WAIT) &&
				    (ref->state == WT_REF_EVICT_FORCE ||
				    ref->state == WT_REF_LOCKED))
					__wt_yield();
				if (ref->state == WT_REF_DELETED ||
				    ref->state == WT_REF_DISK)
					break;
				/* Grab a hazard pointer. */
				WT_RET(__wt_page_in(session, page, ref));
			} else if (discard) {
				/*
				 * If deleting a range, try to delete the page
				 * without instantiating it.
				 */
				WT_RET(__tree_walk_delete(
				    session, page, ref, &skip));
				if (skip)
					break;
				WT_RET(__wt_page_in(session, page, ref));
			} else {
				/*
				 * If iterating a cursor (or doing compaction),
				 * skip deleted pages that are visible to us.
				 */
				WT_RET(__tree_walk_read(session, ref, &skip));
				if (skip)
					break;

				/*
				 * Test if the page is useful for compaction:
				 * we don't want to read it if it won't help.
				 *
				 * Pages read for compaction aren't "useful";
				 * reset the page generation to 0 so the page
				 * is quickly chosen for eviction.  (This can
				 * race of course, but it's unlikely and will
				 * only result in an incorrectly low page read
				 * generation.)
				 */
				set_read_gen = 0;
				if (compact) {
					WT_RET(__wt_compact_page_skip(
					    session, page, ref, &skip));
					if (skip)
						break;
					set_read_gen =
					    ref->state == WT_REF_DISK ? 1 : 0;
				}

				WT_RET(__wt_page_in(session, page, ref));
				if (set_read_gen)
					page->read_gen = 0;
			}

			page = ref->page;
			WT_ASSERT(session, page != NULL);
			slot = prev ? page->entries - 1 : 0;
		}
	}
	/* NOTREACHED */
}
