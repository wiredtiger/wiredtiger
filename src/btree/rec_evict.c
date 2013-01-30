/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int  __hazard_exclusive(WT_SESSION_IMPL *, WT_REF *, int);
static void __rec_discard_page(WT_SESSION_IMPL *, WT_PAGE *, int);
static void __rec_discard_tree(WT_SESSION_IMPL *, WT_PAGE *, int);
static void __rec_excl_clear(WT_SESSION_IMPL *);
static void __rec_page_clean_update(WT_SESSION_IMPL *, WT_PAGE *);
static int  __rec_page_dirty_update(WT_SESSION_IMPL *, WT_PAGE *);
static int  __rec_review(WT_SESSION_IMPL *, WT_REF *, WT_PAGE *, int, int);
static void __rec_root_update(WT_SESSION_IMPL *);

/*
 * __wt_rec_evict --
 *	Reconciliation plus eviction.
 */
int
__wt_rec_evict(WT_SESSION_IMPL *session, WT_PAGE *page, int exclusive)
{
	WT_DECL_RET;
	WT_PAGE_MODIFY *mod;

	WT_VERBOSE_RET(session, evict,
	    "page %p (%s)", page, __wt_page_type_string(page->type));

	WT_ASSERT(session, session->excl_next == 0);

	/*
	 * Split-merge pages cannot be evicted, they're always merged into their
	 * parent; split-merge pages are ignored by the eviction thread, we
	 * never get a split-merge page to evict.  Check out of sheer paranoia.
	 * Split pages are NOT included in this test, because a split page can
	 * be separately evicted, at which point it's replaced in its parent by
	 * a reference to a split-merge page.  That's a normal part of the leaf
	 * page life-cycle if it grows too large and must be pushed out of the
	 * cache.
	 */
	mod = page->modify;
	if (mod != NULL && F_ISSET(mod, WT_PM_REC_SPLIT_MERGE))
		return (EBUSY);

	/*
	 * Get exclusive access to the page and review the page and its subtree
	 * for conditions that would block our eviction of the page.  If the
	 * check fails (for example, we find a child page that can't be merged),
	 * we're done.  We have to make this check for clean pages, too: while
	 * unlikely eviction would choose an internal page with children, it's
	 * not disallowed anywhere.
	 *
	 * Note that page->ref may be NULL in some cases (e.g., for root pages
	 * or during salvage).  That's OK if WT_REC_SINGLE is set: we won't
	 * check hazard pointers in that case.
	 */
	WT_ERR(__rec_review(session, page->ref, page, exclusive, 1));

	/*
	 * Update the page's modification reference, reconciliation might have
	 * changed it.
	 */
	mod = page->modify;

	/* Count evictions of internal pages during normal operation. */
	if (!exclusive &&
	    (page->type == WT_PAGE_COL_INT || page->type == WT_PAGE_ROW_INT)) {
		WT_CSTAT_INCR(session, cache_eviction_internal);
		WT_DSTAT_INCR(session, cache_eviction_internal);
	}

	/*
	 * Update the parent and discard the page.
	 */
	if (mod == NULL || !F_ISSET(mod, WT_PM_REC_MASK)) {
		WT_ASSERT(session,
		    exclusive || page->ref->state == WT_REF_LOCKED);

		if (WT_PAGE_IS_ROOT(page))
			__rec_root_update(session);
		else
			__rec_page_clean_update(session, page);

		/* Discard the page. */
		__rec_discard_page(session, page, exclusive);

		WT_CSTAT_INCR(session, cache_eviction_clean);
		WT_DSTAT_INCR(session, cache_eviction_clean);
	} else {
		if (WT_PAGE_IS_ROOT(page))
			__rec_root_update(session);
		else
			WT_ERR(__rec_page_dirty_update(session, page));

		/* Discard the tree rooted in this page. */
		__rec_discard_tree(session, page, exclusive);

		WT_CSTAT_INCR(session, cache_eviction_dirty);
		WT_DSTAT_INCR(session, cache_eviction_dirty);
	}
	if (0) {
err:		/*
		 * If unable to evict this page, release exclusive reference(s)
		 * we've acquired.
		 */
		__rec_excl_clear(session);

		WT_CSTAT_INCR(session, cache_eviction_fail);
		WT_DSTAT_INCR(session, cache_eviction_fail);
	}
	session->excl_next = 0;

	return (ret);
}

/*
 * __rec_root_update  --
 *	Update a root page's reference on eviction (clean or dirty).
 */
static void
__rec_root_update(WT_SESSION_IMPL *session)
{
	session->btree->root_page = NULL;
}

/*
 * __rec_page_clean_update  --
 *	Update a clean page's reference on eviction.
 */
static void
__rec_page_clean_update(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_REF *ref;

	ref = page->ref;

	/*
	 * Update the page's WT_REF structure.  If the page has an address, it's
	 * a disk page; if it has no address, it must be a deleted page that was
	 * re-instantiated (for example, by searching) and never written.
	 */
	ref->page = NULL;
	WT_PUBLISH(ref->state,
	    ref->addr == NULL ? WT_REF_DELETED : WT_REF_DISK);

	WT_UNUSED(session);
}

/*
 * __rec_page_dirty_update --
 *	Update a dirty page's reference on eviction.
 */
static int
__rec_page_dirty_update(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_ADDR *addr;
	WT_PAGE_MODIFY *mod;
	WT_REF *parent_ref;

	mod = page->modify;
	parent_ref = page->ref;

	switch (F_ISSET(mod, WT_PM_REC_MASK)) {
	case WT_PM_REC_EMPTY:				/* Page is empty */
		if (parent_ref->addr != NULL &&
		    __wt_off_page(page->parent, parent_ref->addr)) {
			__wt_free(session, ((WT_ADDR *)parent_ref->addr)->addr);
			__wt_free(session, parent_ref->addr);
		}

		/*
		 * Update the parent to reference an empty page.
		 *
		 * Set the transaction ID to WT_TXN_NONE because the fact that
		 * reconciliation left the page "empty" means there's no older
		 * transaction in the system that might need to see an earlier
		 * version of the page.  It isn't necessary (WT_TXN_NONE is 0),
		 * but it's the right thing to do.
		 *
		 * Publish: a barrier to ensure the structure fields are set
		 * before the state change makes the page available to readers.
		 */
		parent_ref->page = NULL;
		parent_ref->addr = NULL;
		parent_ref->txnid = WT_TXN_NONE;
		WT_PUBLISH(parent_ref->state, WT_REF_DELETED);
		break;
	case WT_PM_REC_REPLACE: 			/* 1-for-1 page swap */
		if (parent_ref->addr != NULL &&
		    __wt_off_page(page->parent, parent_ref->addr)) {
			__wt_free(session, ((WT_ADDR *)parent_ref->addr)->addr);
			__wt_free(session, parent_ref->addr);
		}

		/*
		 * Update the parent to reference the replacement page.
		 *
		 * Publish: a barrier to ensure the structure fields are set
		 * before the state change makes the page available to readers.
		 */
		WT_RET(__wt_calloc(session, 1, sizeof(WT_ADDR), &addr));
		*addr = mod->u.replace;
		mod->u.replace.addr = NULL;
		mod->u.replace.size = 0;

		parent_ref->page = NULL;
		parent_ref->addr = addr;
		WT_PUBLISH(parent_ref->state, WT_REF_DISK);
		break;
	case WT_PM_REC_SPLIT:				/* Page split */
		/*
		 * Update the parent to reference new internal page(s).
		 *
		 * Publish: a barrier to ensure the structure fields are set
		 * before the state change makes the page available to readers.
		 */
		parent_ref->page = mod->u.split;
		WT_PUBLISH(parent_ref->state, WT_REF_MEM);

		/* Clear the reference else discarding the page will free it. */
		mod->u.split = NULL;
		F_CLR(mod, WT_PM_REC_SPLIT);
		break;
	WT_ILLEGAL_VALUE(session);
	}

	return (0);
}

/*
 * __rec_discard_tree --
 *	Discard the tree rooted a page (that is, any pages merged into it),
 * then the page itself.
 */
static void
__rec_discard_tree(WT_SESSION_IMPL *session, WT_PAGE *page, int exclusive)
{
	WT_REF *ref;
	uint32_t i;

	switch (page->type) {
	case WT_PAGE_COL_INT:
	case WT_PAGE_ROW_INT:
		/* For each entry in the page... */
		WT_REF_FOREACH(page, ref, i) {
			if (ref->state == WT_REF_DISK ||
			    ref->state == WT_REF_DELETED)
				continue;
			WT_ASSERT(session,
			    exclusive || ref->state == WT_REF_LOCKED);
			__rec_discard_tree(session, ref->page, exclusive);
		}
		/* FALLTHROUGH */
	default:
		__rec_discard_page(session, page, exclusive);
		break;
	}
}

/*
 * __rec_discard_page --
 *	Discard the page.
 */
static void
__rec_discard_page(WT_SESSION_IMPL *session, WT_PAGE *page, int exclusive)
{
	/* We should never evict the file's current eviction point. */
	WT_ASSERT(session, session->btree->evict_page != page);

	/* Make sure a page is not in the eviction request list. */
	if (!exclusive)
		__wt_evict_list_clr_page(session, page);

	/* Discard the page. */
	__wt_page_out(session, &page);
}

/*
 * __rec_review --
 *	Get exclusive access to the page and review the page and its subtree
 *	for conditions that would block its eviction.
 *
 *	The ref and page arguments may appear to be redundant, because usually
 *	ref->page == page and page->ref == ref.  However, we need both because
 *	(a) there are cases where ref == NULL (e.g., for root page or during
 *	salvage), and (b) we can't safely look at page->ref until we have a
 *	hazard pointer.
 */
static int
__rec_review(WT_SESSION_IMPL *session,
    WT_REF *ref, WT_PAGE *page, int exclusive, int top)
{
	WT_BTREE *btree;
	WT_DECL_RET;
	WT_PAGE_MODIFY *mod;
	WT_PAGE *t;
	WT_TXN *txn;
	uint32_t i;

	btree = session->btree;
	txn = &session->txn;

	/*
	 * Get exclusive access to the page if our caller doesn't have the tree
	 * locked down.
	 */
	if (!exclusive)
		WT_RET(__hazard_exclusive(session, ref, top));

	/*
	 * Recurse through the page's subtree: this happens first because we
	 * have to write pages in depth-first order, otherwise we'll dirty
	 * pages after we've written them.
	 */
	if (page->type == WT_PAGE_COL_INT || page->type == WT_PAGE_ROW_INT)
		WT_REF_FOREACH(page, ref, i)
			switch (ref->state) {
			case WT_REF_DISK:		/* On-disk */
			case WT_REF_DELETED:		/* On-disk, deleted */
				break;
			case WT_REF_MEM:		/* In-memory */
				WT_RET(__rec_review(
				    session, ref, ref->page, exclusive, 0));
				break;
			case WT_REF_EVICT_WALK:		/* Walk point */
			case WT_REF_EVICT_FORCE:	/* Forced evict */
			case WT_REF_LOCKED:		/* Being evicted */
			case WT_REF_READING:		/* Being read */
				return (EBUSY);
			}

	/*
	 * If the file is being checkpointed, we cannot evict dirty pages,
	 * because that may free a page that appears on an internal page in the
	 * checkpoint.  Don't rely on new updates being skipped by the
	 * transaction used for transaction reads: (1) there are paths that
	 * dirty pages for artificial reasons; (2) internal pages aren't
	 * transactional; and (3) if an update was skipped during the
	 * checkpoint (leaving the page dirty), then rolled back, we could
	 * still successfully overwrite a page and corrupt the checkpoint.
	 *
	 * Further, even for clean pages, the checkpoint's reconciliation of an
	 * internal page might race with us as we evict a child in the page's
	 * subtree.
	 *
	 * One half of that test is in the reconciliation code: the checkpoint
	 * thread waits for eviction-locked pages to settle before determining
	 * their status.  The other half of the test is here: after acquiring
	 * the exclusive eviction lock on a page, confirm no page in the page's
	 * stack of pages from the root is being reconciled in a checkpoint.
	 * This ensures we either see the checkpoint-walk state here, or the
	 * reconciliation of the internal page sees our exclusive lock on the
	 * child page and waits until we're finished evicting the child page
	 * (or give up if eviction isn't possible).
	 *
	 * We must check the full stack (we might be attempting to evict a leaf
	 * page multiple levels beneath the internal page being reconciled as
	 * part of the checkpoint, and  all of the intermediate nodes are being
	 * merged into the internal page).
	 *
	 * There's no simple test for knowing if a page in our page stack is
	 * involved in a checkpoint.  The internal page's checkpoint-walk flag
	 * is the best test, but it's not set anywhere for the root page, it's
	 * not a complete test.
	 *
	 * Quit for any page that's not a simple, in-memory page.  (Almost the
	 * same as checking for the checkpoint-walk flag.  I don't think there
	 * are code paths that change the page's status from checkpoint-walk,
	 * but these races are hard enough I'm not going to proceed if there's
	 * anything other than a vanilla, in-memory tree stack.)  Climb until
	 * we find a page which can't be merged into its parent, and failing if
	 * we never find such a page.
	 */
	if (btree->checkpointing && __wt_page_is_modified(page))
		return (EBUSY);

	if (btree->checkpointing && top)
		for (t = page->parent;; t = t->parent) {
			if (t == NULL || t->ref == NULL)	/* root */
				return (EBUSY);
			if (t->ref->state != WT_REF_MEM)	/* scary */
				return (EBUSY);
			if (t->modify == NULL ||		/* not merged */
			    !F_ISSET(t->modify, WT_PM_REC_EMPTY |
			    WT_PM_REC_SPLIT | WT_PM_REC_SPLIT_MERGE))
				break;
		}

	/*
	 * Fail if any page in the top-level page's subtree won't be merged into
	 * its parent, the page that cannot be merged must be evicted first.
	 * The test is necessary but should not fire much: the eviction code is
	 * biased for leaf pages, an internal page shouldn't be selected for
	 * eviction until its children have been evicted.
	 *
	 * We have to write dirty pages to know their final state, a page marked
	 * empty may have had records added since reconciliation, a page marked
	 * split may have had records deleted and no longer need to split.
	 * Split-merge pages are the exception: they can never be change into
	 * anything other than a split-merge page and are merged regardless of
	 * being clean or dirty.
	 *
	 * Writing the page is expensive, do a cheap test first: if it doesn't
	 * appear a subtree page can be merged, quit.  It's possible the page
	 * has been emptied since it was last reconciled, and writing it before
	 * testing might be worthwhile, but it's more probable we're attempting
	 * to evict an internal page with live children, and that's a waste of
	 * time.
	 */
	mod = page->modify;
	if (!top && (mod == NULL || !F_ISSET(mod,
	    WT_PM_REC_EMPTY | WT_PM_REC_SPLIT | WT_PM_REC_SPLIT_MERGE)))
		return (EBUSY);

	/*
	 * If the page is dirty and can possibly change state, write it so we
	 * know the final state.
	 */
	if (__wt_page_is_modified(page) &&
	    !F_ISSET(mod, WT_PM_REC_SPLIT_MERGE)) {
		ret = __wt_rec_write(session, page,
		    NULL, WT_EVICTION_SERVER_LOCKED | WT_SKIP_UPDATE_QUIT);

		/*
		 * Update the page's modification reference, reconciliation
		 * might have changed it.
		 */
		mod = page->modify;

		/* If there are unwritten changes on the page, give up. */
		if (ret == EBUSY) {
			WT_VERBOSE_RET(session, evict,
			    "eviction failed, reconciled page not clean");

			/*
			 * A pathological case: if we're the oldest transaction
			 * in the system and we're stuck trying to find space,
			 * abort the transaction to give up all hazard
			 * references before trying again.
			 */
			if (F_ISSET(txn, TXN_RUNNING) &&
			    __wt_txn_am_oldest(session) &&
			    ++txn->eviction_fails >= 100) {
				txn->eviction_fails = 0;
				ret = WT_DEADLOCK;
				WT_CSTAT_INCR(session, txn_fail_cache);
			}

			/* 
			 * We may be able to discard any "update" memory the
			 * page no longer needs.
			 */
			switch (page->type) {
			case WT_PAGE_COL_FIX:
			case WT_PAGE_COL_VAR:
				__wt_col_leaf_obsolete(session, page);
				break;
			case WT_PAGE_ROW_LEAF:
				__wt_row_leaf_obsolete(session, page);
				break;
			}
		}
		WT_RET(ret);

		WT_ASSERT(session, __wt_page_is_modified(page) == 0);
		txn->eviction_fails = 0;
	}

	/*
	 * Repeat the test: fail if any page in the top-level page's subtree
	 * won't be merged into its parent.
	 */
	if (!top && (mod == NULL || !F_ISSET(mod,
	    WT_PM_REC_EMPTY | WT_PM_REC_SPLIT | WT_PM_REC_SPLIT_MERGE)))
		return (EBUSY);
	return (0);
}

/*
 * __rec_excl_clear --
 *	Discard exclusive access and return a page's subtree to availability.
 */
static void
__rec_excl_clear(WT_SESSION_IMPL *session)
{
	WT_REF *ref;
	uint32_t i;

	for (i = 0; i < session->excl_next; ++i) {
		if ((ref = session->excl[i]) == NULL)
			break;
		WT_ASSERT(session,
		    ref->state == WT_REF_LOCKED && ref->page != NULL);
		ref->state = WT_REF_MEM;
	}
}

/*
 * __hazard_exclusive --
 *	Request exclusive access to a page.
 */
static int
__hazard_exclusive(WT_SESSION_IMPL *session, WT_REF *ref, int top)
{
	/*
	 * Make sure there is space to track exclusive access so we can unlock
	 * to clean up.
	 */
	if (session->excl_next * sizeof(WT_REF *) == session->excl_allocated)
		WT_RET(__wt_realloc(session, &session->excl_allocated,
		    (session->excl_next + 50) * sizeof(WT_REF *),
		    &session->excl));

	/*
	 * Hazard pointers are acquired down the tree, which means we can't
	 * deadlock.
	 *
	 * Request exclusive access to the page.  The top-level page should
	 * already be in the locked state, lock child pages in memory.
	 * If another thread already has this page, give up.
	 */
	if (!top && !WT_ATOMIC_CAS(ref->state, WT_REF_MEM, WT_REF_LOCKED))
		return (EBUSY);	/* We couldn't change the state. */
	WT_ASSERT(session, ref->state == WT_REF_LOCKED);

	session->excl[session->excl_next++] = ref;

	/* Check for a matching hazard pointer. */
	if (__wt_page_hazard_check(session, ref->page) == NULL)
		return (0);

	WT_DSTAT_INCR(session, cache_eviction_hazard);
	WT_CSTAT_INCR(session, cache_eviction_hazard);

	WT_VERBOSE_RET(
	    session, evict, "page %p hazard request failed", ref->page);
	return (EBUSY);
}
