/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __ref_index_slot --
 *     Return the page's index and slot for a reference.
 */
static WT_INLINE void
__ref_index_slot(WT_SESSION_IMPL *session, WT_REF *ref, WT_PAGE_INDEX **pindexp, uint32_t *slotp)
{
    WT_PAGE_INDEX *pindex;
    WT_REF **p, **start, **stop, **t;
    uint64_t sleep_usecs, yield_count;
    uint32_t entries, slot;

    /*
     * If we don't find our reference, the page split and our home pointer references the wrong
     * page. When internal pages split, their WT_REF structure home values are updated; yield and
     * wait for that to happen.
     */
    for (sleep_usecs = yield_count = 0;;) {
        /*
         * Copy the parent page's index value: the page can split at any time, but the index's value
         * is always valid, even if it's not up-to-date.
         */
        WT_INTL_INDEX_GET(session, ref->home, pindex);
        entries = pindex->entries;

        /*
         * Use the page's reference hint: it should be correct unless there was a split or delete in
         * the parent before our slot. If the hint is wrong, it can be either too big or too small,
         * but often only by a small amount. Search up and down the index starting from the hint.
         *
         * It's not an error for the reference hint to be wrong, it just means the first retrieval
         * (which sets the hint for subsequent retrievals), is slower.
         */
        slot = ref->pindex_hint;
        if (slot >= entries)
            slot = entries - 1;
        if (pindex->index[slot] == ref)
            goto found;
        for (start = &pindex->index[0], stop = &pindex->index[entries - 1],
            p = t = &pindex->index[slot];
             p > start || t < stop;) {
            if (p > start && *--p == ref) {
                slot = (uint32_t)(p - start);
                goto found;
            }
            if (t < stop && *++t == ref) {
                slot = (uint32_t)(t - start);
                goto found;
            }
        }
        /*
         * We failed to get the page index and slot reference, yield before retrying, and if we've
         * yielded enough times, start sleeping so we don't burn CPU to no purpose.
         */
        __wt_spin_backoff(&yield_count, &sleep_usecs);
        WT_STAT_CONN_INCRV(session, page_index_slot_ref_blocked, sleep_usecs);
    }

found:
    WT_ASSERT(session, pindex->index[slot] == ref);
    *pindexp = pindex;
    *slotp = slot;
}

/*
 * __ref_ascend --
 *     Ascend the tree one level. If `pindexp` is not null, fill in both `pindexp` and `slotp`.
 */
static WT_INLINE void
__ref_ascend(WT_SESSION_IMPL *session, WT_REF **refp, WT_PAGE_INDEX **pindexp, uint32_t *slotp)
{
    WT_REF *parent_ref, *ref;

    /*
     * Ref points to the first/last slot on an internal page from which we are ascending the tree,
     * moving to the parent page. This is tricky because the internal page we're on may be splitting
     * into its parent. Find a stable configuration where the page we start from and the page we're
     * moving to are connected. The tree eventually stabilizes into that configuration, keep trying
     * until we succeed.
     */
    for (ref = *refp;;) {
        /*
         * Find our parent slot on the next higher internal page, the slot from which we move to a
         * next/prev slot, checking that we haven't reached the root.
         */
        parent_ref = ref->home->pg_intl_parent_ref;
        if (__wt_ref_is_root(parent_ref))
            break;
        if (pindexp)
            __ref_index_slot(session, parent_ref, pindexp, slotp);

        /*
         * There's a split race when a cursor moving forwards through
         * the tree ascends the tree. If we're splitting an internal
         * page into its parent, we move the WT_REF structures and
         * then update the parent's page index before updating the split
         * page's page index, and it's not an atomic update. A thread
         * can read the split page's original page index and then read
         * the parent page's replacement index.
         *
         * This can create a race for next-cursor movements.
         *
         * For example, imagine an internal page with 3 child pages,
         * with the namespaces a-f, g-h and i-j; the first child page
         * splits. The parent starts out with the following page-index:
         *
         *	| ... | a | g | i | ... |
         *
         * which changes to this:
         *
         *	| ... | a | c | e | g | i | ... |
         *
         * The split page starts out with the following page-index:
         *
         *	| a | b | c | d | e | f |
         *
         * Imagine a cursor finishing the 'f' part of the namespace that
         * starts its ascent to the parent's 'a' slot. Then the page
         * splits and the parent page's page index is replaced. If the
         * cursor then searches the parent's replacement page index for
         * the 'a' slot, it finds it and then increments to the slot
         * after the 'a' slot, the 'c' slot, and then it incorrectly
         * repeats its traversal of part of the namespace.
         *
         * This function takes a WT_REF argument which is the page from
         * which we start our ascent. If the parent's slot we find in
         * our search doesn't point to the same page as that initial
         * WT_REF, there's a race and we start over again.
         */
        if (ref->home == parent_ref->page)
            break;
    }

    *refp = parent_ref;
}

/*
 * __split_prev_race --
 *     Check for races when descending the tree during a previous-cursor walk.
 */
static WT_INLINE bool
__split_prev_race(WT_SESSION_IMPL *session, WT_REF *ref, WT_PAGE_INDEX **pindexp)
{
    WT_PAGE_INDEX *pindex;

    /*
     * Handle a cursor moving backwards through the tree or setting up at the end of the tree. We're
     * passed the child page into which we're descending, and the parent page's page-index we used
     * to find that child page.
     *
     * When splitting an internal page into its parent, we move the split pages WT_REF structures,
     * then update the parent's page index, then update the split page's page index, and nothing is
     * atomic. A thread can read the parent page's replacement page index and then the split page's
     * original index, or vice-versa, and either change can cause a cursor moving backwards through
     * the tree to skip pages.
     *
     * This isn't a problem for a cursor setting up at the start of the tree or moving forward
     * through the tree because we do right-hand splits on internal pages and the initial part of
     * the split page's namespace won't change as part of a split (in other words, a thread reading
     * the parent page's and split page's indexes will move to the same slot no matter what order of
     * indexes are read.
     *
     * Acquire the child's page index, then confirm the parent's page index hasn't changed, to check
     * for reading an old version of the parent's page index and then reading a new version of the
     * child's page index.
     */
    WT_INTL_INDEX_GET(session, ref->page, pindex);
    if (__wt_split_descent_race(session, ref, *pindexp))
        return (true);

    /*
     * That doesn't check if we read a new version of parent's page index
     * and then an old version of the child's page index. For example, if
     * a thread were in a newly created split page subtree, the split
     * completes into the parent before the thread reads it and descends
     * into the child (where the split hasn't yet completed).
     *
     * Imagine an internal page with 3 child pages, with the namespaces a-f,
     * g-h and i-j; the first child page splits. The parent starts out with
     * the following page-index:
     *
     *	| ... | a | g | i | ... |
     *
     * The split page starts out with the following page-index:
     *
     *	| a | b | c | d | e | f |
     *
     * The first step is to move the c-f ranges into a new subtree, so, for
     * example we might have two new internal pages 'c' and 'e', where the
     * new 'c' page references the c-d namespace and the new 'e' page
     * references the e-f namespace. The top of the subtree references the
     * parent page, but until the parent's page index is updated, threads in
     * the subtree won't be able to ascend out of the subtree. However, once
     * the parent page's page index is updated to this:
     *
     *	| ... | a | c | e | g | i | ... |
     *
     * threads in the subtree can ascend into the parent. Imagine a cursor
     * in the c-d part of the namespace that ascends to the parent's 'c'
     * slot. It would then decrement to the slot before the 'c' slot, the
     * 'a' slot.
     *
     * The previous-cursor movement selects the last slot in the 'a' page;
     * if the split page's page-index hasn't been updated yet, it selects
     * the 'f' slot, which is incorrect. Once the split page's page index is
     * updated to this:
     *
     *	| a | b |
     *
     * the previous-cursor movement will select the 'b' slot, which is
     * correct.
     *
     * If the last slot on the page no longer points to the current page as
     * its "home", the page is being split and part of its namespace moved,
     * restart. (We probably don't have to restart, I think we could spin
     * until the page-index is updated, but I'm not willing to debug that
     * one if I'm wrong.)
     */
    if (pindex->index[pindex->entries - 1]->home != ref->page)
        return (true);

    *pindexp = pindex;
    return (false);
}

/*
 * __tree_walk_internal --
 *     Move to the next/previous page in the tree, skipping pages in the WT_REF_DELETED state and
 *     for other reasons. Those other reasons are generally controlled by the flags passed in to
 *     this function.
 */
static WT_INLINE int
__tree_walk_internal(WT_SESSION_IMPL *session, WT_REF **refp, uint64_t *walkcntp,
  int (*skip_func)(WT_SESSION_IMPL *, WT_REF *, void *, bool, bool *), void *func_cookie,
  uint32_t flags)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_PAGE_INDEX *pindex;
    WT_REF *couple, *ref, *ref_orig;
    WT_REF_STATE current_state;
    uint64_t restart_sleep, restart_yield;
    uint32_t slot;
    bool empty_internal, prev, skip;

    btree = S2BT(session);
    pindex = NULL;
    restart_sleep = restart_yield = 0;
    empty_internal = false;

    /* Ensure we have a snapshot to check visibility or we only check global visibility. */
    WT_ASSERT(session, LF_ISSET(WT_READ_VISIBLE_ALL) || F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT));

    /*
     * Historically, all tree walks skipped deleted pages. There are now some exceptions to this:
     * Rollback to stable, and column store append. Rather than add the read-see-deleted flag to
     * every single tree walk call, we hide these pages unless:
     *
     * 1. We detect that rollback to stable is in progress
     * 2. Callers opt into seeing these pages with the read-see-deleted flag.
     *
     * Ideally, rollback to stable would also use the read-see-deleted flag but it uses cursor->next
     * and cursor->prev, which don't have flags.
     */
    if (!F_ISSET(session, WT_SESSION_ROLLBACK_TO_STABLE) && !LF_ISSET(WT_READ_SEE_DELETED))
        LF_SET(WT_READ_SKIP_DELETED);

    /*
     * !!!
     * Fast-truncate does not currently work for FLCS trees.
     */
    if (btree->type == BTREE_COL_FIX)
        LF_CLR(WT_READ_TRUNCATE);

    prev = LF_ISSET(WT_READ_PREV) ? 1 : 0;

    /*
     * There are multiple reasons and approaches to walking the in-memory
     * tree:
     *
     * (1) finding pages to evict (the eviction server);
     * (2) writing just dirty leaves or internal nodes (checkpoint);
     * (3) discarding pages (close);
     * (4) truncating pages in a range (fast truncate);
     * (5) skipping pages based on outside information (compaction);
     * (6) cursor scans (applications).
     *
     * Except for cursor scans and compaction, the walk is limited to the
     * cache, no pages are read.  In all cases, hazard pointers protect the
     * walked pages from eviction.
     *
     * Walks use hazard-pointer coupling through the tree and that's OK
     * (hazard pointers can't deadlock, so there's none of the usual
     * problems found when logically locking up a btree).  If the eviction
     * thread tries to evict the active page, it fails because of our
     * hazard pointer.  If eviction tries to evict our parent, that fails
     * because the parent has a child page that can't be discarded.  We do
     * play one game: don't couple up to our parent and then back down to a
     * new leaf, couple to the next page to which we're descending, it
     * saves a hazard-pointer swap for each cursor page movement.
     *
     * The hazard pointer on the original location is held until the end of
     * the movement, in case we have to restart the movement. Take a copy
     * of any held page and clear the return value (it makes future error
     * handling easier).
     */
    couple = NULL;
    ref_orig = *refp;
    *refp = NULL;

    /*
     * Tree walks are special: they look inside page structures that splits may want to free.
     */
    WT_ENTER_PAGE_INDEX(session);

    /* If no page is active, begin a walk from the start/end of the tree. */
    if ((ref = ref_orig) == NULL) {
        if (0) {
restart:
            /*
             * Yield before retrying, and if we've yielded enough times, start sleeping so we don't
             * burn CPU to no purpose.
             */
            __wt_spin_backoff(&restart_yield, &restart_sleep);

            WT_ERR(__wt_page_release(session, couple, flags));
            couple = NULL;
        }

        if ((ref = ref_orig) == NULL) {
            ref = &btree->root;
            WT_INTL_INDEX_GET(session, ref->page, pindex);
            slot = prev ? pindex->entries - 1 : 0;
            goto descend;
        }
    }

    /*
     * If the active page was the root, we've reached the walk's end; we only get here if we've
     * returned the root to our caller, so we're holding no hazard pointers.
     */
    if (__wt_ref_is_root(ref))
        goto done;

    /* Figure out the current slot in the WT_REF array. */
    __ref_index_slot(session, ref, &pindex, &slot);

    for (;;) {
        /*
         * If we're at the last/first slot on the internal page, return it in post-order traversal.
         * Otherwise move to the next/prev slot and left/right-most element in that subtree.
         */
        while ((prev && slot == 0) || (!prev && slot == pindex->entries - 1)) {
            /* Ascend to the parent. */
            __ref_ascend(session, &ref, &pindex, &slot);

            /*
             * If at the root and returning internal pages, return the root page, otherwise we're
             * done.
             */
            if (__wt_ref_is_root(ref)) {
                if (!LF_ISSET(WT_READ_SKIP_INTL)) {
                    *refp = ref;
                    WT_ASSERT(session, ref != ref_orig);
                }
                goto done;
            }

            /*
             * If we got all the way through an internal page and all of the child pages were
             * deleted, mark it for eviction.
             */
            if (empty_internal) {
                __wt_page_evict_soon(session, ref);
                empty_internal = false;
            }

            /* Optionally return internal pages. */
            if (LF_ISSET(WT_READ_SKIP_INTL))
                continue;

            /*
             * Swap our previous hazard pointer for the page we'll return.
             *
             * Not-found is an expected return, as eviction might have been attempted. Restart is
             * not expected, our parent WT_REF should not have split.
             */
            WT_ERR_NOTFOUND_OK(
              __wt_page_swap(session, couple, ref, WT_READ_NOTFOUND_OK | flags), true);
            if (ret == 0) {
                /* Success, "couple" released. */
                couple = NULL;
                *refp = ref;
                WT_ASSERT(session, ref != ref_orig);

                if (__wt_session_prefetch_check(session, ref))
                    WT_ERR(__wti_btree_prefetch(session, ref));

                goto done;
            }

            /* ret == WT_NOTFOUND, an expected error.  Continue with "couple" unchanged. */
        }

        if (prev)
            --slot;
        else
            ++slot;

        if (walkcntp != NULL)
            ++*walkcntp;

        for (;;) {
descend:
            /*
             * Get a reference, setting the reference hint if it's wrong (used when we continue the
             * walk). We don't always update the hints when splitting, it's expected for them to be
             * incorrect in some workloads.
             */
            ref = pindex->index[slot];
            if (ref->pindex_hint != slot)
                ref->pindex_hint = slot;

            /*
             * If we see any child states other than deleted, the page isn't empty.
             */
            current_state = WT_REF_GET_STATE(ref);
            if (current_state != WT_REF_DELETED && !LF_ISSET(WT_READ_TRUNCATE))
                empty_internal = false;

            if (LF_ISSET(WT_READ_CACHE)) {
                /*
                 * Only look at unlocked pages in memory.
                 */
                if (LF_ISSET(WT_READ_NO_WAIT) && current_state != WT_REF_MEM)
                    break;
            } else if (LF_ISSET(WT_READ_TRUNCATE)) {
                /*
                 * If deleting a range, try to delete the page without instantiating it. (Note this
                 * test follows the check to skip the page entirely if it's already deleted.)
                 */
                WT_ERR(__wti_delete_page(session, ref, &skip));
                if (skip)
                    break;
                empty_internal = false;
            } else if (LF_ISSET(WT_READ_SKIP_DELETED) && current_state == WT_REF_DELETED) {
                /*
                 * Try to skip deleted pages visible to us.
                 */
                if (__wti_delete_page_skip(session, ref, LF_ISSET(WT_READ_VISIBLE_ALL)))
                    break;
            }

            /* See if our caller wants to skip this page. */
            if (skip_func != NULL) {
                WT_ERR(skip_func(session, ref, func_cookie, LF_ISSET(WT_READ_VISIBLE_ALL), &skip));
                if (skip)
                    break;
            }

            ret = __wt_page_swap(
              session, couple, ref, WT_READ_NOTFOUND_OK | WT_READ_RESTART_OK | flags);
            if (ret == 0) {
                /* Success, so "couple" has been released. */
                couple = NULL;

                if (__wt_session_prefetch_check(session, ref))
                    WT_ERR(__wti_btree_prefetch(session, ref));

                /* Return leaf pages to our caller. */
                if (F_ISSET(ref, WT_REF_FLAG_LEAF)) {
                    *refp = ref;
                    WT_ASSERT(session, ref != ref_orig);
                    goto done;
                }

                /* Set the new "couple" value. */
                couple = ref;

                /* Configure traversal of any internal page. */
                empty_internal = true;
                if (prev) {
                    if (__split_prev_race(session, ref, &pindex))
                        goto restart;
                    slot = pindex->entries - 1;
                } else {
                    WT_INTL_INDEX_GET(session, ref->page, pindex);
                    slot = 0;
                }
                continue;
            }

            /*
             * Not-found is an expected return when walking only in-cache pages, or if we see a
             * deleted page.
             *
             * An expected error, so "couple" is unchanged.
             */
            if (ret == WT_NOTFOUND) {
                WT_STAT_CONN_INCR(session, cache_eviction_walk_leaf_notfound);
                WT_NOT_READ(ret, 0);
                break;
            }

            /*
             * The page we're moving to might have split, in which case restart the movement.
             *
             * An expected error, so "couple" is unchanged.
             */
            if (ret == WT_RESTART)
                goto restart;

            /* Unexpected error, so "couple" was released. */
            couple = NULL;
            goto err;
        }
    }

done:
err:
    WT_TRET(__wt_page_release(session, couple, flags));
    WT_TRET(__wt_page_release(session, ref_orig, flags));
    WT_LEAVE_PAGE_INDEX(session);
    return (ret);
}

/*
 * __wt_tree_walk --
 *     Move to the next/previous page in the tree.
 */
int
__wt_tree_walk(WT_SESSION_IMPL *session, WT_REF **refp, uint32_t flags)
{
    return (__tree_walk_internal(session, refp, NULL, NULL, NULL, flags));
}

/*
 * __wt_tree_walk_count --
 *     Move to the next/previous page in the tree, tracking how many references were visited to get
 *     there.
 */
int
__wt_tree_walk_count(WT_SESSION_IMPL *session, WT_REF **refp, uint64_t *walkcntp, uint32_t flags)
{
    return (__tree_walk_internal(session, refp, walkcntp, NULL, NULL, flags));
}

/*
 * __wt_tree_walk_custom_skip --
 *     Walk the tree calling a custom function to decide whether to skip refs.
 */
int
__wt_tree_walk_custom_skip(WT_SESSION_IMPL *session, WT_REF **refp,
  int (*skip_func)(WT_SESSION_IMPL *, WT_REF *, void *, bool, bool *), void *func_cookie,
  uint32_t flags)
{
    return (__tree_walk_internal(session, refp, NULL, skip_func, func_cookie, flags));
}

/*
 * __tree_walk_skip_count_callback --
 *     Optionally skip leaf pages. When the skip-leaf-count variable is non-zero, skip some count of
 *     leaf pages, then take the next leaf page we can. The reason to do some of this work here, is
 *     because we can look at the cell and know it's a leaf page without reading it into memory. If
 *     this page is disk-based, crack the cell to figure out it's a leaf page without reading it.
 */
static int
__tree_walk_skip_count_callback(
  WT_SESSION_IMPL *session, WT_REF *ref, void *context, bool visible_all, bool *skipp)
{
    uint64_t *skipleafcntp;

    skipleafcntp = (uint64_t *)context;
    WT_ASSERT(session, skipleafcntp != NULL);

    /*
     * Skip deleted pages visible to us.
     */
    if (WT_REF_GET_STATE(ref) == WT_REF_DELETED &&
      __wti_delete_page_skip(session, ref, visible_all))
        *skipp = true;
    else if (*skipleafcntp > 0 && F_ISSET(ref, WT_REF_FLAG_LEAF)) {
        --*skipleafcntp;
        *skipp = true;
    } else
        *skipp = false;
    return (0);
}

/*
 * __wti_tree_walk_skip --
 *     Move to the next/previous page in the tree, skipping a certain number of leaf pages before
 *     returning.
 */
int
__wti_tree_walk_skip(WT_SESSION_IMPL *session, WT_REF **refp, uint64_t *skipleafcntp)
{
    /*
     * Optionally skip leaf pages, the second half. The tree-walk function didn't have an on-page
     * cell it could use to figure out if the page was a leaf page or not, it had to acquire the
     * hazard pointer and look at the page. The tree-walk code never acquires a hazard pointer on a
     * leaf page without returning it, and it's not trivial to change that. So, the tree-walk code
     * returns all leaf pages here and we deal with decrementing the count.
     */
    do {
        WT_RET(__tree_walk_internal(session, refp, NULL, __tree_walk_skip_count_callback,
          skipleafcntp, WT_READ_NO_GEN | WT_READ_SKIP_INTL | WT_READ_WONT_NEED));

        /*
         * The walk skipped internal pages, any page returned must be a leaf page.
         */
        if (*skipleafcntp > 0)
            --*skipleafcntp;
    } while (*skipleafcntp > 0);

    return (0);
}

/*
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 *
 * Below is a set of functions for computing the normalized position of a page and restoring a page
 * from its normalized position. These functions are used by the eviction server for the sake of
 * not holding the hazard pointer for longer than necessary. Another user is "partition cursor".
 *
 * Normalized position is a number in the range of 0 .. 1 that represents a page's position across
 * all pages. It's primary design goal is to be cheap rather than precise. It works best when the
 * tree is perfectly balanced, i.e. all internal pages at the same level have the same number of
 * children and the depth of all leaf pages is the same. In practice, the tree is not perfect, so
 * the normalized position is imprecise. However, it's totally fine for the eviction server because
 * it only uses an approximate position in the tree to continue to walk from.
 * Even when using a hazard pointer, page splits can shift data so that some pages or sub-trees can
 * be skipped an eviction pass.
 *
 * Eviction wants to be as non-intrusive as possible and never loads pages into memory, while
 * Partition Cursor can load pages or wait for them to be unlocked. The behavior is controlled by
 * the flags passed to the functions. The overall set of flags is quite complex.
 * To simplify the use of this machinery, two helper functions are provided:
 *   - __wt_page_from_npos_for_eviction
 *   - __wt_page_from_npos_for_read
 *
 *    === Detailed description.
 *
 * Normalized position is a number in the range of 0 .. 1 defining a page's position in the tree.
 * In fact, each page occupies a range of positions. For example, if there are 5 pages then
 * positions of pages are:
 *   - [0.0 .. 0.2) -> page 0
 *   - [0.2 .. 0.4) -> page 1
 *   - [0.4 .. 0.6) -> page 2
 *   - [0.6 .. 0.8) -> page 3
 *   - [0.8 .. 1.0] -> page 4
 * The starting point is inclusive, the ending point is exclusive.
 *
 * When calculating a page's position, the returned result is always in the range of 0 .. 1. Because
 * of that, any number outside of this range can be used as an invalid position when storing it.
 *
 * When retrieving a page, any number below 0 will lead to the first page, any number above 1 will
 * lead to the last page. This has useful consequences discussed below.
 *
 *    === Finding a page from its normalized position.
 *
 * If all leaf pages are attached straight to the root, then finding a page from its normalized
 * position is just as simple as multiplying it by the number of leaf pages and using the integer
 * part of the result as the page's index.
 *
 * If there are multiple levels, then the process is similar: the integer part is used as an index
 * at the current level, and the fractional part is used as a normalized position at the next level.
 *
 * This process is repeated until we reach a leaf page.
 *
 * The remaining fractional part at the leaf page can be potentially used to find an exact key
 * on the page. This is not implemented since there's no need for it.
 *
 *    === Calculating a page's normalized position.
 *
 * As opposed to finding a page from its normalized position, the process goes back from the page
 * up to the root. The process is a reverse of the finding process.
 *
 * A nuance is that because there is a whole range of numbers corresponding to a page, the user
 * can choose a starting position within the page. Say, numbers closer to 0 will point to somewhere
 * closer to the beginning of the page, and numbers closer to 1 will point close to the end.
 *
 * If there's only one level, then the normalized position is just the page's index (plus fractional
 * starting point) divided by the number of pages at the parent level.
 *
 * If there are multiple levels, the process is repeated until we reach the root with starting
 * point being whatever has been calculated at the previous level.
 *
 * NOTE that starting points 0 and 1 are corner cases and can lead you to an adjacent page when
 * retrieving a page because of rounding errors.
 * To reliably get back to the same page, the best starting point is 0.5.
 *
 * A useful side effect is that using starting numbers outside of 0 .. 1 range will lead you to
 * adjacent pages. This can be used to iterate over pages without storing any hazard pointers.
 * An example of this can be found in test.
 *
 *   === Precision considerations.
 *
 * Because the precision of the position if affected by tree's structure, it can be used to quantify
 * the shape of the tree. The integral difference of all page's normalized positions and their
 * actual positions can be used to estimate the tree's quality.
 *
 * Note that the tree shape in memory can significantly diverge from the tree shape on disk.
 *
 *   === How many pages can be addressed by a double precision number?
 *
 * The maximum number of pages that can be addressed by a double is roughly 2^53 = ~ 10^16
 * (where 53 is the number of bits in a double mantissa).
 * We have multiple orders of magnitude spare by now.
 *
 * For distributed storage it still can be not enough (the dataset size can exceed
 * petabytes or exabytes), then we can shift to using 64-bit or 128-bit fixed-point numbers.
 *
 */

/*
 * !!!
 * __wt_page_npos --
 *     Get the page's normalized position in the tree.
 *     - If `path_str_offsetp` is set, return a string representation of the page's path.
 *     - `start` is a position within the leaf page: 0 .. 1.
 *       * When calculating a leaf page's position, use 0.5 to get the middle of the page.
 *       * 0 and 1 are corner cases and can lead you to an adjacent page.
 *       * Numbers outside of 0 .. 1 range will lead you to a prev/next page.
 */
double
__wt_page_npos(WT_SESSION_IMPL *session, WT_REF *ref, double start, char *path_str,
  size_t *path_str_offsetp, size_t path_str_sz_max)
{
    WT_PAGE_INDEX *pindex;
    double npos;
    uint32_t entries, slot;
    int unused = 1; /* WT_UNUSED(snprintf) is cooked in GCC */

    npos = start;
    if (path_str)
        *path_str_offsetp = 0;

    WT_ENTER_PAGE_INDEX(session);
    while (!__wt_ref_is_root(ref)) {
        slot = UINT32_MAX; /* We get this invalid value in case of error */
        __ref_index_slot(session, ref, &pindex, &slot);
        entries = pindex->entries;
        /*
         * Depending on the implementation, `__ref_index_slot` might return an error or `slot`
         * outside of range. Check for `slot < entries` ensures that it's a valid number. If it's
         * not, then just skip the adjustment: the resulting number will be wrong but still within
         * the range of the current page. Alternatively, could assign it any "reasonable" estimate
         * like 0.5 or 0 / 1 depending on the walk direction.
         */
        if (slot < entries)
            npos = (slot + npos) / entries;
        if (path_str)
            WT_UNUSED(unused = __wt_snprintf_len_incr(&path_str[*path_str_offsetp],
                        path_str_sz_max - *path_str_offsetp, path_str_offsetp,
                        "[%" PRIu32 "/%" PRIu32 "]", slot, entries));
        __ref_ascend(session, &ref, NULL, NULL);
    }
    WT_LEAVE_PAGE_INDEX(session);

    if (path_str) {
        path_str[*path_str_offsetp] = 0;
    }

    return (WT_CLAMP(npos, 0.0, 1.0));
}

/*
 * !!!
 * __find_closest_leaf --
 *     Find the closest suitable page according to flags.
 *     - It should not be deleted.
 *     - If WT_READ_CACHE is set, the page should be in memory.
 *     - If the initial ref is to a good page, it will be returned.
 *     - If the initial ref is null, it does nothing.
 */
static int
__find_closest_leaf(WT_SESSION_IMPL *session, WT_REF **refp, uint32_t flags)
{
    if (*refp == NULL || F_ISSET(*refp, WT_REF_FLAG_LEAF))
        return (0);
    LF_SET(WT_READ_SKIP_INTL);
    return (__wt_tree_walk(session, refp, flags));
}

/*
 * __page_from_npos_internal --
 *     Go to a leaf page given its normalized position. Note that this function can return a "bad"
 *     page (deleted, locked, etc). The caller of this function should walk the tree to find a
 *     suitable page.
 *
 * NOTE: Must be called within WT_WITH_PAGE_INDEX or WT_ENTER_PAGE_INDEX
 */
static int
__page_from_npos_internal(WT_SESSION_IMPL *session, WT_REF **refp, uint32_t flags, double npos)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_PAGE *page;
    WT_PAGE_INDEX *pindex;
    WT_REF *current, *descent;
    double npos_local;
    int idx, entries;
    bool eviction;

    *refp = NULL;

    btree = S2BT(session);
    current = NULL;
    /*
     * This function is called by eviction to find a page in the cache. That case is indicated by
     * the WT_READ_CACHE flag. Ordinary lookups in a tree will read pages into cache as needed.
     */
    eviction = LF_ISSET(WT_READ_CACHE);

restart: /* Restart the search from the root. */

    /* Search the internal pages of the tree. */
    current = &btree->root;
    npos_local = npos;
    for (;;) {
        if (F_ISSET(current, WT_REF_FLAG_LEAF))
            goto done;

        /* The entire for loop's body is for INTERNAL pages only */

        page = current->page;
        WT_INTL_INDEX_GET(session, page, pindex);
        entries = (int)pindex->entries;

        npos_local *= entries;
        idx = (int)npos_local;
        idx = WT_CLAMP(idx, 0, entries - 1);
        npos_local -= idx;
        descent = pindex->index[idx];

        if (eviction) {
            /*
             * In case of eviction, we never want to load pages from disk. Also, page_swap with
             * WT_READ_CACHE will fail anyway and we'll lose our pointer, so avoid making a call
             * that will fail.
             */
            switch (WT_REF_GET_STATE(descent)) {
            case WT_REF_DISK:
            case WT_REF_LOCKED:
            case WT_REF_DELETED:
                /* Can't go down from here but it's ok to return this page */
                goto done;
            default: /* WT_REF_MEM, WT_REF_SPLIT */
                goto descend;
            }
            /* Unreachable */
        } else {
            /* Not eviction */
            switch (WT_REF_GET_STATE(descent)) {
            case WT_REF_LOCKED:
                if (!LF_ISSET(WT_READ_NO_WAIT)) {
                    WT_RET(__wt_page_release(session, current, flags));
                    __wt_sleep(0, 10);
                    goto restart;
                }
                /* Fall through */
            case WT_REF_DELETED:
                /*
                 * Can't go down from here. Return this page and __find_closest_leaf will finish the
                 * job.
                 */
                goto done;
            default: /* WT_REF_DISK, WT_REF_MEM, WT_REF_SPLIT */
                goto descend;
            }
            /* Unreachable */
        }
        /* Unreachable */

descend:
        /*
         * Swap the current page for the child page. If the page splits while we're retrieving it,
         * restart the search at the root.
         *
         * On other error, simply return, the swap call ensures we're holding nothing on failure.
         */
        if ((ret = __wt_page_swap(session, current, descent, flags)) == 0) {
            current = descent;
            continue;
        }
        if (eviction && (ret == WT_NOTFOUND || ret == WT_RESTART))
            goto done;
        if (ret == WT_RESTART) {
            WT_RET(__wt_page_release(session, current, flags));
            goto restart;
        }
        return (ret);
    }
done:
    /*
     * Because eviction considers internal pages in post-order, returning the root page will
     * indicate the end of walk. Also, eviction will never evict the root. Also, returning a NULL is
     * not an error for eviction but a signal to start over. So handle this case individually.
     */
    if (eviction && __wt_ref_is_root(current)) {
        WT_RET(__wt_page_release(session, current, flags));
        current = NULL;
    }
    *refp = current;
    return (0);
}

/*
 * __wt_page_from_npos --
 *     Find a page given its normalized position.
 */
int
__wt_page_from_npos(
  WT_SESSION_IMPL *session, WT_REF **refp, uint32_t read_flags, uint32_t walk_flags, double npos)
{
    WT_DECL_RET;

    WT_WITH_PAGE_INDEX(session, ret = __page_from_npos_internal(session, refp, read_flags, npos));
    WT_RET(ret);
    /* Return the first good page starting from here */
    return (__find_closest_leaf(session, refp, walk_flags));
}

/*
 * !!!
 * __wt_page_from_npos_for_eviction --
 *     Go to a page given its normalized position (for eviction).
 *     - Use WT_READ_PREV to look up backwards.
 */
int
__wt_page_from_npos_for_eviction(
  WT_SESSION_IMPL *session, WT_REF **refp, uint32_t read_flags, uint32_t walk_flags, double npos)
{
    return (__wt_page_from_npos(session, refp, read_flags | WT_READ_EVICT_READ_FLAGS,
      walk_flags | WT_READ_EVICT_WALK_FLAGS, npos));
}

/*
 * !!!
 * __wt_page_from_npos_for_read --
 *     Go to a leaf page given its normalized position (for reading).
 *     - Use WT_READ_PREV to look up backwards.
 */
int
__wt_page_from_npos_for_read(
  WT_SESSION_IMPL *session, WT_REF **refp, uint32_t read_flags, uint32_t walk_flags, double npos)
{
    return (__wt_page_from_npos(
      session, refp, read_flags | WT_READ_DATA_FLAGS, walk_flags | WT_READ_DATA_FLAGS, npos));
}
