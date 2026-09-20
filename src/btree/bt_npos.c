/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 *
 * Below is a set of functions for computing the normalized position of a page and restoring a page
 * from its normalized position. These functions are used by the eviction server for the sake of
 * not holding the hazard pointer for longer than necessary. Another use is being able to seek to a
 * page at a fraction of the entire dataset.
 *
 * Normalized position is a number in the range of 0 .. 1 that represents a page's position across
 * all pages. It's primary goal is to be cheap rather than precise. It works best when the
 * tree is perfectly balanced, i.e. all internal pages at the same level have the same number of
 * children and the depth of all leaf pages is the same. In practice, the tree is not perfect, so
 * the normalized position is imprecise. However, it's totally fine for the eviction server because
 * it only uses an approximate position in the tree to continue to walk from.
 * Even when using a hazard pointer, page splits can shift data so that some pages or sub-trees can
 * be skipped an eviction pass.
 *
 * Eviction wants to be as non-intrusive as possible and never loads pages into memory, while
 * seek-for-read can load pages or wait for them to be unlocked. The behavior is controlled by
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
 * The remaining fractional part at the leaf page selects a slot on the page, see
 * __wt_btcur_set_position.
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
 * Here's an example: Suppose we have a depth 2 tree, at the lowest level we are the 6th out of 10
 * pages and at the higher level the 3rd out of 5 pages. Furthermore we want to start around quarter
 * of the way into the page.
 *
 * Initially we have a starting position of 0.25, our page is at the 6/10th position in this level
 * of the tree. This gives us the following calculation:
 * Level 2 position = (6 + 0.25)/10  = 0.625
 *
 * Then we add the position from level 1:
 * Final position = (3 + Level 2 position)/5 = (3 + 0.625)/5 = 0.725.
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
 *     - If 'path_str_offsetp' is set, return a string representation of the page's path.
 *     - 'start' is a position within the leaf page: 0 .. 1.
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
    int unused = 1; /* WT_UNUSED(snprintf) is cooked in GCC. */

    npos = start;
    if (path_str)
        *path_str_offsetp = 0;

    WT_ENTER_PAGE_INDEX(session);
    while (!__wt_ref_is_root(ref)) {
        slot = UINT32_MAX; /* We get this invalid value in case of error. */
        __wt_ref_index_slot(session, ref, &pindex, &slot);
        entries = pindex->entries;
        /*
         * Depending on the implementation, '__wt_ref_index_slot' might return an error or 'slot'
         * outside of range. Check for 'slot < entries' ensures that it's a valid number. If it's
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
        __wt_ref_ascend(session, &ref, NULL, NULL);
    }
    WT_LEAVE_PAGE_INDEX(session);

    if (path_str)
        path_str[*path_str_offsetp] = 0;

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
__find_closest_leaf(WT_SESSION_IMPL *session, WT_REF **refp, uint32_t flags, uint64_t *walkcntp)
{
    WT_DECL_RET;

    *walkcntp = 0;
    if (*refp == NULL || F_ISSET(*refp, WT_REF_FLAG_LEAF))
        return (0);
    LF_SET(WT_READ_SKIP_INTL);

    ret = __wt_tree_walk_count(session, refp, walkcntp, flags);

    if (LF_ISSET(WT_READ_EVICT_WALK_FLAGS))
        WT_STAT_CONN_INCR(session, npos_evict_walk_max);
    else
        WT_STAT_CONN_INCR(session, npos_read_walk_max);

    return (ret);
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
__page_from_npos_internal(WT_SESSION_IMPL *session, WT_REF **refp, double npos, uint32_t flags)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_PAGE *page;
    WT_PAGE_INDEX *pindex;
    WT_REF *current, *descent;
    double npos_local;
    int idx, entries;
    bool read_cache;

    *refp = NULL;

    btree = S2BT(session);
    current = NULL;
    /*
     * This function is called by eviction to find a page in the cache. That case is indicated by
     * the WT_READ_CACHE flag. Ordinary lookups in a tree will read pages into cache as needed.
     */
    read_cache = LF_ISSET(WT_READ_CACHE);

restart: /* Restart the search from the root. */

    /* Search the internal pages of the tree. */
    current = &btree->root;
    npos_local = npos;
    for (;;) {
        /*
         * This function will always return a leaf page even if the saved position was for an
         * internal one. This potentially can lead to internal pages being skipped by eviction in
         * case when eviction happened to pause on an internal page and some subsequent internal
         * pages don't have any leafs and also are subject for eviction.
         *
         * This is a rare case. However, it doesn't lead to completely non-evictable internal pages
         * because eviction will eventually reach these pages during other passes.
         */
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

        if (read_cache) {
            /*
             * In case of eviction, we never want to load pages from disk. Also, page_swap with
             * WT_READ_CACHE will fail anyway and we'll lose our pointer, so avoid making a call
             * that will fail.
             */
            switch (WT_REF_GET_STATE(descent)) {
            case WT_REF_DISK:
            case WT_REF_LOCKED:
            case WT_REF_DELETED:
                /* Can't go down from here but it's ok to return the "current" page. */
                goto done;
            default: /* WT_REF_MEM, WT_REF_SPLIT */
                goto descend;
            }
            /* Unreachable but it's ok to be here. */
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
                 * Can't go down from here. Return the "current" page and
                 * __find_closest_leaf will finish the job.
                 */
                goto done;
            default: /* WT_REF_DISK, WT_REF_MEM, WT_REF_SPLIT */
                goto descend;
            }
            /* Unreachable but it's ok to be here. */
        }
        /* Unreachable but it's ok to be here. */

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
        if (read_cache && (ret == WT_NOTFOUND || ret == WT_RESTART))
            goto done;
        if (ret == WT_RESTART) {
            /* The swap keeps the held page only when restarts are expected. */
            if (LF_ISSET(WT_READ_RESTART_OK))
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
    if (read_cache && __wt_ref_is_root(current)) {
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
  WT_SESSION_IMPL *session, WT_REF **refp, double npos, uint32_t read_flags, uint32_t walk_flags)
{
    WT_DECL_RET;
    uint64_t walkcnt;

    WT_WITH_PAGE_INDEX(session, ret = __page_from_npos_internal(session, refp, npos, read_flags));
    WT_RET(ret);
    /* Return the first good page starting from here. */
    return (__find_closest_leaf(session, refp, walk_flags, &walkcnt));
}

/*
 * !!!
 * __wt_page_from_npos_for_eviction --
 *     Go to a page given its normalized position (for eviction).
 *     - Use WT_READ_PREV to look up backwards.
 */
int
__wt_page_from_npos_for_eviction(
  WT_SESSION_IMPL *session, WT_REF **refp, double npos, uint32_t read_flags, uint32_t walk_flags)
{
    return (__wt_page_from_npos(session, refp, npos, read_flags | WT_READ_EVICT_READ_FLAGS,
      walk_flags | WT_READ_EVICT_WALK_FLAGS));
}

/*
 * !!!
 * __wt_page_from_npos_for_read --
 *     Go to a leaf page given its normalized position (for reading).
 *     - Use WT_READ_PREV to look up backwards.
 */
int
__wt_page_from_npos_for_read(
  WT_SESSION_IMPL *session, WT_REF **refp, double npos, uint32_t read_flags, uint32_t walk_flags)
{
    return (__wt_page_from_npos(
      session, refp, npos, read_flags | WT_READ_DATA_FLAGS, walk_flags | WT_READ_DATA_FLAGS));
}

/*
 * __npos_ref_usable --
 *     Return whether a descent can enter a child ref. A locked child that the caller is willing to
 *     wait for is reported separately: the caller releases its page and retries from the root.
 */
static bool
__npos_ref_usable(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags, bool *waitp)
{
    *waitp = false;
    switch (WT_REF_GET_STATE(ref)) {
    case WT_REF_LOCKED:
        *waitp = !LF_ISSET(WT_READ_NO_WAIT);
        return (false);
    case WT_REF_DISK:
        return (!LF_ISSET(WT_READ_CACHE));
    case WT_REF_DELETED:
        /* A deletion that isn't visible to this transaction means the page must be read. */
        return (!LF_ISSET(WT_READ_CACHE) &&
          !__wti_delete_page_skip(session, ref, !F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT)));
    default: /* WT_REF_MEM, WT_REF_SPLIT */
        return (true);
    }
}

/*
 * __npos_leaf_closest --
 *     Descend to the leaf at a normalized position for a cursor. When the addressed child cannot be
 *     entered, scan the pinned parent's index in the walk direction for the nearest usable child
 *     and enter neighboring subtrees at the edge closest to the addressed page, counting the
 *     children stepped over. If the parent runs out of children the parent is returned and the
 *     caller finishes with a tree walk.
 *
 * NOTE: Must be called within WT_WITH_PAGE_INDEX or WT_ENTER_PAGE_INDEX
 */
static int
__npos_leaf_closest(WT_SESSION_IMPL *session, WT_REF **refp, double npos, uint32_t flags,
  double *remainderp, uint64_t *skippedp)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_PAGE_INDEX *pindex;
    WT_REF *current, *descent;
    double npos_local;
    uint64_t skipped;
    int entries, i, idx, step;
    bool on_path, wait;

    btree = S2BT(session);
    step = LF_ISSET(WT_READ_PREV) ? -1 : 1;
    *refp = NULL;

restart:
    current = &btree->root;
    npos_local = npos;
    skipped = 0;
    on_path = true;
    while (!F_ISSET(current, WT_REF_FLAG_LEAF)) {
        WT_INTL_INDEX_GET(session, current->page, pindex);
        entries = (int)pindex->entries;

        if (on_path) {
            npos_local *= entries;
            idx = (int)npos_local;
            idx = WT_CLAMP(idx, 0, entries - 1);
            npos_local -= idx;
        } else
            idx = step < 0 ? entries - 1 : 0;

        for (descent = NULL, i = idx; i >= 0 && i < entries; i += step) {
            if (__npos_ref_usable(session, pindex->index[i], flags, &wait)) {
                /* Both expected failures leave the current page pinned. */
                ret = __wt_page_swap(session, current, pindex->index[i],
                  flags | WT_READ_NOTFOUND_OK | WT_READ_RESTART_OK);
                if (ret == 0) {
                    descent = pindex->index[i];
                    break;
                }
                if (ret == WT_RESTART) {
                    WT_RET(__wt_page_release(session, current, flags));
                    goto restart;
                }
                if (ret != WT_NOTFOUND)
                    return (ret);
            }
            if (wait) {
                WT_RET(__wt_page_release(session, current, flags));
                __wt_sleep(0, 10);
                goto restart;
            }
            ++skipped;
            on_path = false;
        }
        if (descent == NULL)
            break;
        current = descent;
    }

    *refp = current;
    *remainderp = on_path ? npos_local : (step < 0 ? 1.0 : 0.0);
    *skippedp = skipped;
    return (0);
}

/*
 * __npos_key --
 *     Find the boundary key of the leaf page at a normalized position without reading the leaf. The
 *     boundary is the separator key of the deepest ref on the path reached through a non-zero slot;
 *     slot 0 carries no key, so a path of nothing but zero slots yields the empty key.
 *
 * NOTE: Must be called within WT_WITH_PAGE_INDEX or WT_ENTER_PAGE_INDEX
 */
static int
__npos_key(WT_SESSION_IMPL *session, double npos, uint32_t flags, WT_ITEM *key)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_PAGE *page;
    WT_PAGE_INDEX *pindex;
    WT_REF *current, *descent;
    double npos_local;
    size_t size;
    int entries, idx;
    bool read_cache;
    void *data;

    btree = S2BT(session);
    read_cache = LF_ISSET(WT_READ_CACHE);

    /*
     * The empty key must be a valid zero-length item, not a NULL reference, and NUL-terminated so
     * string-format callers reading it as a C string see an empty string rather than stale bytes.
     */
    WT_RET(__wt_buf_init(session, key, 1));

restart:
    current = &btree->root;
    npos_local = npos;
    key->size = 0;
    ((char *)key->mem)[0] = '\0';
    for (;;) {
        if (F_ISSET(current, WT_REF_FLAG_LEAF))
            break;

        page = current->page;
        WT_INTL_INDEX_GET(session, page, pindex);
        entries = (int)pindex->entries;

        npos_local *= entries;
        idx = (int)npos_local;
        idx = WT_CLAMP(idx, 0, entries - 1);
        npos_local -= idx;
        descent = pindex->index[idx];

        /*
         * Copy the key while the page holding it is pinned. Overflow keys on internal pages are
         * always instantiated, so the reference is valid for any slot.
         */
        if (idx != 0) {
            __wt_ref_key(page, descent, &data, &size);
            WT_ERR(__wt_buf_set(session, key, data, size));
            /* Separators are unterminated prefixes; string-format callers read a C string. */
            WT_ERR(__wt_buf_grow(session, key, size + 1));
            ((char *)key->mem)[size] = '\0';
        }

        if (F_ISSET(descent, WT_REF_FLAG_LEAF))
            break;

        switch (WT_REF_GET_STATE(descent)) {
        case WT_REF_LOCKED:
            if (LF_ISSET(WT_READ_NO_WAIT))
                WT_ERR(WT_NOTFOUND);
            ret = __wt_page_release(session, current, flags);
            current = NULL;
            WT_ERR(ret);
            __wt_sleep(0, 10);
            goto restart;
        case WT_REF_DISK:
        case WT_REF_DELETED:
            if (read_cache)
                WT_ERR(WT_NOTFOUND);
            break;
        default: /* WT_REF_MEM, WT_REF_SPLIT */
            break;
        }

        /*
         * The two expected failures leave the current page pinned, any other failure releases it.
         */
        ret = __wt_page_swap(
          session, current, descent, flags | WT_READ_NOTFOUND_OK | WT_READ_RESTART_OK);
        if (ret == 0) {
            current = descent;
            continue;
        }
        if (ret == WT_RESTART) {
            ret = __wt_page_release(session, current, flags);
            current = NULL;
            WT_ERR(ret);
            goto restart;
        }
        if (ret != WT_NOTFOUND)
            current = NULL;
        WT_ERR(ret);
    }

err:
    WT_TRET(__wt_page_release(session, current, flags));
    return (ret);
}

/*
 * __npos_cursor_start --
 *     Arrange for the next cursor movement to evaluate the given on-disk slot of the pinned page
 *     first, instead of stepping past it. The prepare-conflict retry path re-reads the current
 *     element, which is exactly the behavior needed here.
 */
static void
__npos_cursor_start(WT_CURSOR_BTREE *cbt, uint32_t slot, bool prev)
{
    WT_PAGE *page;

    page = cbt->ref->page;

    __cursor_pos_clear(cbt);
    if (page->entries == 0) {
        /* Everything on the page is on the smallest-key insert list. */
        cbt->slot = UINT32_MAX;
        cbt->ins_head = WT_ROW_INSERT_SMALLEST(page);
        cbt->ins = prev ? WT_SKIP_LAST(cbt->ins_head) : WT_SKIP_FIRST(cbt->ins_head);
        cbt->iter_retry = WT_CBT_RETRY_INSERT;
    } else {
        cbt->slot = slot;
        cbt->iter_retry = WT_CBT_RETRY_PAGE;
    }
    __wti_btcur_iterate_setup(cbt);
    /* Iterate setup only lands on the smallest-key slot when that insert list exists. */
    if (cbt->ins_head == NULL && page->entries == 0)
        cbt->row_iteration_slot = 1;

    F_SET(cbt, prev ? WT_CBT_ITERATE_RETRY_PREV : WT_CBT_ITERATE_RETRY_NEXT);
}

/*
 * __wt_btcur_set_position --
 *     Position the cursor at a normalized position in the tree.
 */
int
__wt_btcur_set_position(WT_CURSOR_BTREE *cbt, WT_POSITION *position)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_PAGE *page;
    WT_SESSION_IMPL *session;
    double npos, remainder;
    uint64_t skipped, walkcnt;
    uint32_t entries, flags, read_flags, slot, walk_flags;
    bool cache_only, prev;

    cursor = &cbt->iface;
    session = CUR2S(cbt);
    flags = position->flags;
    cache_only = LF_ISSET(WT_POSITION_CACHE_ONLY);
    prev = LF_ISSET(WT_POSITION_PREV);
    position->pages_skipped = 0;

    WT_STAT_CONN_DSRC_INCR(session, cursor_set_position);

    WT_ASSERT(session, CUR2BT(cbt)->type == BTREE_ROW);

    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);
    WT_RET(__wt_cursor_func_init(cbt, true));

    npos = WT_CLAMP(position->pos, 0.0, 1.0);

    read_flags = WT_READ_RESTART_OK;
    if (prev)
        read_flags |= WT_READ_PREV;
    if (F_ISSET(cbt, WT_CBT_READ_ONCE))
        read_flags |= WT_READ_WONT_NEED;
    walk_flags = read_flags;
    if (cache_only) {
        read_flags |= WT_READ_EVICT_READ_FLAGS;
        walk_flags |= WT_READ_EVICT_WALK_FLAGS;
    } else {
        read_flags |= WT_READ_DATA_FLAGS;
        walk_flags |= WT_READ_DATA_FLAGS;
    }
    if (!F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT))
        walk_flags |= WT_READ_VISIBLE_ALL;

    if (LF_ISSET(WT_POSITION_KEY_ONLY)) {
        WT_WITH_PAGE_INDEX(session, ret = __npos_key(session, npos, read_flags, &cursor->key));
        WT_TRET(__cursor_reset(cbt));
        if (ret == 0)
            F_SET(cursor, WT_CURSTD_KEY_EXT);
        return (ret);
    }

    WT_WITH_PAGE_INDEX(session,
      ret = __npos_leaf_closest(session, &cbt->ref, npos, read_flags, &remainder, &skipped));
    WT_ERR(ret);

    /*
     * The descent returns an internal page only when none of its remaining children could be
     * entered; the tree walk then moves on to the next subtree, skipping an unknown number of
     * pages.
     */
    WT_ERR(__find_closest_leaf(session, &cbt->ref, walk_flags, &walkcnt));
    if (walkcnt != 0) {
        skipped += walkcnt;
        remainder = prev ? 1.0 : 0.0;
    }
    position->pages_skipped = (uint32_t)WT_MIN(skipped, UINT32_MAX);
    if (cbt->ref == NULL)
        WT_ERR(WT_NOTFOUND);
    WT_ASSERT(session, F_ISSET(cbt->ref, WT_REF_FLAG_LEAF));

    page = cbt->ref->page;
    entries = page->entries;
    switch (flags & WT_POSITION_ANCHOR_MASK) {
    case WT_POSITION_ANCHOR_FIRST:
        slot = 0;
        break;
    case WT_POSITION_ANCHOR_MIDDLE:
        slot = entries / 2;
        break;
    case WT_POSITION_ANCHOR_LAST:
        slot = entries == 0 ? 0 : entries - 1;
        break;
    default:
        slot = (uint32_t)(remainder * entries);
        break;
    }
    if (entries != 0 && slot >= entries)
        slot = entries - 1;

    /* Walking backwards through prefix-compressed keys is quadratic unless some are built. */
    if (prev)
        WT_ERR(__wt_row_leaf_key_instantiate(session, page));

    __npos_cursor_start(cbt, slot, prev);

    if (cache_only)
        F_SET(cbt, WT_CBT_WALK_CACHE_ONLY);
    ret = prev ? __wt_btcur_prev(cbt, false) : __wt_btcur_next(cbt, false);
    F_CLR(cbt, WT_CBT_WALK_CACHE_ONLY);
    return (ret);

err:
    WT_TRET(__cursor_reset(cbt));
    return (ret);
}

/*
 * __wt_btcur_get_position --
 *     Return the normalized position of the record the cursor is positioned on.
 */
void
__wt_btcur_get_position(WT_CURSOR_BTREE *cbt, double *posp)
{
    WT_PAGE *page;
    WT_SESSION_IMPL *session;
    double remainder;
    uint32_t entries, slot;

    session = CUR2S(cbt);
    page = cbt->ref->page;
    entries = page->entries;
    slot = cbt->slot;

    WT_STAT_CONN_DSRC_INCR(session, cursor_get_position);

    if (entries == 0)
        remainder = WT_NPOS_MID;
    else {
        /* Insert-list records before the first or after the last cell have no slot of their own. */
        if (slot >= entries)
            slot = cbt->ins_head == WT_ROW_INSERT_SMALLEST(page) ? 0 : entries - 1;
        remainder = (slot + 0.5) / entries;
    }

    *posp = __wt_page_npos(session, cbt->ref, remainder, NULL, NULL, 0);
}
