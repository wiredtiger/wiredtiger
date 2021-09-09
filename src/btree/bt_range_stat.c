/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __cursor_range_stat_search_intl --
 *     Return the internal page slot for a key.
 */
static int
__cursor_range_stat_search_intl(WT_SESSION_IMPL *session, WT_PAGE *page, WT_PAGE_INDEX *pindex,
  WT_ITEM *srch_key, uint64_t recno, uint32_t *slotp)
{
    WT_BTREE *btree;
    WT_COLLATOR *collator;
    WT_ITEM item;
    WT_REF *current;
    uint32_t base, indx, limit;
    int cmp;

    btree = S2BT(session);
    collator = btree->collator;

    /* Binary search of an internal page. */
    base = 1;
    limit = pindex->entries - 1;
    if (btree->type == BTREE_COL_FIX || btree->type == BTREE_COL_VAR) {
        for (; limit != 0; limit >>= 1) {
            indx = base + (limit >> 1);
            current = pindex->index[indx];

            if (recno > current->ref_recno) {
                base = indx + 1;
                --limit;
            }
            if (recno == current->ref_recno) {
                *slotp = indx;
                return (0);
            }
        }
    } else if (collator == NULL) {
        for (; limit != 0; limit >>= 1) {
            indx = base + (limit >> 1);
            current = pindex->index[indx];
            __wt_ref_key(page, current, &item.data, &item.size);

            cmp = __wt_lex_compare(srch_key, &item, false);
            if (cmp > 0) {
                base = indx + 1;
                --limit;
            } else if (cmp == 0) {
                *slotp = indx;
                return (0);
            }
        }
    } else
        for (; limit != 0; limit >>= 1) {
            indx = base + (limit >> 1);
            current = pindex->index[indx];
            __wt_ref_key(page, current, &item.data, &item.size);

            WT_RET(__wt_compare(session, collator, srch_key, &item, &cmp));
            if (cmp > 0) {
                base = indx + 1;
                --limit;
            } else if (cmp == 0) {
                *slotp = indx;
                return (0);
            }
        }
    *slotp = base - 1;
    return (0);
}

/*
 * __cursor_range_stat --
 *     Return cursor statistics for a cursor range from the tree.
 */
static int
__cursor_range_stat(
  WT_CURSOR_BTREE *start, WT_CURSOR_BTREE *stop, uint64_t *row_countp, uint64_t *byte_countp)
{
    WT_ADDR_COPY copy;
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_ITEM kstart, kstop;
    WT_PAGE *page;
    WT_PAGE_INDEX *parent_pindex, *pindex;
    WT_REF *child, *current, *descent, *ref;
    WT_SESSION_IMPL *session;
    uint64_t byte_count, child_byte_count, child_row_count, recno_start, recno_stop, row_count;
    uint32_t child_missing_addr, child_reviewed, missing_addr, reviewed, slot, startslot, stopslot;
    uint8_t child_previous_state, previous_state;

    session = CUR2S(start);
    btree = S2BT(session);

    /* Get the key. */
    recno_start = start->recno;
    recno_stop = stop->recno;
    WT_RET(__wt_cursor_get_raw_key((WT_CURSOR *)start, &kstart));
    WT_RET(__wt_cursor_get_raw_key((WT_CURSOR *)stop, &kstop));

restart:
    /* Descend the tree, searching internal pages for the keys. */
    current = &btree->root;
    for (pindex = NULL;;) {
        parent_pindex = pindex;
        page = current->page;

        /*
         * Get the page index and search for the start/stop keys. We could tighten this up some (see
         * the row- and column-search loop for ideas), but it doesn't seem worth the effort. We only
         * need to check the stop key for a split race, as the start key must be either earlier in
         * the page or on the same, last, slot of the page.
         */
        WT_INTL_INDEX_GET(session, page, pindex);
        WT_ERR(
          __cursor_range_stat_search_intl(session, page, pindex, &kstart, recno_start, &startslot));
        WT_ERR(
          __cursor_range_stat_search_intl(session, page, pindex, &kstop, recno_stop, &stopslot));
        if (stopslot == pindex->entries - 1 &&
          __wt_split_descent_race(session, current, parent_pindex))
            goto restart;

        /*
         * If the two slots are different, we've reached the first internal page where the keys
         * diverge into different sub-trees. Don't descend further, we have what we want. Or, if the
         * cursors point to the same leaf page we're done.
         */
        if (startslot != stopslot)
            break;
        descent = pindex->index[startslot];
        if (F_ISSET(descent, WT_REF_FLAG_LEAF))
            break;

        /*
         * Swap the current page for the child page. If the page splits while we're retrieving it,
         * restart the search at the root. We cannot restart in the "current" page; for example, if
         * a thread is appending to the tree, the page it's waiting for did an insert-split into the
         * parent, then the parent split into its parent, the name space we are searching for may
         * have moved above the current page in the tree.
         *
         * On other error, simply return, the swap call ensures we're holding nothing on failure.
         */
        if ((ret = __wt_page_swap(
               session, current, descent, WT_READ_RESTART_OK | WT_READ_WONT_NEED)) == 0) {
            current = descent;
            continue;
        }
        if (ret == WT_RESTART)
            goto restart;
        WT_ERR(ret);
    }

    /* Aggregate the information between the two slots. */
    for (missing_addr = reviewed = 0, slot = startslot; slot <= stopslot; ++slot) {
        ref = pindex->index[slot];

        /*
         * If there's an address, crack it and use the information. Otherwise, walk the underlying
         * page in a quick and dirty manner (we're not guaranteeing accuracy here). Do some basic
         * sanity checks for the existence of the page: the checks are redundant at the moment, but
         * it's cheap and I'm not interested in debugging this in the future.
         */
        ++reviewed;
        row_count = byte_count = 0;
        WT_REF_LOCK(session, ref, &previous_state);
        if (__wt_ref_addr_copy(session, ref, &copy)) {
            row_count = copy.row_count;
            byte_count = copy.byte_count;
        } else if (previous_state == WT_REF_MEM && (page = ref->page) != NULL)
            switch (page->type) {
            case WT_PAGE_COL_INT:
            case WT_PAGE_ROW_INT:
                child_missing_addr = child_reviewed = 0;
                WT_INTL_FOREACH_BEGIN (session, page, child) {
                    ++child_reviewed;
                    child_row_count = child_byte_count = 0;
                    WT_REF_LOCK(session, child, &child_previous_state);
                    if (__wt_ref_addr_copy(session, child, &copy)) {
                        child_row_count = copy.row_count;
                        child_byte_count = copy.byte_count;
                    } else
                        ++child_missing_addr;
                    WT_REF_UNLOCK(child, child_previous_state);
                    row_count += child_row_count;
                    byte_count += child_byte_count;
                }
                WT_INTL_FOREACH_END;

                /*
                 * If 50% of the child slots we check don't have the information we want, don't
                 * count the overall slot as a success.
                 */
                if (child_missing_addr > child_reviewed / 2)
                    ++missing_addr;
                break;
            case WT_PAGE_COL_FIX:
            case WT_PAGE_COL_VAR:
                /* No reason to set the row count, we can simply calculate it. */
                byte_count = page->memory_footprint;
                break;
            case WT_PAGE_ROW_LEAF:
                row_count = page->entries / 2;
                byte_count = page->memory_footprint;
                break;
            }
        else
            ++missing_addr;
        WT_REF_UNLOCK(ref, previous_state);

        /*
         * An adjustment to improve accuracy: assume the key takes up half of the range in the slot
         * itself, on the first and last slots. This also makes the case where the two cursors are
         * on the same leaf page return something reasonable, a range that isn't insane. If we want
         * something better, we should descend into the first/last slots to get a better value.
         */
        if (slot == startslot || slot == stopslot) {
            row_count /= 2;
            byte_count /= 2;
        }
        *row_countp += row_count;
        *byte_countp += byte_count;
    }

    /*
     * If 50% of the slots we check don't have the information we want, fail the call. (There isn't
     * any evidence this is a good choice, we may want to tune it to better reflect real workloads.)
     */
    if (missing_addr > reviewed / 2)
        ret = WT_NOTFOUND;

err:
    WT_TRET(__wt_page_release(session, current, 0));
    return (ret);
}

/*
 * __wt_btcur_range_stat --
 *     Return cursor statistics for a cursor range from the tree.
 */
int
__wt_btcur_range_stat(
  WT_CURSOR *start, WT_CURSOR *stop, uint64_t *row_countp, uint64_t *byte_countp)
{
    WT_BTREE *btree;
    WT_CURSOR_BTREE *bt_start, *bt_stop;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = CUR2S(start);
    btree = CUR2BT(start);
    bt_start = (WT_CURSOR_BTREE *)start;
    bt_stop = (WT_CURSOR_BTREE *)stop;

    WT_WITH_BTREE(session, btree,
      WT_WITH_PAGE_INDEX(
        session, ret = __cursor_range_stat(bt_start, bt_stop, row_countp, byte_countp)));

    /*
     * There are paths in the worker code that either do or don't calculate the row count because
     * it's simpler that way. Once we're about to return, simply correct the row count to be exact.
     * Note the row count is inclusive of the end points.
     */
    if (btree->type == BTREE_COL_FIX || btree->type == BTREE_COL_VAR)
        *row_countp = (stop->recno - start->recno) + 1;

    return (ret);
}
