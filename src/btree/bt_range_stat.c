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
    WT_REF *current, *descent, *ref;
    WT_SESSION_IMPL *session;
    uint64_t byte_count, recno_start, recno_stop, row_count;
    uint32_t missing_addr, slot, startslot, stopslot;
    uint8_t previous_state;

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
        return (ret);
    }

    /* Aggregate the information between the two slots. */
    for (missing_addr = 0, slot = startslot; slot <= stopslot; ++slot) {
        ref = pindex->index[slot];
        WT_REF_LOCK(session, ref, &previous_state);
        if (__wt_ref_addr_copy(session, ref, &copy))
            ret = __wt_addr_cookie_btree_unpack(copy.addr, &row_count, &byte_count);
        else {
            ++missing_addr;
            row_count = byte_count = 0;
        }
        WT_REF_UNLOCK(ref, previous_state);
        WT_ERR(ret);

        /*
         * An adjustment to improve accuracy: assume the key takes up half of the range in the slot
         * itself, on the first and last slots. This also makes the case where the two cursors are
         * on the same leaf page work return something reasonable, a range that isn't insane. If we
         * want something even better, we could descend into the first/last slots to get a closer
         * adjustment value.
         */
        if (slot == startslot || slot == stopslot)
            row_count /= 2;
        *row_countp += row_count;
        if (slot == startslot || slot == stopslot)
            byte_count /= 2;
        *byte_countp += byte_count;
    }

    /*
     * If we don't find any pages with addresses, the default would return row and byte counts of
     * zero. There's no good rule here, for now I'm going with 50% failure means the call fails.
     */
    if (missing_addr > (stopslot - startslot) / 2)
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
    WT_CURSOR_BTREE *bt_start, *bt_stop;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    session = CUR2S(start);

    bt_start = (WT_CURSOR_BTREE *)start;
    bt_stop = (WT_CURSOR_BTREE *)stop;

    WT_WITH_BTREE(session, CUR2BT(bt_start),
      WT_WITH_PAGE_INDEX(
        session, ret = __cursor_range_stat(bt_start, bt_stop, row_countp, byte_countp)));
    return (ret);
}
