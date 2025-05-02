/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Walking backwards through skip lists.
 *
 * The skip list stack is an array of pointers set up by a search. It points to the position a node
 * should go in the skip list. In other words, the skip list search stack always points *after* the
 * search item (that is, into the search item's next array).
 *
 * Helper macros to go from a stack pointer at level i, pointing into a next array, back to the
 * insert node containing that next array.
 */
#undef PREV_ITEM
#define PREV_ITEM(ins_head, insp, i)                      \
    (((insp) == &(ins_head)->head[i] || (insp) == NULL) ? \
        NULL :                                            \
        (WT_INSERT *)((char *)((insp) - (i)) - offsetof(WT_INSERT, next)))

#undef PREV_INS
#define PREV_INS(cbt, i) PREV_ITEM((cbt)->ins_head, (cbt)->ins_stack[(i)], (i))

/*
 * __cursor_skip_prev --
 *     Move back one position in a skip list stack (aka "finger").
 */
static WT_INLINE int
__cursor_skip_prev(WT_CURSOR_BTREE *cbt)
{
    WT_INSERT *current, *ins, *next_ins;
    WT_ITEM key;
    WT_SESSION_IMPL *session;
    int i;

    session = CUR2S(cbt);

restart:
    while ((current = cbt->ins) != PREV_INS(cbt, 0)) {
        key.data = WT_INSERT_KEY(current);
        key.size = WT_INSERT_KEY_SIZE(current);
        WT_RET(__wt_search_insert(session, cbt, cbt->ins_head, &key));
    }

    /*
     * Find the first node up the search stack that does not move.
     *
     * The depth of the current item must be at least this level, since we
     * see it in that many levels of the stack.
     *
     * !!! Watch these loops carefully: they all rely on the value of i,
     * and the exit conditions to end up with the right values are
     * non-trivial.
     */
    ins = NULL; /* -Wconditional-uninitialized */
    for (i = 0; i < WT_SKIP_MAXDEPTH - 1; i++)
        if ((ins = PREV_INS(cbt, i + 1)) != current)
            break;

    /*
     * Find a starting point for the new search. That is either at the non-moving node if we found a
     * valid node, or the beginning of the next list down that is not the current node.
     *
     * Since it is the beginning of a list, and we know the current node is has a skip depth at
     * least this high, any node we find must sort before the current node.
     */
    if (ins == NULL || ins == current)
        for (; i >= 0; i--) {
            cbt->ins_stack[i] = NULL;
            cbt->next_stack[i] = NULL;
            /*
             * Compiler may replace the usage of the variable with another read in the following
             * code. Here we don't need to worry about CPU reordering as we are reading a thread
             * local value.
             *
             * Place an acquire barrier to avoid this issue.
             */
            WT_ACQUIRE_READ_WITH_BARRIER(ins, cbt->ins_head->head[i]);
            if (ins != NULL && ins != current)
                break;
        }

    /* Walk any remaining levels until just before the current node. */
    while (i >= 0) {
        /*
         * If we get to the end of a list without finding the current item, we must have raced with
         * an insert. Restart the search.
         */
        if (ins == NULL) {
            cbt->ins_stack[0] = NULL;
            cbt->next_stack[0] = NULL;
            goto restart;
        }
        /*
         * CPUs with weak memory ordering may reorder the read and return a stale value. This can
         * lead us to wrongly skip a value in the lower levels of the skip list.
         *
         * For example, if we have A -> C initially for both level 0 and level 1 and we concurrently
         * insert B into both level 0 and level 1. If B is visible on level 1 to this thread, it
         * must also be visible on level 0. Otherwise, we would record an inconsistent stack.
         *
         * Place an acquire barrier to avoid this issue.
         */
        WT_ACQUIRE_READ_WITH_BARRIER(next_ins, ins->next[i]);
        if (next_ins != current) /* Stay at this level */
            ins = next_ins;
        else { /* Drop down a level */
            cbt->next_stack[i] = next_ins;
            cbt->ins_stack[i] = &ins->next[i];
            --i;
        }
    }

    /* If we found a previous node, the next one must be current. */
    if (cbt->ins_stack[0] != NULL && *cbt->ins_stack[0] != current)
        goto restart;

    cbt->ins = PREV_INS(cbt, 0);
    return (0);
}

/*
 * __cursor_row_prev --
 *     Move to the previous row-store item.
 */
static WT_INLINE int
__cursor_row_prev(
  WT_CURSOR_BTREE *cbt, bool newpage, bool restart, size_t *skippedp, bool *key_out_of_boundsp)
{
    WT_CELL_UNPACK_KV kpack;
    WT_DECL_RET;
    WT_INSERT *ins;
    WT_ITEM *key;
    WT_PAGE *page;
    WT_ROW *rip;
    WT_SESSION_IMPL *session;

    key = &cbt->iface.key;
    page = cbt->ref->page;
    session = CUR2S(cbt);
    *skippedp = 0;

    /* If restarting after a prepare conflict, jump to the right spot. */
    if (restart) {
        if (cbt->iter_retry == WT_CBT_RETRY_INSERT)
            goto restart_read_insert;
        if (cbt->iter_retry == WT_CBT_RETRY_PAGE)
            goto restart_read_page;
    }
    cbt->iter_retry = WT_CBT_RETRY_NOTSET;

    /*
     * For row-store pages, we need a single item that tells us the part of the page we're walking
     * (otherwise switching from next to prev and vice-versa is just too complicated), so we map the
     * WT_ROW and WT_INSERT_HEAD insert array slots into a single name space: slot 1 is the
     * "smallest key insert list", slot 2 is WT_ROW[0], slot 3 is WT_INSERT_HEAD[0], and so on. This
     * means WT_INSERT lists are odd-numbered slots, and WT_ROW array slots are even-numbered slots.
     *
     * Initialize for each new page.
     */
    if (newpage) {
        /* Check if keys need to be instantiated before we walk the page. */
        WT_RET(__wt_row_leaf_key_instantiate(session, page));

        /*
         * Be paranoid and set the slot out of bounds when moving to a new page.
         */
        cbt->slot = UINT32_MAX;
        if (page->entries == 0)
            cbt->ins_head = WT_ROW_INSERT_SMALLEST(page);
        else
            cbt->ins_head = WT_ROW_INSERT_SLOT(page, page->entries - 1);
        cbt->ins = WT_SKIP_LAST(cbt->ins_head);
        cbt->row_iteration_slot = page->entries * 2 + 1;
        cbt->rip_saved = NULL;
        goto new_insert;
    }

    /* Move to the previous entry and return the item. */
    for (;;) {
        /*
         * Continue traversing any insert list. Maintain the reference to the current insert element
         * in case we switch to a cursor next movement.
         */
        if (cbt->ins != NULL)
            WT_RET(__cursor_skip_prev(cbt));

new_insert:
        cbt->iter_retry = WT_CBT_RETRY_INSERT;
restart_read_insert:
        if ((ins = cbt->ins) != NULL) {
            key->data = WT_INSERT_KEY(ins);
            key->size = WT_INSERT_KEY_SIZE(ins);

            if (F_ISSET(&cbt->iface, WT_CURSTD_KEY_ONLY))
                return (0);

            /*
             * If a lower bound has been set ensure that the key is within the range, otherwise
             * early exit.
             */
            if ((ret = __wt_btcur_bounds_early_exit(session, cbt, false, key_out_of_boundsp)) ==
              WT_NOTFOUND)
                WT_STAT_CONN_DSRC_INCR(session, cursor_bounds_prev_early_exit);
            WT_RET(ret);

            WT_RET(__wt_txn_read_upd_list(session, cbt, ins->upd));
            if (cbt->upd_value->type == WT_UPDATE_INVALID) {
                ++*skippedp;
                continue;
            }
            if (cbt->upd_value->type == WT_UPDATE_TOMBSTONE) {
                if (__wt_txn_upd_value_visible_all(session, cbt->upd_value))
                    ++cbt->page_deleted_count;
                ++*skippedp;
                continue;
            }
            __wt_value_return(cbt, cbt->upd_value);
            return (0);
        }

        /* Check for the beginning of the page. */
        if (cbt->row_iteration_slot == 1)
            return (WT_NOTFOUND);
        --cbt->row_iteration_slot;

        /*
         * Odd-numbered slots configure as WT_INSERT_HEAD entries, even-numbered slots configure as
         * WT_ROW entries.
         */
        if (cbt->row_iteration_slot & 0x01) {
            cbt->ins_head = cbt->row_iteration_slot == 1 ?
              WT_ROW_INSERT_SMALLEST(page) :
              WT_ROW_INSERT_SLOT(page, cbt->row_iteration_slot / 2 - 1);
            cbt->ins = WT_SKIP_LAST(cbt->ins_head);
            goto new_insert;
        }
        cbt->ins_head = NULL;
        cbt->ins = NULL;

        cbt->iter_retry = WT_CBT_RETRY_PAGE;
        cbt->slot = cbt->row_iteration_slot / 2 - 1;
restart_read_page:
        rip = &page->pg_row[cbt->slot];
        /*
         * The saved cursor key from the slot is used later to get the value from the history store
         * if the on-disk data is not visible.
         */
        WT_RET(__cursor_row_slot_key_return(cbt, rip, &kpack));

        if (F_ISSET(&cbt->iface, WT_CURSTD_KEY_ONLY))
            return (0);

        /*
         * If a lower bound has been set ensure that the key is within the range, otherwise early
         * exit.
         */
        if ((ret = __wt_btcur_bounds_early_exit(session, cbt, false, key_out_of_boundsp)) ==
          WT_NOTFOUND)
            WT_STAT_CONN_DSRC_INCR(session, cursor_bounds_prev_early_exit);
        WT_RET(ret);

        /*
         * Read the on-disk value and/or history. Pass an update list: the update list may contain
         * the base update for a modify chain after rollback-to-stable, required for correctness.
         */
        WT_RET(__wt_txn_read(session, cbt, &cbt->iface.key, WT_ROW_UPDATE(page, rip)));
        if (cbt->upd_value->type == WT_UPDATE_INVALID) {
            ++*skippedp;
            continue;
        }
        if (cbt->upd_value->type == WT_UPDATE_TOMBSTONE) {
            if (__wt_txn_upd_value_visible_all(session, cbt->upd_value))
                ++cbt->page_deleted_count;
            ++*skippedp;
            continue;
        }
        __wt_value_return(cbt, cbt->upd_value);
        return (0);
    }
    /* NOTREACHED */
}

/*
 * __wt_btcur_prev --
 *     Move to the previous record in the tree.
 */
int
__wt_btcur_prev(WT_CURSOR_BTREE *cbt, bool truncating)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_PAGE *page;
    WT_PAGE_WALK_SKIP_STATS walk_skip_stats;
    WT_SESSION_IMPL *session;
    size_t skipped, total_skipped;
    uint64_t time_start;
    uint32_t flags;
    bool key_out_of_bounds, need_walk, newpage, repositioned, restart;
#ifdef HAVE_DIAGNOSTIC
    bool inclusive_set;
    WT_NOT_READ(inclusive_set, false);
#endif

    cursor = &cbt->iface;
    key_out_of_bounds = need_walk = newpage = repositioned = false;
    session = CUR2S(cbt);
    total_skipped = 0;
    walk_skip_stats.total_del_pages_skipped = 0;
    walk_skip_stats.total_inmem_del_pages_skipped = 0;
    WT_NOT_READ(time_start, 0);

    WT_STAT_CONN_DSRC_INCR(session, cursor_prev);

    /* tree walk flags */
    flags = WT_READ_NO_SPLIT | WT_READ_PREV | WT_READ_SKIP_INTL;
    if (truncating)
        LF_SET(WT_READ_TRUNCATE);

    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    WT_ERR(__wt_cursor_func_init(cbt, false));

    /*
     * If we have a bound set we should position our cursor appropriately if it isn't already
     * positioned. It is possible that the positioning function can directly return the record. For
     * that to happen, the cursor must be placed on a valid record and must be positioned on the
     * first record within the bounds. If the record is not valid or is not positioned within the
     * bounds, continue the prev traversal logic.
     */
    if (F_ISSET(cursor, WT_CURSTD_BOUND_UPPER) && !WT_CURSOR_IS_POSITIONED(cbt)) {
        repositioned = true;
        time_start = __wt_clock(session);
        WT_ERR(__wti_btcur_bounds_position(session, cbt, false, &need_walk));
        if (!need_walk) {
            __wt_value_return(cbt, cbt->upd_value);
            goto done;
        }
    }

    /*
     * If we aren't already iterating in the right direction, there's some setup to do.
     */
    if (!F_ISSET(cbt, WT_CBT_ITERATE_PREV))
        __wti_btcur_iterate_setup(cbt);

    /*
     * Walk any page we're holding until the underlying call returns not-found. Then, move to the
     * previous page, until we reach the start of the file.
     */
    restart = F_ISSET(cbt, WT_CBT_ITERATE_RETRY_PREV);
    F_CLR(cbt, WT_CBT_ITERATE_RETRY_PREV);
    for (newpage = false;; newpage = true, restart = false) {
        /* Calls with key only flag should never restart. */
        WT_ASSERT(session, !F_ISSET(&cbt->iface, WT_CURSTD_KEY_ONLY) || !restart);
        page = cbt->ref == NULL ? NULL : cbt->ref->page;

        if (page != NULL) {
            switch (page->type) {
            case WT_PAGE_ROW_LEAF:
                ret = __cursor_row_prev(cbt, newpage, restart, &skipped, &key_out_of_bounds);
                total_skipped += skipped;
                break;
            default:
                WT_ERR(__wt_illegal_value(session, page->type));
            }
            if (ret != WT_NOTFOUND)
                break;
        }

        /*
         * If we are doing an operation with bounds set, we need to check if we have exited the prev
         * function due to the key being out of bounds. If so, we break instead of walking onto the
         * prev page. We're not directly returning here to allow the cursor to be reset first before
         * we return WT_NOTFOUND.
         */
        if (key_out_of_bounds)
            break;

        /*
         * If we saw a lot of deleted records on this page, or we went all the way through a page
         * and only saw deleted records, try to evict the page when we release it. Otherwise
         * repeatedly deleting from the beginning of a tree can have quadratic performance. Take
         * care not to force eviction of pages that are genuinely empty, in new trees.
         *
         * A visible stop timestamp could have been treated as a tombstone and accounted in the
         * deleted count. Such a page might not have any new updates and be clean, but could benefit
         * from reconciliation getting rid of the obsolete content. Hence mark the page dirty to
         * force it through reconciliation.
         */
        if (page != NULL &&
          (cbt->page_deleted_count > WT_BTREE_DELETE_THRESHOLD ||
            (newpage && cbt->page_deleted_count > 0))) {
            WT_ERR(__wt_page_dirty_and_evict_soon(session, cbt->ref));
            WT_STAT_CONN_INCR(session, eviction_force_delete);
        }
        cbt->page_deleted_count = 0;

        if (F_ISSET(cbt, WT_CBT_READ_ONCE))
            LF_SET(WT_READ_WONT_NEED);

        if (!F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT))
            LF_SET(WT_READ_VISIBLE_ALL);

        /*
         * If we are running with snapshot isolation, and not interested in returning tombstones, we
         * could potentially skip pages. The skip function looks at the aggregated timestamp
         * information to determine if something is visible on the page. If nothing is, the page is
         * skipped.
         */
        if (!F_ISSET(&cbt->iface, WT_CURSTD_KEY_ONLY) &&
          session->txn->isolation == WT_ISO_SNAPSHOT &&
          !F_ISSET(&cbt->iface, WT_CURSTD_IGNORE_TOMBSTONE))
            WT_ERR(__wt_tree_walk_custom_skip(
              session, &cbt->ref, __wt_btcur_skip_page, &walk_skip_stats, flags));
        else
            WT_ERR(__wt_tree_walk(session, &cbt->ref, flags));
        WT_ERR_TEST(cbt->ref == NULL, WT_NOTFOUND, false);
    }

done:
err:
    if (total_skipped != 0) {
        if (total_skipped < 100)
            WT_STAT_CONN_DSRC_INCR(session, cursor_prev_skip_lt_100);
        else
            WT_STAT_CONN_DSRC_INCR(session, cursor_prev_skip_ge_100);
    }

    WT_STAT_CONN_DSRC_INCRV(session, cursor_prev_skip_total, total_skipped);
    if (walk_skip_stats.total_del_pages_skipped != 0)
        WT_STAT_CONN_DSRC_INCRV(
          session, cursor_tree_walk_del_page_skip, walk_skip_stats.total_del_pages_skipped);
    if (walk_skip_stats.total_inmem_del_pages_skipped != 0)
        WT_STAT_CONN_DSRC_INCRV(session, cursor_tree_walk_inmem_del_page_skip,
          walk_skip_stats.total_inmem_del_pages_skipped);

    /*
     * If we positioned the cursor using bounds, which is similar to a search, update the read
     * latency histogram.
     *
     * This includes the traversal if need_walk is true.
     */
    if (repositioned)
        __wt_stat_usecs_hist_incr_opread(session, WT_CLOCKDIFF_US(__wt_clock(session), time_start));

    switch (ret) {
    case 0:
        if (F_ISSET(&cbt->iface, WT_CURSTD_KEY_ONLY))
            F_SET(cursor, WT_CURSTD_KEY_INT);
        else
            F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
#ifdef HAVE_DIAGNOSTIC
        /*
         * Skip key order check, if next is called after a prev returned a prepare conflict error,
         * i.e cursor has changed direction at a prepared update, hence current key returned could
         * be same as earlier returned key.
         *
         * eg: Initial data set : (2,3,...10) insert key 1 in a prepare transaction. loop on prev
         * will return 10,...3,2 and subsequent call to prev will return a prepare conflict. Now if
         * we call next key 2 will be returned which will be same as earlier returned key.
         *
         * Additionally, reset the cursor check when we are using read uncommitted isolation mode
         * and cross a page boundary. It's possible to see out-of-order keys when the earlier
         * returned key is removed and new keys are inserted at the end of the page.
         */
        if (!F_ISSET(cbt, WT_CBT_ITERATE_RETRY_NEXT)) {
            if (session->txn->isolation == WT_ISO_READ_UNCOMMITTED && newpage)
                __wt_cursor_key_order_reset(cbt);
            ret = __wti_cursor_key_order_check(session, cbt, false);
        }

        if (need_walk) {
            /*
             * The bounds positioning code relies on the assumption that if we had to walk then we
             * can't possibly have walked to the upper bound. We check that assumption here by
             * comparing the upper bound with our current key or recno. Force inclusive to be false
             * so we don't consider the bound itself.
             */
            inclusive_set = F_ISSET(cursor, WT_CURSTD_BOUND_UPPER_INCLUSIVE);
            F_CLR(cursor, WT_CURSTD_BOUND_UPPER_INCLUSIVE);
            ret = __wt_compare_bounds(session, cursor, &cbt->iface.key, true, &key_out_of_bounds);
            WT_ASSERT(session, ret == 0 && !key_out_of_bounds);
            if (inclusive_set)
                F_SET(cursor, WT_CURSTD_BOUND_UPPER_INCLUSIVE);
        }
#endif
        break;
    case WT_PREPARE_CONFLICT:
        /*
         * If prepare conflict occurs, cursor should not be reset unless they have bounds and were
         * being initially positioned, as the current cursor position will be reused in case of a
         * retry from user.
         *
         * Bounded cursors don't lose their bounds if the reset call is internal, per the API.
         * Additionally by resetting the cursor here we have a slightly different semantic to a
         * traditional prepare conflict. We are giving up the page which may allow to be evicted but
         * for the purposes of the bounded cursor this should be fine.
         */
        if (repositioned)
            WT_TRET(__cursor_reset(cbt));
        else
            F_SET(cbt, WT_CBT_ITERATE_RETRY_PREV);
        break;
    default:
        WT_TRET(__cursor_reset(cbt));
    }
    F_CLR(cbt, WT_CBT_ITERATE_RETRY_NEXT);

    if (ret == 0)
        WT_RET(__wti_btcur_evict_reposition(cbt));

    return (ret);
}
