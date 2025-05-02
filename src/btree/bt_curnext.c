/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __cursor_row_next --
 *     Move to the next row-store item.
 */
static WT_INLINE int
__cursor_row_next(
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
    *key_out_of_boundsp = false;
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
        /*
         * Be paranoid and set the slot out of bounds when moving to a new page.
         */
        cbt->slot = UINT32_MAX;
        cbt->ins_head = WT_ROW_INSERT_SMALLEST(page);
        cbt->ins = WT_SKIP_FIRST(cbt->ins_head);
        cbt->row_iteration_slot = 1;
        cbt->rip_saved = NULL;
        goto new_insert;
    }

    /* Move to the next entry and return the item. */
    for (;;) {
        /*
         * Continue traversing any insert list; maintain the insert list head reference and entry
         * count in case we switch to a cursor previous movement.
         */
        if (cbt->ins != NULL)
            cbt->ins = WT_SKIP_NEXT(cbt->ins);

new_insert:
        cbt->iter_retry = WT_CBT_RETRY_INSERT;
restart_read_insert:
        if ((ins = cbt->ins) != NULL) {
            key->data = WT_INSERT_KEY(ins);
            key->size = WT_INSERT_KEY_SIZE(ins);

            /*
             * If an upper bound has been set ensure that the key is within the range, otherwise
             * early exit.
             */
            if ((ret = __wt_btcur_bounds_early_exit(session, cbt, true, key_out_of_boundsp)) ==
              WT_NOTFOUND)
                WT_STAT_CONN_DSRC_INCR(session, cursor_bounds_next_early_exit);
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

        /* Check for the end of the page. */
        if (cbt->row_iteration_slot >= page->entries * 2 + 1)
            return (WT_NOTFOUND);
        ++cbt->row_iteration_slot;

        /*
         * Odd-numbered slots configure as WT_INSERT_HEAD entries, even-numbered slots configure as
         * WT_ROW entries.
         */
        if (cbt->row_iteration_slot & 0x01) {
            cbt->ins_head = WT_ROW_INSERT_SLOT(page, cbt->row_iteration_slot / 2 - 1);
            cbt->ins = WT_SKIP_FIRST(cbt->ins_head);
            goto new_insert;
        }
        cbt->ins_head = NULL;
        cbt->ins = NULL;

        cbt->iter_retry = WT_CBT_RETRY_PAGE;
        cbt->slot = cbt->row_iteration_slot / 2 - 1;
restart_read_page:
        rip = &page->pg_row[cbt->slot];
        /*
         * The saved cursor key from the slot is used later to match the prefix match or get the
         * value from the history store if the on-disk data is not visible.
         */
        WT_RET(__cursor_row_slot_key_return(cbt, rip, &kpack));

        /*
         * If an upper bound has been set ensure that the key is within the range, otherwise early
         * exit.
         */
        if ((ret = __wt_btcur_bounds_early_exit(session, cbt, true, key_out_of_boundsp)) ==
          WT_NOTFOUND)
            WT_STAT_CONN_DSRC_INCR(session, cursor_bounds_next_early_exit);
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

#ifdef HAVE_DIAGNOSTIC
/*
 * __cursor_key_order_check_row --
 *     Check key ordering for row-store cursor movements.
 */
static int
__cursor_key_order_check_row(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, bool next)
{
    WT_BTREE *btree;
    WT_DECL_ITEM(a);
    WT_DECL_ITEM(b);
    WT_DECL_RET;
    WT_ITEM *key;
    int cmp;

    btree = S2BT(session);
    key = &cbt->iface.key;
    cmp = 0; /* -Werror=maybe-uninitialized */

    if (cbt->lastkey->size != 0)
        WT_RET(__wt_compare(session, btree->collator, cbt->lastkey, key, &cmp));

    if (cbt->lastkey->size == 0 || (next && cmp < 0) || (!next && cmp > 0)) {
        cbt->lastref = cbt->ref;
        cbt->lastslot = cbt->slot;
        cbt->lastins = cbt->ins;
        return (__wt_buf_set(session, cbt->lastkey, cbt->iface.key.data, cbt->iface.key.size));
    }

    WT_ERR(__wt_scr_alloc(session, 512, &a));
    WT_ERR(__wt_scr_alloc(session, 512, &b));

    __wt_verbose_error(session, WT_VERB_OUT_OF_ORDER,
      "WT_CURSOR.%s out-of-order returns: returned key %.1024s then key %.1024s",
      next ? "next" : "prev",
      __wt_buf_set_printable_format(
        session, cbt->lastkey->data, cbt->lastkey->size, btree->key_format, false, a),
      __wt_buf_set_printable_format(session, key->data, key->size, btree->key_format, false, b));
    WT_ERR(__wt_msg(session, "dumping the tree"));
    WT_WITH_BTREE(session, btree, ret = __wt_debug_tree_all(session, NULL, NULL, NULL));
    WT_ERR_PANIC(session, EINVAL, "found key out-of-order returns");

err:
    __wt_scr_free(session, &a);
    __wt_scr_free(session, &b);

    return (ret);
}

/*
 * __wti_cursor_key_order_check --
 *     Check key ordering for cursor movements.
 */
int
__wti_cursor_key_order_check(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, bool next)
{
    switch (cbt->ref->page->type) {
    case WT_PAGE_ROW_LEAF:
        return (__cursor_key_order_check_row(session, cbt, next));
    default:
        return (__wt_illegal_value(session, cbt->ref->page->type));
    }
    /* NOTREACHED */
}

/*
 * __wt_cursor_key_order_init --
 *     Initialize key ordering checks for cursor movements after a successful search.
 */
int
__wt_cursor_key_order_init(WT_CURSOR_BTREE *cbt)
{
    WT_SESSION_IMPL *session = CUR2S(cbt);

    cbt->lastref = cbt->ref;
    cbt->lastslot = cbt->slot;
    cbt->lastins = cbt->ins;

    /*
     * Cursor searches set the position for cursor movements, set the last-key value for diagnostic
     * checking.
     */
    switch (cbt->ref->page->type) {
    case WT_PAGE_ROW_LEAF:
        return (__wt_buf_set(session, cbt->lastkey, cbt->iface.key.data, cbt->iface.key.size));
    default:
        return (__wt_illegal_value(session, cbt->ref->page->type));
    }
    /* NOTREACHED */
}

/*
 * __wt_cursor_key_order_reset --
 *     Turn off key ordering checks for cursor movements.
 */
void
__wt_cursor_key_order_reset(WT_CURSOR_BTREE *cbt)
{
    /*
     * Clear the last-key returned, it doesn't apply.
     */
    if (cbt->lastkey != NULL)
        cbt->lastkey->size = 0;
    cbt->lastref = NULL;
    cbt->lastslot = UINT32_MAX;
    cbt->lastins = NULL;
}
#endif

/*
 * __wti_btcur_iterate_setup --
 *     Initialize a cursor for iteration, usually based on a search.
 */
void
__wti_btcur_iterate_setup(WT_CURSOR_BTREE *cbt)
{
    WT_PAGE *page;

    /*
     * We don't currently have to do any setup when we switch between next and prev calls, but I'm
     * sure we will someday -- I'm leaving support here for both flags for that reason.
     */
    F_SET(cbt, WT_CBT_ITERATE_NEXT | WT_CBT_ITERATE_PREV);

    /* Clear the count of deleted items on the page. */
    cbt->page_deleted_count = 0;

    /* Clear saved iteration cursor position information. */
    cbt->rip_saved = NULL;

    /*
     * If we don't have a search page, then we're done, we're starting at the beginning or end of
     * the tree, not as a result of a search.
     */
    if (cbt->ref == NULL) {
#ifdef HAVE_DIAGNOSTIC
        __wt_cursor_key_order_reset(cbt);
#endif
        return;
    }

    page = cbt->ref->page;
    WT_ASSERT(NULL, page->type == WT_PAGE_ROW_LEAF);
    /*
     * For row-store pages, we need a single item that tells us the part of the page we're
     * walking (otherwise switching from next to prev and vice-versa is just too complicated),
     * so we map the WT_ROW and WT_INSERT_HEAD insert array slots into a single name space: slot
     * 1 is the "smallest key insert list", slot 2 is WT_ROW[0], slot 3 is WT_INSERT_HEAD[0],
     * and so on. This means WT_INSERT lists are odd-numbered slots, and WT_ROW array slots are
     * even-numbered slots.
     */
    cbt->row_iteration_slot = (cbt->slot + 1) * 2;
    if (cbt->ins_head != NULL) {
        if (cbt->ins_head == WT_ROW_INSERT_SMALLEST(page))
            cbt->row_iteration_slot = 1;
        else
            cbt->row_iteration_slot += 1;
    }
}

/*
 * __wt_btcur_next --
 *     Move to the next record in the tree.
 */
int
__wt_btcur_next(WT_CURSOR_BTREE *cbt, bool truncating)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_PAGE_WALK_SKIP_STATS walk_skip_stats;
    WT_SESSION_IMPL *session;
    size_t skipped, total_skipped;
    uint64_t time_start;
    uint32_t flags;
    bool key_out_of_bounds, need_walk, newpage, repositioned, restart;

    cursor = &cbt->iface;
    key_out_of_bounds = need_walk = newpage = repositioned = false;
    session = CUR2S(cbt);
    total_skipped = 0;
    walk_skip_stats.total_del_pages_skipped = 0;
    walk_skip_stats.total_inmem_del_pages_skipped = 0;
    WT_NOT_READ(time_start, 0);

    WT_STAT_CONN_DSRC_INCR(session, cursor_next);

    flags = WT_READ_NO_SPLIT | WT_READ_SKIP_INTL; /* tree walk flags */
    if (truncating)
        LF_SET(WT_READ_TRUNCATE);

    F_CLR(cursor, WT_CURSTD_KEY_SET | WT_CURSTD_VALUE_SET);

    WT_ERR(__wt_cursor_func_init(cbt, false));

    /*
     * If we have a bound set we should position our cursor appropriately if it isn't already
     * positioned. It is possible that the positioning function can directly return the record. For
     * that to happen, the cursor must be placed on a valid record and must be positioned on the
     * first record within the bounds. If the record is not valid or is not positioned within the
     * bounds, continue the next traversal logic.
     */
    if (F_ISSET(cursor, WT_CURSTD_BOUND_LOWER) && !WT_CURSOR_IS_POSITIONED(cbt)) {
        repositioned = true;
        time_start = __wt_clock(session);
        WT_ERR(__wti_btcur_bounds_position(session, cbt, true, &need_walk));
        if (!need_walk) {
            __wt_value_return(cbt, cbt->upd_value);
            goto done;
        }
    }

    /*
     * If we aren't already iterating in the right direction, there's some setup to do.
     */
    if (!F_ISSET(cbt, WT_CBT_ITERATE_NEXT))
        __wti_btcur_iterate_setup(cbt);

    /*
     * Walk any page we're holding until the underlying call returns not-found. Then, move to the
     * next page, until we reach the end of the file.
     */
    restart = F_ISSET(cbt, WT_CBT_ITERATE_RETRY_NEXT);
    F_CLR(cbt, WT_CBT_ITERATE_RETRY_NEXT);
    for (newpage = false;; newpage = true, restart = false) {
        WT_PAGE *page = cbt->ref == NULL ? NULL : cbt->ref->page;

        if (page != NULL) {
            switch (page->type) {
            case WT_PAGE_ROW_LEAF:
                ret = __cursor_row_next(cbt, newpage, restart, &skipped, &key_out_of_bounds);
                total_skipped += skipped;
                break;
            default:
                WT_ERR(__wt_illegal_value(session, page->type));
            }
            if (ret != WT_NOTFOUND)
                break;

            /*
             * If we are doing an operation when the cursor has bounds set, we need to check if we
             * have exited the next function due to the key being out of bounds. If so, we break
             * instead of walking onto the next page. We're not directly returning here to allow the
             * cursor to be reset first before we return WT_NOTFOUND.
             */
            if (key_out_of_bounds)
                break;
        }
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
        if (session->txn->isolation == WT_ISO_SNAPSHOT &&
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
            WT_STAT_CONN_DSRC_INCR(session, cursor_next_skip_lt_100);
        else
            WT_STAT_CONN_DSRC_INCR(session, cursor_next_skip_ge_100);
    }

    WT_STAT_CONN_DSRC_INCRV(session, cursor_next_skip_total, total_skipped);
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
        F_SET(cursor, WT_CURSTD_KEY_INT | WT_CURSTD_VALUE_INT);
#ifdef HAVE_DIAGNOSTIC
        /*
         * Skip key order check, if prev is called after a next returned a prepare conflict error,
         * i.e cursor has changed direction at a prepared update, hence current key returned could
         * be same as earlier returned key.
         *
         * eg: Initial data set : (1,2,3,...10) insert key 11 in a prepare transaction. loop on next
         * will return 1,2,3...10 and subsequent call to next will return a prepare conflict. Now if
         * we call prev key 10 will be returned which will be same as earlier returned key.
         *
         * Additionally, reset the cursor check when we are using read uncommitted isolation mode
         * and cross a page boundary. It's possible to see out-of-order keys when the earlier
         * returned key is removed and new keys are inserted at the start of the page.
         */
        if (!F_ISSET(cbt, WT_CBT_ITERATE_RETRY_PREV)) {
            if (session->txn->isolation == WT_ISO_READ_UNCOMMITTED && newpage) {
                __wt_cursor_key_order_reset(cbt);
            }
            ret = __wti_cursor_key_order_check(session, cbt, true);
        }

        if (need_walk) {
            /*
             * The bounds positioning code relies on the assumption that if we had to walk then we
             * can't possibly have walked to the lower bound. We check that assumption here by
             * comparing the lower bound with our current key or recno. Force inclusive to be false
             * so we don't consider the bound itself.
             */
            bool inclusive_set = F_ISSET(cursor, WT_CURSTD_BOUND_LOWER_INCLUSIVE);
            F_CLR(cursor, WT_CURSTD_BOUND_LOWER_INCLUSIVE);
            ret = __wt_compare_bounds(session, cursor, &cbt->iface.key, false, &key_out_of_bounds);
            WT_ASSERT(session, ret == 0 && !key_out_of_bounds);
            if (inclusive_set)
                F_SET(cursor, WT_CURSTD_BOUND_LOWER_INCLUSIVE);
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
            F_SET(cbt, WT_CBT_ITERATE_RETRY_NEXT);
        break;
    default:
        WT_TRET(__cursor_reset(cbt));
    }
    F_CLR(cbt, WT_CBT_ITERATE_RETRY_PREV);

    if (ret == 0)
        WT_RET(__wti_btcur_evict_reposition(cbt));

    return (ret);
}
