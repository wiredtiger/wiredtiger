/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
/*
 * __bulk_row_keycmp_err --
 *     Error routine when row-store keys inserted out-of-order.
 */
static int
__bulk_row_keycmp_err(WT_CURSOR_BULK *cbulk)
{
    WT_CURSOR *cursor;
    WT_DECL_ITEM(a);
    WT_DECL_ITEM(b);
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    cursor = &cbulk->cbt.iface;
    session = CUR2S(cbulk);

    WT_ERR(__wt_scr_alloc(session, 512, &a));
    WT_ERR(__wt_scr_alloc(session, 512, &b));

    WT_ERR_MSG(session, EINVAL,
      "bulk-load presented with out-of-order keys: %s is less than or equal to the previously "
      "inserted "
      "key %s",
      __wt_buf_set_printable(session, cursor->key.data, cursor->key.size, false, a),
      __wt_buf_set_printable(session, cbulk->last->data, cbulk->last->size, false, b));

err:
    __wt_scr_free(session, &a);
    __wt_scr_free(session, &b);
    return (ret);
}

/*
 * __curbulk_insert_row --
 *     Row-store bulk cursor insert, with key-sort checks.
 */
static int
__curbulk_insert_row(WT_CURSOR *cursor)
{
    WT_BTREE *btree;
    WT_CURSOR_BULK *cbulk;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;
    int cmp;

    cbulk = (WT_CURSOR_BULK *)cursor;
    btree = CUR2BT(&cbulk->cbt);

    /*
     * Bulk cursor inserts are updates, but don't need auto-commit transactions because they are
     * single-threaded and not visible until the bulk cursor is closed.
     */
    CURSOR_API_CALL(cursor, session, ret, insert, btree);
    WT_STAT_CONN_DSRC_INCR(session, cursor_insert_bulk);

    WT_ERR(__cursor_checkkey(cursor));
    WT_ERR(__cursor_checkvalue(cursor));

    /*
     * If this isn't the first key inserted, compare it against the last key to ensure the
     * application doesn't accidentally corrupt the table.
     */
    if (!cbulk->first_insert) {
        WT_ERR(__wt_compare(session, btree->collator, &cursor->key, cbulk->last, &cmp));
        if (cmp <= 0)
            WT_ERR(__bulk_row_keycmp_err(cbulk));
    } else
        cbulk->first_insert = false;

    /* Save a copy of the key for the next comparison. */
    WT_ERR(__wt_buf_set(session, cbulk->last, cursor->key.data, cursor->key.size));

    ret = __wt_bulk_insert_row(session, cbulk);

err:
    API_END_RET(session, ret);
}

/*
 * __curbulk_insert_row_skip_check --
 *     Row-store bulk cursor insert, without key-sort checks.
 */
static int
__curbulk_insert_row_skip_check(WT_CURSOR *cursor)
{
    WT_BTREE *btree;
    WT_CURSOR_BULK *cbulk;
    WT_DECL_RET;
    WT_SESSION_IMPL *session;

    cbulk = (WT_CURSOR_BULK *)cursor;
    btree = CUR2BT(&cbulk->cbt);

    /*
     * Bulk cursor inserts are updates, but don't need auto-commit transactions because they are
     * single-threaded and not visible until the bulk cursor is closed.
     */
    CURSOR_API_CALL(cursor, session, ret, insert, btree);
    WT_STAT_CONN_DSRC_INCR(session, cursor_insert_bulk);

    WT_ERR(__cursor_checkkey(cursor));
    WT_ERR(__cursor_checkvalue(cursor));

    ret = __wt_bulk_insert_row(session, cbulk);

err:
    API_END_RET(session, ret);
}

/*
 * __wti_curbulk_init --
 *     Initialize a bulk cursor.
 */
int
__wti_curbulk_init(
  WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk, bool bitmap, bool skip_sort_check)
{
    WT_CURSOR *cursor;
    WT_CURSOR_BTREE *cbt;

    cursor = &cbulk->cbt.iface;
    cbt = &cbulk->cbt;
    WT_UNUSED(bitmap);
    /* Bulk cursors only support insert and close (reset is a no-op). */
    __wti_cursor_set_notsup(cursor);
    switch (CUR2BT(cbt)->type) {
    case BTREE_ROW:
        /*
         * Row-store order comparisons are expensive, so we optionally skip them when we know the
         * input is correct.
         */
        cursor->insert = skip_sort_check ? __curbulk_insert_row_skip_check : __curbulk_insert_row;
        break;
    }

    cbulk->first_insert = true;
    /* The bulk last buffer is used to detect out-of-order keys in row-store to avoid corruption. */
    WT_RET(__wt_scr_alloc(session, 100, &cbulk->last));

    return (__wt_bulk_init(session, cbulk));
}

/*
 * __wti_curbulk_close --
 *     Close a bulk cursor.
 */
int
__wti_curbulk_close(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
{
    WT_DECL_RET;

    ret = __wt_bulk_wrapup(session, cbulk);
    if (ret == 0)
        WT_STAT_CONN_DECR_ATOMIC(session, cursor_bulk_count);

    __wt_scr_free(session, &cbulk->last);
    return (ret);
}
