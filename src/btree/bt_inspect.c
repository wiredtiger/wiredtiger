/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __dump_tree(WT_SESSION_IMPL *, WT_REF *);
static int __dump_cell_data(WT_SESSION_IMPL *, const char *, const char *, const void *, size_t);
static int __dump_page_col_var(WT_SESSION_IMPL *, WT_REF *);
static int __dump_page_row_leaf(WT_SESSION_IMPL *, WT_PAGE *);

/*
 * __wt_dump --
 *     Dump the table
 */
int
__wt_dump(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_BTREE *btree;
    WT_DECL_RET;

    WT_UNUSED(cfg);
    btree = S2BT(session);

    WT_WITH_PAGE_INDEX(session, ret = __dump_tree(session, &btree->root));
    return (ret);
}

/*
 * __dump_tree --
 *     Dump the tree, by iterating through tree
 */
static int
__dump_tree(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_DECL_RET;
    WT_PAGE *page;
    const WT_PAGE_HEADER *dsk;
    WT_REF *child_ref;

    page = ref->page;

    if ((dsk = ref->page->dsk) == NULL) {
        return (0);
    }
    /*
     * Check overflow pages and timestamps. Done in one function as both checks require walking the
     * page cells and we don't want to do it twice.
     */
    switch (page->type) {
    case WT_PAGE_COL_VAR:
        WT_RET(__dump_page_col_var(session, ref));
        break;
    case WT_PAGE_ROW_LEAF:
        WT_RET(__dump_page_row_leaf(session, page));
        break;
    }

    /* Check tree connections and recursively descend the tree. */
    switch (page->type) {
    case WT_PAGE_COL_INT:
    case WT_PAGE_ROW_INT:
        /* For each entry in an internal page, verify the subtree. */
        WT_INTL_FOREACH_BEGIN (session, page, child_ref) {
            /* Iterate through tree using depth-firth traversal */
            WT_RET(__wt_page_in(session, child_ref, 0));
            ret = __dump_tree(session, child_ref);
            WT_TRET(__wt_page_release(session, child_ref, 0));
            WT_RET(ret);
        }
        WT_INTL_FOREACH_END;
        break;
    }
    return (0);
}

/*
 * __dump_page_col_var --
 *     Dump the leaf page in a column store tree.
 */
static int
__dump_page_col_var(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_BTREE *btree;
    WT_CELL *cell;
    WT_CELL_UNPACK *unpack, _unpack;
    WT_COL *cip;
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(val);
    WT_DECL_RET;
    WT_PAGE *page;
    uint64_t recno, rle;
    uint32_t i;
    char tag[64];
    char ts_string[2][WT_TS_INT_STRING_SIZE];

    btree = S2BT(session);
    page = ref->page;
    recno = ref->ref_recno;
    unpack = &_unpack;

    WT_ERR(__wt_scr_alloc(session, 256, &key));
    WT_ERR(__wt_scr_alloc(session, 256, &val));

    WT_COL_FOREACH (page, cip, i) {
        cell = WT_COL_PTR(page, cip);
        __wt_cell_unpack(session, page, cell, unpack);
        rle = __wt_cell_rle(unpack);
        if (unpack->raw == WT_CELL_DEL) {
            recno += rle;
            continue;
        }
        WT_ERR(__wt_snprintf(tag, sizeof(tag), "%" PRIu64 " %" PRIu64, recno, rle));
        WT_ERR(__wt_msg(session, "K: %" PRIu64, recno));
        switch (unpack->raw) {
        case WT_CELL_VALUE:
        case WT_CELL_VALUE_COPY:
        case WT_CELL_VALUE_OVFL:
        case WT_CELL_VALUE_SHORT:
            WT_ERR(__wt_page_cell_data_ref(session, page, unpack, val));
            WT_ERR(__dump_cell_data(session, btree->value_format, "V:", val->data, val->size));
            break;
        default:
            WT_ERR(__wt_illegal_value(session, unpack->raw));
        }
        WT_ERR(__wt_msg(session, "T: <%s:%" PRIu64 ", %s:%" PRIu64 ">",
          __wt_timestamp_to_string(unpack->start_ts, ts_string[0]), unpack->start_txn, 
          __wt_timestamp_to_string(unpack->stop_ts, ts_string[1]), unpack->stop_txn));
        recno += rle;
    }
err:
    __wt_scr_free(session, &key);
    __wt_scr_free(session, &val);
    return (ret);
}

/*
 * __dump_page_row_leaf --
 *     Dump the leaf page in a row store tree.
 */
static int
__dump_page_row_leaf(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_BTREE *btree;
    WT_CELL_UNPACK *unpack, _unpack;
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(val);
    WT_DECL_RET;
    WT_ROW *rip;
    uint32_t i;

    char ts_string[2][WT_TS_INT_STRING_SIZE];

    unpack = &_unpack;
    btree = S2BT(session);

    WT_ERR(__wt_scr_alloc(session, 256, &key));
    WT_ERR(__wt_scr_alloc(session, 256, &val));

    WT_ROW_FOREACH (page, rip, i) {
        WT_ERR(__wt_row_leaf_key(session, page, rip, key, false));
        WT_ERR(__dump_cell_data(session, btree->key_format, "K:", key->data, key->size));

        __wt_row_leaf_value_cell(session, page, rip, NULL, unpack);
        switch (unpack->raw) {
        case WT_CELL_VALUE:
        case WT_CELL_VALUE_COPY:
        case WT_CELL_VALUE_OVFL:
        case WT_CELL_VALUE_SHORT:
            WT_ERR(__wt_page_cell_data_ref(session, page, unpack, val));
            WT_ERR(__dump_cell_data(session, btree->value_format, "V:", val->data, val->size));
            break;
        default:
            WT_ERR(__wt_illegal_value(session, unpack->raw));
        }
        WT_ERR(__wt_msg(session, "T: <%s:%" PRIu64 ", %s:%" PRIu64 ">",
          __wt_timestamp_to_string(unpack->start_ts, ts_string[0]), unpack->start_txn, 
          __wt_timestamp_to_string(unpack->stop_ts, ts_string[1]), unpack->stop_txn));
    }

err:
    __wt_scr_free(session, &key);
    __wt_scr_free(session, &val);
    return (ret);
}

/*
 * __dump_cell_data --
 *     Dump the cell data
 */
static int
__dump_cell_data(
  WT_SESSION_IMPL *session, const char *format, const char *tag, const void *data_arg, size_t size)
{
    WT_DECL_ITEM(a);
    WT_DECL_ITEM(b);
    WT_DECL_RET;

    WT_ERR(__wt_scr_alloc(session, 512, &a));
    WT_ERR(__wt_scr_alloc(session, 512, &b));

    if (size == 0)
        WT_ERR(__wt_msg(session, "%s%s", tag == NULL ? "" : tag, tag == NULL ? "" : " "));

    if (WT_STREQ(format, "S") && ((char *)data_arg)[size - 1] != '\0') {
        WT_ERR(__wt_buf_fmt(session, a, "%.*s", (int)size, (char *)data_arg));
        data_arg = a->data;
        size = a->size + 1;
    }

    WT_ERR(__wt_msg(session, "%s%s%s", tag == NULL ? "" : tag, tag == NULL ? "" : " ",
      __wt_buf_set_printable_format(session, data_arg, size, format, b)));

err:
    __wt_scr_free(session, &a);
    __wt_scr_free(session, &b);
    return (ret);
}
