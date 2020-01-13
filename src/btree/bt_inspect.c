/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __dump_tree(WT_SESSION_IMPL *, WT_REF *);
static int __dump_cell_data(WT_SESSION_IMPL *, const char *, const char *, WT_ITEM *);
static int __dump_page_col_var(WT_SESSION_IMPL *, WT_REF *);
static int __dump_page_row_leaf(WT_SESSION_IMPL *, WT_PAGE *);
static int __dump_page_col_fix(WT_SESSION_IMPL *, const WT_PAGE_HEADER *);
/*
 * __wt_dump --
 *     Dump the table, by iterating through btree and printing all K/V pairs and the timestamp
 *     information.
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
 *     Dump the tree, recursively descend through it in depth-first fashion. Then print all the K/V
 *     pairs and along with the timestamp information that is present in each leaf page. The page
 *     argument was physically verified (so we know it's correctly formed), and the in-memory
 *     version built.
 */
static int
__dump_tree(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_DECL_RET;
    WT_PAGE *page;
    const WT_PAGE_HEADER *dsk;
    WT_REF *child_ref;

    page = ref->page;

    /*
     * If a tree is empty (just created), it won't have a disk image; if there is no disk image,
     * we're done.
     */
    if ((dsk = ref->page->dsk) == NULL) {
        return (0);
    }

    /*
     * Dump the leaf pages
     */
    switch (page->type) {
    case WT_PAGE_COL_FIX:
        WT_RET(__dump_page_col_fix(session, dsk));
        break;
    case WT_PAGE_COL_VAR:
        WT_RET(__dump_page_col_var(session, ref));
        break;
    case WT_PAGE_ROW_LEAF:
        WT_RET(__dump_page_row_leaf(session, page));
        break;
    }

    /* recursively descend the tree. */
    switch (page->type) {
    case WT_PAGE_COL_INT:
    case WT_PAGE_ROW_INT:
        /* For each entry in an internal page, iterate through the subtree. */
        WT_INTL_FOREACH_BEGIN (session, page, child_ref) {
            /* Iterate through tree using depth-first traversal */
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
 * __dump_page_col_fix --
 *     Dump the leaf page in a column store tree. Iterates through each column cell and unpacks the
 *     content of the cell to print the K/V pairs
 */
static int
__dump_page_col_fix(WT_SESSION_IMPL *session, const WT_PAGE_HEADER *dsk)
{
    WT_BTREE *btree;
    uint64_t recno;
    uint32_t i;
    uint8_t v;

    btree = S2BT(session);

    WT_FIX_FOREACH (btree, dsk, v, i) {
        WT_RET(__wt_msg(session, "\t%" PRIu64 "\t{", dsk->recno));
        WT_RET(__wt_msg(session, "#%c%c", __wt_hex((v & 0xf0) >> 4), __wt_hex(v & 0x0f)));
        WT_RET(__wt_msg(session, "}\n"));
        ++recno;
    }
    return (0);
}

/*
 * __dump_page_col_var --
 *     Dump the leaf page in a column store tree. Iterates through each column cell and unpacks the
 *     content of the cell to print the K/V pairs
 */
static int
__dump_page_col_var(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_BTREE *btree;
    WT_CELL *cell;
    WT_CELL_UNPACK *unpack, _unpack;
    WT_COL *cip;
    WT_DECL_ITEM(val);
    WT_DECL_RET;
    WT_PAGE *page;
    uint64_t recno, rle;
    uint32_t i;
    char ts_string[2][WT_TS_INT_STRING_SIZE];

    btree = S2BT(session);
    page = ref->page;
    recno = ref->ref_recno;
    unpack = &_unpack;

    WT_ERR(__wt_scr_alloc(session, 256, &val));

    WT_COL_FOREACH (page, cip, i) {
        cell = WT_COL_PTR(page, cip);
        __wt_cell_unpack(session, page, cell, unpack);
        rle = __wt_cell_rle(unpack);
        if (unpack->raw == WT_CELL_DEL) {
            recno += rle;
            continue;
        }
        WT_ERR(__wt_msg(session, "K: %" PRIu64, recno));
        switch (unpack->raw) {
        case WT_CELL_VALUE:
        case WT_CELL_VALUE_COPY:
        case WT_CELL_VALUE_OVFL:
        case WT_CELL_VALUE_SHORT:
            WT_ERR(__wt_page_cell_data_ref(session, page, unpack, val));
            WT_ERR(__dump_cell_data(session, btree->value_format, "V:", val));
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
    __wt_scr_free(session, &val);
    return (ret);
}

/*
 * __dump_page_row_leaf --
 *     Dump the leaf page in a row store btree. Iterates through each row cell and unpacks the
 *     content of the cell to print the K/V pairs and timestamp information
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
        WT_ERR(__dump_cell_data(session, btree->key_format, "K:", key));

        __wt_row_leaf_value_cell(session, page, rip, NULL, unpack);
        switch (unpack->raw) {
        case WT_CELL_VALUE:
        case WT_CELL_VALUE_COPY:
        case WT_CELL_VALUE_OVFL:
        case WT_CELL_VALUE_SHORT:
            WT_ERR(__wt_page_cell_data_ref(session, page, unpack, val));
            WT_ERR(__dump_cell_data(session, btree->value_format, "V:", val));
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
 *     Dump the cell data, with configurations of format, print the contents of the cell.
 */
static int
__dump_cell_data(WT_SESSION_IMPL *session, const char *format, const char *tag, WT_ITEM *item)
{
    WT_DECL_ITEM(a);
    WT_DECL_ITEM(b);
    WT_DECL_RET;

    WT_ERR(__wt_scr_alloc(session, 512, &a));
    WT_ERR(__wt_scr_alloc(session, 512, &b));

    if (item->size == 0)
        WT_ERR(__wt_msg(session, "%s%s", tag == NULL ? "" : tag, tag == NULL ? "" : " "));

    if (WT_STREQ(format, "S") && ((char *)item->data)[item->size - 1] != '\0') {
        WT_ERR(__wt_buf_fmt(session, a, "%.*s", (int)item->size, (char *)item->data));
        item->data = a->data;
        item->size = a->size + 1;
    }

    WT_ERR(__wt_msg(session, "%s%s%s", tag == NULL ? "" : tag, tag == NULL ? "" : " ",
      __wt_buf_set_printable_format(session, item->data, item->size, format, b)));

err:
    __wt_scr_free(session, &a);
    __wt_scr_free(session, &b);
    return (ret);
}