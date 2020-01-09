/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
static int __dump_tree(WT_SESSION_IMPL *session, WT_REF *ref);
static int  dump_cell_data(WT_SESSION_IMPL *, const char *, const char *, const void *, size_t);

int
__wt_dump(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_BM *bm;
    WT_BTREE *btree;
    WT_CKPT ckpt_base, *ckpt;
    WT_DECL_RET;
    size_t root_addr_size;
    uint8_t root_addr[WT_BTREE_MAX_ADDR_COOKIE];
    const char *name;

    WT_UNUSED(cfg);
    btree = S2BT(session);
    bm = btree->bm;
    ckpt = NULL;
    name = session->dhandle->name;
    /*
     * Grab checkpoint file information
     */
    ret = __wt_meta_checkpoint(session, name, NULL, &ckpt_base);
    if (ret == WT_NOTFOUND) {
        ret = 0;
        goto done;
    }
    ckpt = &ckpt_base;
    WT_ERR(ret);

    /* Load the checkpoint. */
    WT_ERR(bm->checkpoint_load(
      bm, session, ckpt->raw.data, ckpt->raw.size, root_addr, &root_addr_size, true));

    /* Skip trees with no root page. */
    if (root_addr_size != 0) {
        WT_ERR(__wt_btree_tree_open(session, root_addr, root_addr_size));

        __wt_evict_file_exclusive_off(session);

        WT_WITH_PAGE_INDEX(session, ret = __dump_tree(session, &btree->root));

        /*
         * We have an exclusive lock on the handle, but we're swapping root pages in-and-out of that
         * handle, and there's a race with eviction entering the tree and seeing an invalid root
         * page. Eviction must work on trees being verified (else we'd have to do our own eviction),
         * lock eviction out whenever we're loading a new root page. This loops works because we are
         * called with eviction locked out, so we release the lock at the top of the loop and
         * re-acquire it here.
         */
        WT_TRET(__wt_evict_file_exclusive_on(session));
        WT_TRET(__wt_evict_file(session, WT_SYNC_DISCARD));
    }

done:
err:
    return (ret);
}

static int
__dump_tree(WT_SESSION_IMPL *session, WT_REF *ref)
{

    WT_BTREE *btree;
    WT_CELL_UNPACK *unpack, _unpack;
    WT_DECL_RET;
    WT_PAGE *page;
    WT_ROW *rip;
    const WT_PAGE_HEADER *dsk;
    WT_REF *child_ref;
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(val);
    uint32_t i;

    btree = S2BT(session);
    unpack = &_unpack;
    page = ref->page;

    WT_RET(__wt_scr_alloc(session, 256, &key));
    WT_RET(__wt_scr_alloc(session, 256, &val));
    /*
     * Check overflow pages and timestamps. Done in one function as both checks require walking the
     * page cells and we don't want to do it twice.
     */
    switch (page->type) {
    case WT_PAGE_ROW_LEAF:
        if ((dsk = ref->page->dsk) == NULL) {
            return (0);
        }

        WT_ROW_FOREACH (page, rip, i) {
            WT_ERR(__wt_row_leaf_key(session, page, rip, key, false));
            WT_ERR(dump_cell_data(session, btree->key_format, "K:", key->data, key->size));

            __wt_row_leaf_value_cell(session, page, rip, NULL, unpack);
            WT_ERR(__wt_page_cell_data_ref(session, page, unpack, val));
            switch (unpack->raw) {
            case WT_CELL_VALUE:
            case WT_CELL_VALUE_COPY:
            case WT_CELL_VALUE_OVFL:
            case WT_CELL_VALUE_SHORT:
                WT_ERR(dump_cell_data(session, btree->key_format, "V:", val->data, val->size));
                break;
            default:
                WT_ERR(__wt_illegal_value(session, unpack->raw));
            }
            WT_RET(
              __wt_msg(session, "T: <%" PRIu64 ", %" PRIu64 ">", unpack->start_ts, unpack->stop_ts));
        }
        break;
    }

    /* Check tree connections and recursively descend the tree. */
    switch (page->type) {
    case WT_PAGE_ROW_INT:
        /* For each entry in an internal page, verify the subtree. */
        WT_INTL_FOREACH_BEGIN (session, page, child_ref) {
            /*
             * It's a depth-first traversal: this entry's starting key should be larger than the
             * largest key previously reviewed.
             *
             */

            /* Iterate through tree */
            WT_RET(__wt_page_in(session, child_ref, 0));
            ret = __dump_tree(session, child_ref);
            WT_TRET(__wt_page_release(session, child_ref, 0));
            WT_RET(ret);
        }
        WT_INTL_FOREACH_END;
        break;
    }
err:
    __wt_scr_free(session, &key);
    __wt_scr_free(session, &val);
    return (0);
}

static int
dump_cell_data(
  WT_SESSION_IMPL *session, const char *format, const char *tag, const void *data_arg, size_t size)
{
    WT_DECL_ITEM(a);
    WT_DECL_ITEM(b);
    WT_DECL_RET;

    WT_ERR(__wt_scr_alloc(session, 512, &a));
    WT_ERR(__wt_scr_alloc(session, 512, &b));

    if (size == 0)
        WT_RET(__wt_msg(session, "%s%s{}", tag == NULL ? "" : tag, tag == NULL ? "" : " "));

    if (WT_STREQ(format, "S") && ((char *)data_arg)[size - 1] != '\0') {
        WT_RET(__wt_buf_fmt(session, a, "%.*s", (int)size, (char *)data_arg));
        data_arg = a->data;
        size = a->size + 1;
    }

    WT_RET(__wt_msg(session, "%s%s%s", tag == NULL ? "" : tag, tag == NULL ? "" : " ",
      __wt_buf_set_printable_format(session, data_arg, size, format, b)));

err:
    __wt_scr_free(session, &a);
    __wt_scr_free(session, &b);
    return (ret);
}