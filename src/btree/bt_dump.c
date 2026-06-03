/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Walker state. Caller fills emit + emit_ctx + read_corrupt; the walker accumulates the first error
 * encountered into dump_err so the caller can return a non-zero exit even when iteration completed
 * via the read_corrupt skip path.
 */
typedef struct {
    int (*emit)(WT_SESSION_IMPL *, WT_ITEM *key, WT_ITEM *value, void *);
    void *emit_ctx;
    bool read_corrupt;
    int dump_err;
} WT_DSTUFF;

static int __dump_tree(WT_SESSION_IMPL *, WT_REF *, WT_DSTUFF *);

/*
 * __dump_row_leaf --
 *     Emit each key/value pair on a row-store leaf via the caller's emit callback.
 */
static int
__dump_row_leaf(WT_SESSION_IMPL *session, WT_PAGE *page, WT_DSTUFF *ds)
{
    WT_CELL_UNPACK_KV unpack;
    WT_DECL_ITEM(key);
    WT_DECL_ITEM(value);
    WT_DECL_RET;
    WT_ROW *rip;
    uint32_t i;

    WT_RET(__wt_scr_alloc(session, 0, &key));
    WT_ERR(__wt_scr_alloc(session, 0, &value));

    WT_ROW_FOREACH (page, rip, i) {
        WT_ERR(__wt_row_leaf_key(session, page, rip, key, false));
        __wt_row_leaf_value_cell(session, page, rip, &unpack);
        WT_ERR(__wt_page_cell_data_ref_kv(session, page, &unpack, value));

        WT_ERR(ds->emit(session, key, value, ds->emit_ctx));
    }

err:
    __wt_scr_free(session, &key);
    __wt_scr_free(session, &value);
    return (ret);
}

/*
 * __dump_tree --
 *     Walk the btree below ref. On a leaf, emit each record via the caller's callback. On an
 *     internal page, recurse into each child subtree; if a child's __wt_page_in fails and
 *     read_corrupt is set, skip that subtree and continue with siblings. Modeled on __verify_tree
 *     in the btree verify code - the verify pattern for read_corrupt is the canonical reference.
 */
static int
__dump_tree(WT_SESSION_IMPL *session, WT_REF *ref, WT_DSTUFF *ds)
{
    WT_DECL_RET;
    WT_PAGE *page;
    WT_REF *child_ref;

    page = ref->page;

    switch (page->type) {
    case WT_PAGE_ROW_INT:
        WT_INTL_FOREACH_BEGIN (session, page, child_ref) {
            ret = __wt_page_in(session, child_ref, 0);
            if (ret != 0) {
                if (!ds->read_corrupt)
                    WT_RET(ret);
                if (ds->dump_err == 0)
                    ds->dump_err = ret;
                continue;
            }
            ret = __dump_tree(session, child_ref, ds);
            WT_TRET(__wt_page_release(session, child_ref, 0));
            WT_RET(ret);
        }
        WT_INTL_FOREACH_END;
        break;
    case WT_PAGE_ROW_LEAF:
        WT_RET(__dump_row_leaf(session, page, ds));
        break;
    default:
        /*
         * Column-store and FLCS are not in scope for the MVP. The caller is expected to route those
         * URIs to the cursor-based dump path.
         */
        WT_RET_MSG(session, ENOTSUP, "dump tree walker does not yet support page type %s",
          __wt_page_type_string(page->type));
    }

    return (0);
}

/*
 * __wt_dump_tree --
 *     Walk the btree backing uri, handing each record to the caller's emit callback. Under
 *     read_corrupt mode, skip subtrees that cannot be loaded (corrupt internal or leaf pages) and
 *     continue with siblings. Returns 0 if the walk completed cleanly; returns the first error
 *     encountered during a corrupt-skip walk; returns a propagated error if read_corrupt is off and
 *     a page load failed.
 *
 * Acquires and releases the dhandle internally - the caller must not be holding it.
 */
int
__wt_dump_tree(WT_SESSION_IMPL *session, const char *uri,
  int (*emit)(WT_SESSION_IMPL *, WT_ITEM *, WT_ITEM *, void *), void *emit_ctx, bool read_corrupt)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_DSTUFF ds;
    bool dhandle_held;

    WT_CLEAR(ds);
    ds.emit = emit;
    ds.emit_ctx = emit_ctx;
    ds.read_corrupt = read_corrupt;
    ds.dump_err = 0;
    dhandle_held = false;

    WT_ERR(__wt_session_get_dhandle(session, uri, NULL, NULL, 0));
    dhandle_held = true;
    btree = S2BT(session);

    WT_WITH_PAGE_INDEX(session, ret = __dump_tree(session, &btree->root, &ds));

err:
    if (dhandle_held)
        WT_TRET(__wt_session_release_dhandle(session));

    return (ret == 0 ? ds.dump_err : ret);
}
