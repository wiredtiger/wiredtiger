/*-
 * Copyright (c) 2014-2019 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __dump_tree(WT_SESSION_IMPL *, WT_REF *);

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
    case WT_PAGE_COL_VAR:
    case WT_PAGE_ROW_LEAF:
        WT_RET(__wt_debug_page(session, NULL, ref, NULL));
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