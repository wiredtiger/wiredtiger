/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * skunk_94 -- "touch" cursor POC.
 *
 * A touch cursor is fire-and-forget. Its WT_CURSOR::search descends the btree using
 * only internal pages and, instead of materializing the would-be leaf page into the
 * WiredTiger cache, forwards a non-returning warmup hint to the page log layer
 * (PALI). The caller always sees WT_NOTFOUND. The intent is to let storage classes
 * below WT (e.g. the SLS data movement layer behind PALI) act on the heuristic
 * without paying the WT-side leaf-read cost.
 */

#include "wt_internal.h"

/*
 * __touch_warmup_leaf --
 *     Issue a fire-and-forget warmup hint for the leaf page referenced by descent.
 *     Only meaningful when the btree sits on top of disaggregated storage; for any
 *     other storage class this is a no-op success.
 */
static int
__touch_warmup_leaf(WT_SESSION_IMPL *session, WT_REF *descent, const WT_ITEM *cmd)
{
    WT_ADDR_COPY addr;
    WT_BLOCK_DISAGG *block_disagg;
    WT_BLOCK_DISAGG_ADDRESS_COOKIE cookie;
    WT_BM *bm;
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_ITEM results_array[WT_DELTA_LIMIT + 1];
    WT_PAGE_LOG_GET_ARGS get_args;
    const uint8_t *p;
    uint32_t i, results_count;

    btree = S2BT(session);
    bm = btree->bm;

    if (!F_ISSET(btree, WT_BTREE_DISAGGREGATED) || bm == NULL)
        return (0);

    /* Pull the address cookie for the leaf without locking the WT_REF in place. */
    if (!__wt_ref_addr_copy(session, descent, &addr))
        return (0);
    if (addr.size == 0)
        return (0);

    /* Crack the cookie for the page id. */
    p = addr.addr;
    WT_RET(__wt_block_disagg_addr_unpack(session, &p, addr.size, &cookie));

    /* The block manager handle stores the PALI handle directly. */
    block_disagg = (WT_BLOCK_DISAGG *)bm->block;
    if (block_disagg == NULL || block_disagg->plhandle == NULL ||
      block_disagg->plhandle->plh_get == NULL)
        return (0);

    /*
     * Issue the warmup. Palite is configured to ignore the returned buffer set on this
     * code path; we still hand it the scratch array because the API mandates a non-null
     * results buffer. We do not allocate; palite must return zero results for warmup.
     */
    memset(results_array, 0, sizeof(results_array));
    memset(&get_args, 0, sizeof(get_args));
    get_args.flags = WT_PAGE_LOG_WARMUP;
    get_args.command = cmd;
    results_count = WT_DELTA_LIMIT + 1;
    ret = block_disagg->plhandle->plh_get(block_disagg->plhandle, &session->iface,
      cookie.page_id, 0, &get_args, results_array, &results_count);
    /*
     * The contract is that warmup returns zero results, but a faulty implementation
     * could still allocate. Free anything that came back so we don't leak.
     */
    for (i = 0; i < results_count; ++i)
        if (results_array[i].mem != NULL)
            __wt_free(session, results_array[i].mem);

    return (ret);
}

/*
 * __touch_descend --
 *     Walk down a row-store btree using only internal pages. Stops at the parent of
 *     the would-be leaf and returns *descentp pointing at the matching internal-index
 *     entry. The caller must release the held page (returned in *currentp) after the
 *     warmup hint has been issued.
 */
static int
__touch_descend(WT_SESSION_IMPL *session, WT_ITEM *srch_key, WT_REF **currentp, WT_REF **descentp)
{
    WT_BTREE *btree;
    WT_COLLATOR *collator;
    WT_DECL_RET;
    WT_ITEM item;
    WT_PAGE *page;
    WT_PAGE_INDEX *pindex;
    WT_REF *current, *descent;
    uint32_t base, indx, limit, read_flags;
    int cmp;

    btree = S2BT(session);
    collator = btree->collator;
    *currentp = NULL;
    *descentp = NULL;

    if (0) {
restart:
        WT_RET(__wt_page_release(session, current, 0));
    }

    current = &btree->root;
    for (pindex = NULL;;) {
        page = current->page;
        if (page->type != WT_PAGE_ROW_INT)
            break;

        WT_INTL_INDEX_GET(session, page, pindex);

        /*
         * Binary search the internal page. The 0th key on an internal page is the
         * "smallest" sentinel so always start at base = 1, exactly as __wt_row_search.
         */
        base = 1;
        limit = pindex->entries - 1;
        for (; limit != 0; limit >>= 1) {
            indx = base + (limit >> 1);
            descent = pindex->index[indx];
            __wt_ref_key(page, descent, &item.data, &item.size);

            WT_ERR(__wt_compare(session, collator, srch_key, &item, &cmp));
            if (cmp == 0) {
                base = indx + 1;
                break;
            }
            if (cmp > 0) {
                base = indx + 1;
                --limit;
            }
        }
        descent = pindex->index[base - 1];

        /*
         * If the child is a row-store leaf, we have found the leaf parent. Don't
         * swap into the leaf. The caller will issue the warmup and release current.
         */
        {
            uint8_t state = WT_REF_GET_STATE(descent);
            WT_PAGE *child = descent->page;
            bool child_is_leaf = false;
            WT_ADDR_COPY addr;

            if (state == WT_REF_MEM && child != NULL)
                child_is_leaf = (child->type == WT_PAGE_ROW_LEAF);
            else if (__wt_ref_addr_copy(session, descent, &addr))
                child_is_leaf =
                  (addr.type == WT_ADDR_LEAF || addr.type == WT_ADDR_LEAF_NO);

            if (child_is_leaf) {
                *currentp = current;
                *descentp = descent;
                return (0);
            }
        }

        /* Swap to the (internal) child and continue descending. */
        read_flags = WT_READ_RESTART_OK;
        if ((ret = __wt_page_swap(session, current, descent, read_flags)) == 0) {
            current = descent;
            continue;
        }
        if (ret == WT_RESTART)
            goto restart;
        WT_ERR(ret);
    }

    /* current is already a row-store leaf - it's in cache so nothing to warm up. */
    *currentp = current;
    *descentp = NULL;
    return (0);

err:
    if (current != NULL && current != &btree->root)
        WT_TRET(__wt_page_release(session, current, 0));
    return (ret);
}

/*
 * __wt_btcur_touch --
 *     Implementation of WT_CURSOR::search for a touch cursor. Descends through internal
 *     pages only, issues a warmup hint via PALI for the would-be leaf page, and returns
 *     WT_NOTFOUND.
 */
int
__wt_btcur_touch(WT_CURSOR_BTREE *cbt)
{
    WT_BTREE *btree;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_REF *current, *descent;
    WT_SESSION_IMPL *session;

    cursor = &cbt->iface;
    session = CUR2S(cbt);
    btree = S2BT(session);

    if (btree->type != BTREE_ROW)
        WT_RET_MSG(session, ENOTSUP, "touch cursor: only row-store tables are supported");

    WT_RET(__wt_cursor_localkey(cursor));
    __cursor_pos_clear(cbt);

    WT_RET(__wt_cursor_func_init(cbt, true));

    current = descent = NULL;
    /*
     * Accessing ref->addr from an unloaded leaf requires holding the split generation.
     * __wt_page_swap handles this internally for the in-cache case; we read the address
     * cookie directly via __wt_ref_addr_copy so we have to enter the generation ourselves.
     */
    WT_ENTER_GENERATION(session, WT_GEN_SPLIT);
    ret = __touch_descend(session, &cursor->key, &current, &descent);
    if (ret == 0 && descent != NULL)
        ret = __touch_warmup_leaf(
          session, descent, cbt->touch_command.size > 0 ? &cbt->touch_command : NULL);
    WT_LEAVE_GENERATION(session, WT_GEN_SPLIT);

    /* Always release the parent before returning. */
    if (current != NULL && current != &btree->root)
        WT_TRET(__wt_page_release(session, current, 0));

    WT_STAT_CONN_DSRC_INCR(session, cursor_search);

    /* Fire-and-forget; the caller sees WT_NOTFOUND so it knows no value is materialized. */
    return (ret == 0 ? WT_NOTFOUND : ret);
}
