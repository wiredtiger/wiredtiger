/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Touch cursor implementation (skunk_94).
 *
 * A touch cursor's WT_CURSOR::search descends the btree using only internal
 * pages and forwards a fire-and-forget warmup hint to the page log layer
 * (PALI) for the would-be leaf page. The caller always sees WT_NOTFOUND;
 * no leaf is materialized into the WiredTiger cache.
 *
 * The intent is to let storage layers below WT (e.g. SLS via PALI) promote
 * pages to a faster tier ahead of the real read. Worst-case latency is
 * identical to the non-touch path: a warmup hint that the page log ignores
 * costs at most one extra round-trip, and the caller's subsequent real
 * search() still pays full cold-tier cost.
 *
 * Public surface:
 *   - WT_SESSION.open_cursor sub-config touch=(enabled,class_id,action,command)
 *     (see dist/api_data.py / src/cursor/cur_file.c::__curfile_create)
 *   - WT_PAGE_LOG_GET_ARGS.flags |= WT_PAGE_LOG_WARMUP
 *   - WT_PAGE_LOG_GET_ARGS.command (opaque payload)
 *
 * Restrictions enforced at cursor open time (cur_file.c):
 *   - row-store only
 *   - not compatible with bulk, next_random or checkpoint cursors
 */

#include "wt_internal.h"

/*
 * __curtouch_warmup_leaf --
 *     Issue a fire-and-forget warmup hint for the leaf page referenced by descent. Only meaningful
 *     when the tree is on disaggregated storage; on a non-disagg tree the call is a no-op success
 *     because there is no PALI handle to forward to. The contract with PALI is that the
 *     implementation must return zero results for a warmup; we defensively free anything that leaks
 *     back to avoid a memory leak from a buggy implementation.
 */
static int
__curtouch_warmup_leaf(WT_SESSION_IMPL *session, WT_REF *descent, const WT_ITEM *command)
{
    WT_ADDR_COPY addr;
    WT_BLOCK_DISAGG *block_disagg;
    WT_BLOCK_DISAGG_ADDRESS_COOKIE cookie;
    WT_BM *bm;
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_ITEM results[WT_DELTA_LIMIT + 1];
    WT_PAGE_LOG_GET_ARGS get_args;
    WT_PAGE_LOG_HANDLE *plhandle;
    uint32_t i, results_count;
    const uint8_t *p;

    btree = S2BT(session);
    bm = btree->bm;

    /*
     * No-op for storage classes that don't speak PALI. We still return success so the touch cursor
     * is usable on any tree -- the caller observes the same WT_NOTFOUND either way.
     */
    if (!F_ISSET(btree, WT_BTREE_DISAGGREGATED) || bm == NULL) {
        WT_STAT_CONN_DSRC_INCR(session, cursor_touch_skipped_non_disagg);
        return (0);
    }

    /*
     * Pull the address cookie for the leaf without locking the WT_REF in place. __wt_ref_addr_copy
     * returns false if the ref has no address (e.g. a freshly created empty page); nothing to warm
     * in that case.
     */
    if (!__wt_ref_addr_copy(session, descent, &addr) || addr.size == 0) {
        WT_STAT_CONN_DSRC_INCR(session, cursor_touch_skipped_no_addr);
        return (0);
    }

    /* Crack the cookie for the page id. */
    p = addr.addr;
    WT_RET(__wt_block_disagg_addr_unpack(session, &p, addr.size, &cookie));

    /* The disaggregated block manager handle stores the PALI handle directly. */
    block_disagg = (WT_BLOCK_DISAGG *)bm->block;
    if (block_disagg == NULL || (plhandle = block_disagg->plhandle) == NULL ||
      plhandle->plh_get == NULL) {
        WT_STAT_CONN_DSRC_INCR(session, cursor_touch_skipped_non_disagg);
        return (0);
    }

    memset(results, 0, sizeof(results));
    memset(&get_args, 0, sizeof(get_args));
    get_args.flags = WT_PAGE_LOG_WARMUP;
    get_args.command = command;
    results_count = WT_DELTA_LIMIT + 1;

    WT_STAT_CONN_DSRC_INCR(session, cursor_touch_warmup);
    ret = plhandle->plh_get(
      plhandle, &session->iface, cookie.page_id, 0, &get_args, results, &results_count);
    if (ret != 0)
        WT_STAT_CONN_DSRC_INCR(session, cursor_touch_warmup_error);

    /*
     * The contract is that warmup returns zero results, but a faulty implementation could still
     * allocate. Free anything that came back.
     */
    for (i = 0; i < results_count; ++i)
        if (results[i].mem != NULL)
            __wt_free(session, results[i].mem);

    return (ret);
}

/*
 * __curtouch_descend --
 *     Walk down a row-store btree using only internal pages. On success *currentp points at the
 *     page whose hazard pointer the caller must release, and *descentp at the WT_REF for the
 *     would-be leaf, or NULL if the descent never reached a leaf parent (single-page tree, or the
 *     leaf was already in cache). The split generation must be held by the caller; we read
 *     ref->addr without loading the target page.
 */
static int
__curtouch_descend(
  WT_SESSION_IMPL *session, WT_ITEM *srch_key, WT_REF **currentp, WT_REF **descentp)
{
    WT_ADDR_COPY addr;
    WT_BTREE *btree;
    WT_COLLATOR *collator;
    WT_DECL_RET;
    WT_ITEM item;
    WT_PAGE *page;
    WT_PAGE_INDEX *pindex;
    WT_REF *current, *descent;
    uint32_t base, indx, limit;
    int cmp;
    bool leaf;

    *currentp = NULL;
    *descentp = NULL;

    btree = S2BT(session);
    collator = btree->collator;
    descent = NULL;
    pindex = NULL;

    if (0) {
restart:
        WT_RET(__wt_page_release(session, current, 0));
    }

    current = &btree->root;
    for (;;) {
        page = current->page;
        if (page->type != WT_PAGE_ROW_INT)
            break;

        WT_INTL_INDEX_GET(session, page, pindex);

        /*
         * Binary search the internal page. The 0th key on an internal page is the smallest
         * sentinel, so we always start at base=1, exactly mirroring __wt_row_search's internal-page
         * loop.
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
         * Detect whether the descent target is a leaf without loading it. If the page happens to
         * already be cached, peek at the type directly; otherwise crack the address cookie.
         * WT_GEN_SPLIT held by the caller keeps the ref alive during the peek.
         */
        leaf = false;
        if (WT_REF_GET_STATE(descent) == WT_REF_MEM && descent->page != NULL)
            leaf = (descent->page->type == WT_PAGE_ROW_LEAF);
        else if (__wt_ref_addr_copy(session, descent, &addr))
            leaf = (addr.type == WT_ADDR_LEAF || addr.type == WT_ADDR_LEAF_NO);

        if (leaf) {
            *currentp = current;
            *descentp = descent;
            return (0);
        }

        /* Swap to the (internal) child and continue descending. */
        if ((ret = __wt_page_swap(session, current, descent, WT_READ_RESTART_OK)) == 0) {
            current = descent;
            continue;
        }
        if (ret == WT_RESTART)
            goto restart;
        WT_ERR(ret);
    }

    /*
     * Loop exit: current is already a leaf (single-page tree or shrunk tree). The leaf is in cache
     * by construction, so there is no warmup to issue. Hand the held ref back to the caller for
     * release.
     */
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
 *     WT_CURSOR::search implementation for a touch cursor. Descends through internal pages only,
 *     issues a warmup hint via PALI for the would-be leaf page, and returns WT_NOTFOUND.
 */
int
__wt_btcur_touch(WT_CURSOR_BTREE *cbt)
{
    WT_BTREE *btree;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_ITEM *command;
    WT_REF *current, *descent;
    WT_SESSION_IMPL *session;

    cursor = &cbt->iface;
    session = CUR2S(cbt);
    btree = S2BT(session);

    /*
     * Touch cursor opens reject anything other than row-store, so this should never trip. Defensive
     * check keeps __wt_btcur_touch usable as a stand-alone helper.
     */
    if (btree->type != BTREE_ROW)
        WT_RET_MSG(session, ENOTSUP, "touch cursor: only row-store tables are supported");

    WT_STAT_CONN_DSRC_INCR(session, cursor_touch_search);

    WT_RET(__wt_cursor_localkey(cursor));
    __cursor_pos_clear(cbt);
    WT_RET(__wt_cursor_func_init(cbt, true));

    current = descent = NULL;
    command = cbt->touch_command.size > 0 ? &cbt->touch_command : NULL;

    /*
     * The custom descent reads ref->addr from an unloaded leaf via
     * __wt_ref_addr_copy. That is only safe inside WT_GEN_SPLIT;
     * __wt_page_swap holds it internally for the in-cache path but we
     * bypass page_swap for the final descent, so we take the generation
     * ourselves here.
     */
    WT_ENTER_GENERATION(session, WT_GEN_SPLIT);
    ret = __curtouch_descend(session, &cursor->key, &current, &descent);
    if (ret == 0) {
        if (descent != NULL)
            ret = __curtouch_warmup_leaf(session, descent, command);
        else
            WT_STAT_CONN_DSRC_INCR(session, cursor_touch_leaf_cached);
    }
    WT_LEAVE_GENERATION(session, WT_GEN_SPLIT);

    if (current != NULL && current != &btree->root)
        WT_TRET(__wt_page_release(session, current, 0));

    /* Fire-and-forget: WT_NOTFOUND signals "no value was materialized". */
    return (ret == 0 ? WT_NOTFOUND : ret);
}
