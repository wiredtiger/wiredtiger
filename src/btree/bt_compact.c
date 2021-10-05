/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __compact_leaf_inmem_check_addrs --
 *     Return if a clean, in-memory page needs to be re-written.
 */
static int
__compact_leaf_inmem_check_addrs(WT_SESSION_IMPL *session, WT_REF *ref, bool *skipp)
{
    WT_ADDR_COPY addr;
    WT_BM *bm;
    WT_MULTI *multi;
    WT_PAGE_MODIFY *mod;
    uint32_t i;

    *skipp = true; /* Default skip. */

    bm = S2BT(session)->bm;

    /* If the page is clean, test the original addresses. */
    if (__wt_page_evict_clean(ref->page))
        return (__wt_ref_addr_copy(session, ref, &addr) ?
            bm->compact_page_skip(bm, session, addr.addr, addr.size, skipp) :
            0);

    /*
     * If the page is a replacement, test the replacement addresses. Ignore empty pages, they get
     * merged into the parent.
     */
    mod = ref->page->modify;
    if (mod->rec_result == WT_PM_REC_REPLACE)
        return (
          bm->compact_page_skip(bm, session, mod->mod_replace.addr, mod->mod_replace.size, skipp));

    if (mod->rec_result == WT_PM_REC_MULTIBLOCK)
        for (multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i) {
            if (multi->addr.addr == NULL)
                continue;
            WT_RET(bm->compact_page_skip(bm, session, multi->addr.addr, multi->addr.size, skipp));
            if (!*skipp)
                break;
        }

    return (0);
}

/*
 * __compact_leaf_inmem --
 *     Return if an in-memory page needs to be re-written.
 */
static int
__compact_leaf_inmem(WT_SESSION_IMPL *session, WT_REF *ref, bool *skipp)
{
    WT_DECL_RET;
    bool busy;

    *skipp = true; /* Default skip. */

    /*
     * Reviewing in-memory pages requires looking at page reconciliation results, because we care
     * about where the page is stored now, not where the page was stored when we first read it into
     * the cache. We need to ensure we don't race with page reconciliation as it's writing the page
     * modify information. There are two ways we call reconciliation: checkpoints and eviction. We
     * are already blocking checkpoints in this tree, acquire a hazard pointer to block eviction. If
     * the page is in transition or switches state (we've already released our lock), walk away,
     * we'll deal with it next time.
     */
    WT_RET(__wt_hazard_set(session, ref, &busy));
    if (busy)
        return (0);
    if (ref->state != WT_REF_MEM)
        goto done;

    /*
     * Ignore dirty pages, checkpoint will likely write them. There are cases where checkpoint can
     * skip dirty pages: to avoid that, we could alter the transactional information of the page,
     * which is what checkpoint reviews to decide if a page can be skipped. Not doing that for now,
     * the repeated checkpoints that compaction requires are more than likely to pick up all dirty
     * pages at some point.
     *
     * Check clean page addresses, and mark page and tree dirty if the page needs to be rewritten.
     */
    if (!__wt_page_is_modified(ref->page))
        WT_ERR(__compact_leaf_inmem_check_addrs(session, ref, skipp));

    if (!*skipp) {
        WT_ERR(__wt_page_modify_init(session, ref->page));
        __wt_page_modify_set(session, ref->page);

        /* Have reconciliation write new blocks. */
        F_SET_ATOMIC(ref->page, WT_PAGE_COMPACTION_WRITE);

        WT_STAT_DATA_INCR(session, btree_compact_rewrite);
    }

done:
err:
    WT_TRET(__wt_hazard_clear(session, ref));
    return (ret);
}

/*
 * __compact_leaf_replace_addr --
 *     Replace a leaf page's WT_ADDR.
 */
static int
__compact_leaf_replace_addr(WT_SESSION_IMPL *session, WT_REF *ref, WT_ADDR_COPY *copy)
{
    WT_ADDR *addr;
    WT_CELL_UNPACK_ADDR unpack;
    WT_DECL_RET;
    // WT_PAGE *page;

    // page = ref->page;
    /*
     * If there's no address at all (the page has never been written), allocate a new WT_ADDR
     * structure, otherwise, the address has already been instantiated, replace the cookie.
     */
    addr = ref->addr;
    WT_ASSERT(session, addr != NULL);

    if (__wt_off_page(ref->home, addr))
        __wt_free(session, addr->addr);
    else {
        __wt_cell_unpack_addr(session, ref->home->dsk, (WT_CELL *)addr, &unpack);

        WT_RET(__wt_calloc_one(session, &addr));
        addr->ta.newest_start_durable_ts = unpack.ta.newest_start_durable_ts;
        addr->ta.newest_stop_durable_ts = unpack.ta.newest_stop_durable_ts;
        addr->ta.oldest_start_ts = unpack.ta.oldest_start_ts;
        addr->ta.newest_txn = unpack.ta.newest_txn;
        addr->ta.newest_stop_ts = unpack.ta.newest_stop_ts;
        addr->ta.newest_stop_txn = unpack.ta.newest_stop_txn;
        switch (unpack.raw) {
        case WT_CELL_ADDR_INT:
            addr->type = WT_ADDR_INT;
            break;
        case WT_CELL_ADDR_LEAF:
            addr->type = WT_ADDR_LEAF;
            break;
        case WT_CELL_ADDR_LEAF_NO:
            addr->type = WT_ADDR_LEAF_NO;
            break;
        }
    }

    WT_ERR(__wt_strndup(session, copy->addr, copy->size, &addr->addr));
    addr->size = copy->size;

    ref->addr = addr;
    return (0);

err:
    if (addr != ref->addr)
        __wt_free(session, addr);
    return (ret);
}

/*
 * __compact_leaf --
 *     Compaction for a leaf page.
 */
static int
__compact_leaf(WT_SESSION_IMPL *session, WT_REF *ref, bool *skipp)
{
    WT_ADDR_COPY copy;
    WT_BM *bm;
    WT_DECL_RET;
    size_t addr_size;
    uint8_t previous_state;
    // bool locked;

    *skipp = true; /* Default skip. */
    // locked = false;
    addr_size = 0;

    /*
     * Skip deleted pages but consider them progress (the on-disk block is discarded by the next
     * checkpoint).
     */
    if (ref->state == WT_REF_DELETED) {
        *skipp = false;
        return (0);
    }

    /*
     * Lock the WT_REF.
     *
     * If it's on-disk, get a copy of the address and ask the block manager to rewrite the block if
     * it's useful. This is safe because we're holding the WT_REF locked, so nobody can read the
     * page giving eviction a chance to modify the address. We are holding the WT_REF lock across
     * two OS buffer cache I/Os (the read of the original block and the write of the new block),
     * plus whatever overhead that entails. It's not ideal, we could alternatively release the lock,
     * but then we'd have to deal with the block having been read into memory while we were moving
     * it.
     */
    WT_REF_LOCK(session, ref, &previous_state);
    // locked = true;
    if (previous_state == WT_REF_DISK && __wt_ref_addr_copy(session, ref, &copy)) {
        bm = S2BT(session)->bm;
        addr_size = copy.size;
        WT_ERR(bm->compact_page_rewrite(bm, session, copy.addr, &addr_size, skipp));
        if (!*skipp) {
            copy.size = (uint8_t)addr_size;
            WT_ERR(__compact_leaf_replace_addr(session, ref, &copy));

            WT_STAT_DATA_INCR(session, btree_compact_rewrite);
        }
    }
    WT_REF_UNLOCK(ref, previous_state);

    /*
     * Ignore pages that aren't in-memory for some reason other than they're on-disk, for example,
     * they might have split or been deleted while we were locking the WT_REF. This includes the
     * case where we found an on-disk page and either rewrite the block successfully or failed to
     * get a copy of the address (which shouldn't ever happen, but if that goes wrong, it's not our
     * problem to solve).
     */
    if (previous_state != WT_REF_MEM)
        return (0);

    /*
     * Check the in-memory page. Remember, all we know at this point is the page was in-memory at
     * some point in the past, and we're holding its parent so the WT_REF can't go anywhere.
     */
    return (__compact_leaf_inmem(session, ref, skipp));

err:
    if (ret != 0)
        WT_REF_UNLOCK(ref, previous_state);
    return (ret);
}

/*
 * __compact_internal --
 *     Compaction for an internal page.
 */
static int
__compact_internal(WT_SESSION_IMPL *session, WT_REF *parent)
{
    WT_DECL_RET;
    WT_REF *ref;
    bool overall_progress, skipp;

    ref = NULL; /* [-Wconditional-uninitialized] */

    /*
     * We could corrupt a checkpoint if we moved a block that's part of the checkpoint, that is, if
     * we race with checkpoint's review of the tree. Get the tree's flush lock which blocks threads
     * writing pages for checkpoints, and hold it long enough to review a single internal page. Quit
     * working the file if checkpoint is holding the lock, it will be revisited in the next pass.
     */
    WT_RET(__wt_spin_trylock(session, &S2BT(session)->flush_lock));

    /* Walk the internal page and check any leaf pages it references. */
    overall_progress = false;
    WT_INTL_FOREACH_BEGIN (session, parent->page, ref) {
        if (F_ISSET(ref, WT_REF_FLAG_LEAF)) {
            WT_ERR(__compact_leaf(session, ref, &skipp));
            if (!skipp)
                overall_progress = true;
        }
    }
    WT_INTL_FOREACH_END;

    /*
     * If we moved a leaf page, we'll write the parent. If we didn't move a leaf page, check pages
     * other than the root to see if we want to move the internal page itself. (Skip the root as a
     * forced checkpoint will always rewrite it, and you can't just "move" a root page.)
     */
    if (!overall_progress && !__wt_ref_is_root(parent)) {
        WT_ERR(__compact_leaf(session, parent, &skipp));
        if (!skipp)
            overall_progress = true;
    }

    /* If we found a page to compact, mark the parent and tree dirty. */
    if (overall_progress) {
        WT_TRET(__wt_page_parent_modify_set(session, ref, false));
        session->compact_state = WT_COMPACT_SUCCESS;
    }

err:
    /* Unblock checkpoint threads. */
    __wt_spin_unlock(session, &S2BT(session)->flush_lock);

    return (ret);
}

/*
 * __compact_progress --
 *     Output a compact progress message.
 */
static void
__compact_progress(WT_SESSION_IMPL *session, u_int *msg_countp)
{
    struct timespec cur_time;
    WT_BLOCK *block;
    uint64_t time_diff;

    if (!WT_VERBOSE_ISSET(session, WT_VERB_COMPACT_PROGRESS))
        return;

    block = S2BT(session)->bm->block;
    __wt_epoch(session, &cur_time);

    /* Log one progress message every twenty seconds. */
    time_diff = WT_TIMEDIFF_SEC(cur_time, session->compact->begin);
    if (time_diff / WT_PROGRESS_MSG_PERIOD > *msg_countp) {
        ++*msg_countp;
        __wt_verbose(session, WT_VERB_COMPACT_PROGRESS,
          "compacting %s for %" PRIu64 " seconds; reviewed %" PRIu64 " pages, skipped %" PRIu64
          " pages, cache pages evicted %" PRIu64 ", on-disk pages moved %" PRIu64,
          block->name, time_diff, block->compact_pages_reviewed, block->compact_pages_skipped,
          block->compact_cache_evictions, block->compact_blocks_moved);
    }
}

/*
 * __compact_walk_page_skip --
 *     Skip leaf pages, all we want are internal pages.
 */
static int
__compact_walk_page_skip(WT_SESSION_IMPL *session, WT_REF *ref, void *context, bool *skipp)
{
    WT_UNUSED(context);
    WT_UNUSED(session);

    /* All we want are the internal pages. */
    *skipp = F_ISSET(ref, WT_REF_FLAG_LEAF) ? true : false;
    return (0);
}

/*
 * __wt_compact --
 *     Compact a file.
 */
int
__wt_compact(WT_SESSION_IMPL *session)
{
    WT_BM *bm;
    WT_DECL_RET;
    WT_REF *ref;
    u_int i, msg_count;
    bool skip;

    bm = S2BT(session)->bm;
    ref = NULL;

    WT_STAT_DATA_INCR(session, session_compact);

    /*
     * Check if compaction might be useful (the API layer will quit trying to compact the data
     * source if we make no progress).
     */
    WT_RET(bm->compact_skip(bm, session, &skip));
    if (skip)
        return (0);

    /* Walk the tree reviewing pages to see if they should be re-written. */
    for (i = msg_count = 0;;) {
        /*
         * Periodically check if we've timed out or eviction is stuck. Quit if eviction is stuck,
         * we're making the problem worse.
         */
        if (++i > 100) {
            __compact_progress(session, &msg_count);
            WT_ERR(__wt_session_compact_check_timeout(session));

            if (__wt_cache_stuck(session))
                WT_ERR(EBUSY);

            i = 0;
        }

        /*
         * Compact pulls pages into cache during the walk without checking whether the cache is
         * full. Check now to throttle compact to match eviction speed.
         */
        WT_ERR(__wt_cache_eviction_check(session, false, false, NULL));

        /*
         * Pages read for compaction aren't "useful"; don't update the read generation of pages
         * already in memory, and if a page is read, set its generation to a low value so it is
         * evicted quickly.
         */
        WT_ERR(__wt_tree_walk_custom_skip(
          session, &ref, __compact_walk_page_skip, NULL, WT_READ_NO_GEN | WT_READ_WONT_NEED));
        if (ref == NULL)
            break;

        WT_WITH_PAGE_INDEX(session, ret = __compact_internal(session, ref));
        WT_ERR(ret);
    }

err:
    WT_TRET(__wt_page_release(session, ref, 0));

    return (ret);
}
