/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

static int __evict_child_check(WT_SESSION_IMPL *, WT_REF *);
static int __evict_page_clean_update(WT_SESSION_IMPL *, WT_REF *, uint32_t);
static int __evict_page_dirty_update(WT_SESSION_IMPL *, WT_REF *, uint32_t);
static int __evict_reconcile(WT_SESSION_IMPL *, WT_REF *, uint32_t);
static int __evict_review(WT_SESSION_IMPL *, WT_REF *, uint32_t, bool *);

/*
 * __evict_exclusive_clear --
 *     Release exclusive access to a page.
 */
static WT_INLINE void
__evict_exclusive_clear(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE previous_state)
{
    WT_ASSERT(session, WT_REF_GET_STATE(ref) == WT_REF_LOCKED && ref->page != NULL);

    WT_REF_SET_STATE(ref, previous_state);
}

/*
 * __evict_exclusive --
 *     Acquire exclusive access to a page. Used only on the closing path where the tree is already
 *     exclusively locked and the ref arrives pre-locked.
 */
static WT_INLINE int
__evict_exclusive(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_ASSERT(session, WT_REF_GET_STATE(ref) == WT_REF_LOCKED);

    /*
     * Check for a hazard pointer indicating another thread is using the page, meaning the page
     * cannot be evicted.
     */
    if (__wt_hazard_check(session, ref, NULL) == NULL)
        return (0);

    WT_STAT_CONN_DSRC_INCR(session, cache_eviction_blocked_hazard);
    return (__wt_set_return(session, EBUSY));
}

/*
 * __evict_acquire_exclusive --
 *     Phase-2 of two-phase eviction: CAS the ref from WT_REF_MEM to WT_REF_LOCKED and clear the
 *     calling session's own hazard pointer. Called after reconciliation completes under
 *     hazard-pointer-only protection.
 *
 * On success, the ref is LOCKED, the caller's hazard pointer is cleared, and *acquiredp is set to
 *     true. The caller must then check other sessions' hazard pointers and perform the dirty-gap
 *     check before proceeding with swap-out.
 *
 * On CAS failure (EBUSY), the ref state is unchanged and the caller's hazard pointer is still set;
 *     the caller must clear it on the error path.
 */
static int
__evict_acquire_exclusive(
  WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE previous_state, bool *acquiredp)
{
    *acquiredp = false;

    /*
     * Atomically claim exclusive ownership of the ref. If this fails, another thread changed the
     * state (e.g., split, another eviction thread, page re-read). Our hazard pointer is still set;
     * the caller will clear it via the error path.
     */
    if (!WT_REF_CAS_STATE(session, ref, previous_state, WT_REF_LOCKED))
        return (__wt_set_return(session, EBUSY));

    /*
     * The ref is now LOCKED. No new hazard pointers can be set by other threads (hazard_set_func
     * re-checks state after its memory barrier and returns busy if not WT_REF_MEM). Clear our own
     * hazard pointer — we are now protected by the exclusive state and no longer need it.
     */
    WT_RET(__wt_hazard_clear(session, ref));
    *acquiredp = true;

    return (0);
}

#define WT_EVICT_STATS_CLEAN 0x01
#define WT_EVICT_STATS_FORCE_HS 0x02
#define WT_EVICT_STATS_SUCCESS 0x04
#define WT_EVICT_STATS_URGENT 0x08

/*
 * Victim Cache Overview
 * ---------------------
 * The Victim Cache is an LRU cache designed to avoid data duplication with WiredTiger's
 * in-memory page cache. Unlike a traditional transparent read-write cache, data enters the
 * Victim Cache only when pages are evicted from WiredTiger's cache. Conversely, when a page
 * is read into or written from WiredTiger's cache, it is removed from the Victim Cache, since
 * it is already present in the main cache.
 *
 * WiredTiger only evicts clean pages from memory. If a page has unwritten data (dirty),
 * it must first be reconciled to produce a clean version before it can be evicted.
 *
 * Only leaf pages are cached; internal pages are not stored in the Victim Cache.
 *
 * Pages are not cached during shutdown, since they will not be needed again.
 *
 * Pages are compressed before being stored in the Victim Cache to reduce memory usage,
 * though this incurs CPU cost.
 *
 * Implementation Note:
 * A page's in-memory representation may differ from its on-disk format. To handle this, we
 * store additional metadata alongside each cached page:
 *   - The original delta length.
 *   - A flag indicating whether to skip address cookie checksum validation when the page is
 *     retrieved from the cache (since the checksum may no longer match the in-memory state).
 */

/*
 * __evict_page_victim_cache --
 *     Check eligibility and put page in victim cache if applicable.
 */
static void
__evict_page_victim_cache(WT_SESSION_IMPL *session, WT_REF *ref)
{
    if (!F_ISSET(S2BT(session), WT_BTREE_DISAGGREGATED))
        return;

    WT_BM *bm = S2BT(session)->bm;
    if (bm == NULL)
        return;

    WT_BLOCK_DISAGG *block_disagg = (WT_BLOCK_DISAGG *)bm->block;
    if (block_disagg == NULL)
        return;

    WT_PAGE_LOG_HANDLE *plh = block_disagg->plhandle;
    if (plh == NULL)
        return;

    if (plh->plh_cache_put == NULL || plh->plh_cache_available == NULL ||
      !plh->plh_cache_available(plh, &session->iface))
        return;

    WT_PAGE *page = ref->page;

    /* Only cache clean pages without modify. */
    if (__wt_page_is_modified(page))
        return;

    /* Must be a leaf page with disagg info and disk image. */
    if (!F_ISSET(ref, WT_REF_FLAG_LEAF) || page->disagg_info == NULL || page->dsk == NULL)
        return;

    if (page->disagg_info->block_meta.page_id == WT_BLOCK_INVALID_PAGE_ID)
        return;

    /* Cannot cache root pages. */
    if (__wt_ref_is_root(ref))
        return;

    /*
     * Victim cache: store evicted pages in disagg cache. The format must match what disagg read
     * path expects: WT_PAGE_HEADER + WT_BLOCK_DISAGG_HEADER + data
     */
    WT_ITEM buf_orig = {
      .data = page->dsk,
      .size = page->dsk->mem_size,
      .mem = (void *)page->dsk,
      .memsize = page->dsk->mem_size,
      .flags = 0,
    };
    WT_ITEM *cache_buf = &buf_orig;
    WT_ITEM *compressed_buf = NULL;
    WT_PAGE_HEADER *dsk;
    bool compressed = false;
    bool data_checksum = true;

    /* Optionally compress the data before caching. */
    WT_IGNORE_RET(
      __wt_blkcache_compress(session, &buf_orig, false, &compressed_buf, NULL, &compressed));
    if (compressed_buf != NULL)
        cache_buf = compressed_buf;

    /* Point dsk to the cache buffer's page header. */
    dsk = (WT_PAGE_HEADER *)cache_buf->mem;

    /*
     * Determine if full data checksum is needed based on btree config. This follows the same logic
     * as __wt_blkcache_write.
     */
    switch (S2BT(session)->checksum) {
    case CKSUM_ON:
        break;
    case CKSUM_OFF:
        data_checksum = false;
        break;
    case CKSUM_UNCOMPRESSED:
        data_checksum = !compressed;
        break;
    case CKSUM_UNENCRYPTED:
        /* Not encrypted in this path. */
        break;
    }

    /*
     * Fill in the disagg block header following the pattern from
     * __wti_block_disagg_write_internal. The disagg block header
     * is at WT_BLOCK_HEADER_REF (after the page header).
     */
    WT_BLOCK_DISAGG_HEADER *blk = WT_BLOCK_HEADER_REF(cache_buf->data);
    memset(blk, 0, sizeof(*blk));

    /* Set disagg header fields. */
    blk->magic = WT_BLOCK_DISAGG_MAGIC_BASE;
    blk->version = WT_BLOCK_DISAGG_VERSION;
    blk->compatible_version = WT_BLOCK_DISAGG_COMPATIBLE_VERSION;
    blk->header_size = WT_BLOCK_DISAGG_HEADER_BYTE_SIZE;
    blk->previous_checksum = page->disagg_info->block_meta.checksum;
    blk->flags = 0;
    if (data_checksum)
        F_SET(blk, WT_BLOCK_DISAGG_DATA_CKSUM);
    if (compressed)
        F_SET(blk, WT_BLOCK_DISAGG_COMPRESSED);
    /* Mark as cached so read path skips address cookie checksum match. */
    F_SET(blk, WT_BLOCK_DISAGG_MODIFIED);
    /* Not encrypted in this path. */

    /* Calculate checksum following __wti_block_disagg_write_internal. */
    blk->checksum = 0;
    blk->checksum = __wt_checksum(cache_buf->data,
      data_checksum ? cache_buf->size : WT_MIN(cache_buf->size, WT_BLOCK_COMPRESS_SKIP));

    /*
     * Swap page header to little-endian for on-disk format.
     */
    __wt_page_header_byteswap(dsk);

    WT_PAGE_LOG_PUT_ARGS args = {
      .backlink_lsn = page->disagg_info->block_meta.backlink_lsn,
      .base_lsn = page->disagg_info->block_meta.base_lsn,
      .backlink_checkpoint_id = 0,
      .base_checkpoint_id = 0,
      .delta_count = page->disagg_info->block_meta.delta_count,
      .image_size = page->dsk->mem_size,
      .flags = compressed ? WT_PAGE_LOG_COMPRESSED : 0,
      .lsn = page->disagg_info->block_meta.disagg_lsn,
    };

    WT_IGNORE_RET(plh->plh_cache_put(
      plh, &session->iface, page->disagg_info->block_meta.page_id, 0, &args, cache_buf));

    if (compressed_buf != NULL)
        __wt_scr_free(session, &compressed_buf);
    else
        /* Swap page header back to native order. */
        __wt_page_header_byteswap(dsk);
}

/*
 * __evict_stats_update --
 *     Update the stats of eviction.
 *
 */
static void
__evict_stats_update(WT_SESSION_IMPL *session, uint8_t flags)
{
    WT_CONNECTION_IMPL *conn;
    uint64_t eviction_time, eviction_time_milliseconds;
    bool ingest = F_ISSET(S2BT(session), WT_BTREE_GARBAGE_COLLECT);
    conn = S2C(session);

    if (session->evict_timeline.reentry_hs_eviction) {
        session->evict_timeline.reentry_hs_evict_finish = __wt_clock(session);
        eviction_time = WT_CLOCKDIFF_US(session->evict_timeline.reentry_hs_evict_finish,
          session->evict_timeline.reentry_hs_evict_start);
    } else {
        session->evict_timeline.evict_finish = __wt_clock(session);
        eviction_time = WT_CLOCKDIFF_US(
          session->evict_timeline.evict_finish, session->evict_timeline.evict_start);
    }
    if (LF_ISSET(WT_EVICT_STATS_SUCCESS)) {
        if (LF_ISSET(WT_EVICT_STATS_URGENT)) {
            if (LF_ISSET(WT_EVICT_STATS_FORCE_HS))
                WT_STAT_CONN_INCR(session, eviction_force_hs_success);
            if (LF_ISSET(WT_EVICT_STATS_CLEAN))
                WT_STAT_CONN_INCR(session, eviction_force_clean);
            else
                WT_STAT_CONN_INCR(session, eviction_force_dirty);
            if (ingest)
                WT_STAT_CONN_INCR(session, eviction_force_ingest_success);
        }

        if (LF_ISSET(WT_EVICT_STATS_CLEAN))
            WT_STAT_CONN_DSRC_INCR(session, cache_eviction_clean);
        else
            WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty);

        if (ingest)
            WT_STAT_CONN_INCR(session, eviction_ingest_success);

        /* Count page evictions in parallel with checkpoint. */
        if (__wt_atomic_load_bool_v_relaxed(&conn->txn_global.checkpoint_running))
            WT_STAT_CONN_INCR(session, eviction_pages_in_parallel_with_checkpoint);
    } else {
        if (LF_ISSET(WT_EVICT_STATS_URGENT)) {
            if (LF_ISSET(WT_EVICT_STATS_FORCE_HS))
                WT_STAT_CONN_INCR(session, eviction_force_hs_fail);
            WT_STAT_CONN_INCR(session, eviction_force_fail);
            if (ingest)
                WT_STAT_CONN_INCR(session, eviction_force_ingest_fail);
        }

        WT_STAT_CONN_DSRC_INCR(session, eviction_fail);
        if (ingest)
            WT_STAT_CONN_INCR(session, eviction_fail_ingest);
    }
    if (!session->evict_timeline.reentry_hs_eviction) {
        eviction_time_milliseconds = eviction_time / WT_THOUSAND;
        __wt_atomic_stats_max_uint64(
          &conn->evict->evict_max_ms_per_checkpoint, eviction_time_milliseconds);
        __wt_atomic_stats_max_uint64(&conn->evict->evict_max_ms, eviction_time_milliseconds);
        if (eviction_time_milliseconds > WT_MINUTE * WT_THOUSAND)
            __wt_verbose_warning(session, WT_VERB_EVICTION,
              "Eviction took more than 1 minute (%" PRIu64 "us). Building disk image took %" PRIu64
              "us. History store wrapup took %" PRIu64 "us.",
              eviction_time,
              WT_CLOCKDIFF_US(session->reconcile_timeline.image_build_finish,
                session->reconcile_timeline.image_build_start),
              WT_CLOCKDIFF_US(session->reconcile_timeline.hs_wrapup_finish,
                session->reconcile_timeline.hs_wrapup_start));
    } else {
        /*
         * We are in the reentrant history store eviction inside a data store reconciliation. Add to
         * the total time taken to do the reentrant history store eviction.
         */
        session->reconcile_timeline.total_reentry_hs_eviction_time +=
          WT_CLOCKDIFF_MS(session->evict_timeline.reentry_hs_evict_finish,
            session->evict_timeline.reentry_hs_evict_start);
        session->evict_timeline.reentry_hs_eviction = false;
    }
}

/*
 * Maximum number of two-phase dirty-gap failures before demoting a page to single-phase. Each
 * dirty-gap failure means a full phase-1 reconcile was discarded (concurrent writer beat us to
 * phase-2). After this many wasted reconciles, fall back to single-phase so the next attempt
 * reconciles exactly once under the exclusive lock.
 *
 * Hazard-pointer spin timeouts do NOT count toward this limit. Two-phase is strictly better for
 * read-hot pages (readers proceed freely during phase-1; single-phase would block them for the
 * entire reconcile). Only write-hot contention (dirty-gap) warrants the single-phase fallback.
 */
#define WT_EVICT_TWO_PHASE_RETRY_LIMIT 2

/*
 * Number of eviction passes to skip a page after it fails phase-2 due to hazard pointer conflicts.
 * Set page->evict_pass_gen to (current_pass_gen + this value) on HP timeout; the walk skips the
 * page until the global pass gen catches up, preventing repeated wasted selection cycles on pages
 * with active readers.
 */
#define WT_EVICT_HP_COOLDOWN_PASSES 2

/*
 * Maximum number of times the pre-phase-1 hazard gate defers eviction (cooldown) before falling
 * back to single-phase. For the first WT_EVICT_HP_GATE_RETRY_LIMIT attempts where the gate finds
 * an HP, we apply a cooldown and return EBUSY rather than falling to single-phase. Single-phase
 * reconciles under WT_REF_LOCKED, blocking active readers for the entire reconcile duration; a
 * short cooldown is cheaper when the HP is from a transient reader. After this many deferrals the
 * HP is considered persistent and single-phase is used to guarantee eventual eviction.
 */
#define WT_EVICT_HP_GATE_RETRY_LIMIT 3

/* !!!
 * __wt_evict --
 *     Evict a page from memory by taking exclusive access to the page.
 *
 *     Based on the page's state, the function either reconciles and writes the page to disk or
 *     simply discards it from the cache. It is called by both eviction worker threads and
 *     application threads.
 *
 *     Input parameters:
 *       (1) `ref`: Reference to the page getting evicted.
 *       (2) `previous_state`: Previous state of the page's reference, restored if the page cannot
 *           be evicted.
 *       (3) `flags`: Eviction-related flags indicating conditions such as `urgent eviction`,
 *           `no splits`, or `tree closing`.
 *
 *     Return an error code for cases blocking exclusive access to the page, failure in
 *     reconciliation, or certain conditions preventing the page's eviction.
 */
int
__wt_evict(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE previous_state, uint32_t flags)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_PAGE *page;
    uint64_t page_size;
    uint8_t stats_flags;
    bool acquired, app_thread_assist, closing, ebusy_only, evict_clean, inmem_split;
    bool is_dirty, tree_dead, two_phase;

    conn = S2C(session);
    page = ref->page;
    closing = LF_ISSET(WT_EVICT_CALL_CLOSING);
    stats_flags = 0;
    acquired = ebusy_only = evict_clean = is_dirty = false;
    app_thread_assist = !F_ISSET(session, WT_SESSION_INTERNAL) && !LF_ISSET(WT_EVICT_CALL_URGENT);

    /*
     * When RTS is active, skip phase-1 reconciliation entirely and defer all reconciliation to
     * phase-2 (under WT_REF_LOCKED). RTS directly modifies update chains (setting txnid to
     * WT_TXN_ABORTED) without acquiring WT_PAGE_LOCK. Phase-1 reconciliation under WT_REF_MEM would
     * race with these modifications, causing assertion failures in rec_hs.c and rec_visibility.c.
     * Under WT_REF_LOCKED, RTS sees the page as locked and skips it.
     *
     * Concurrent non-RTS transaction rollbacks (also setting txnid to WT_TXN_ABORTED) are handled
     * in __wti_rec_upd_select, which detects the post-selection abort race and returns EBUSY. The
     * phase-1 EBUSY handler below converts that to two_phase=false so phase-2 retries under the
     * exclusive lock.
     */
    two_phase = !closing && !__wt_atomic_load_bool_v_acquire(&conn->rts->active) &&
      F_ISSET_ATOMIC_32(&conn->cache->cache_eviction_controls, WT_CACHE_EVICT_TWO_PHASE) &&
      page->evict_dirty_gap_count < WT_EVICT_TWO_PHASE_RETRY_LIMIT;

    __wt_verbose_debug3(
      session, WT_VERB_EVICTION, "page %p (%s)", (void *)page, __wt_page_type_string(page->type));

    tree_dead = F_ISSET(session->dhandle, WT_DHANDLE_DEAD);
    if (tree_dead)
        LF_SET(WT_EVICT_CALL_NO_SPLIT);

    /* As re-entry into eviction is possible, only clear the statistics on the first entry. */
    if (__wt_session_gen((session), (WT_GEN_EVICT)) == 0) {
        WT_CLEAR(session->evict_timeline);
        session->evict_timeline.evict_start = __wt_clock(session);
    } else {
        session->evict_timeline.reentry_hs_eviction = true;
        session->evict_timeline.reentry_hs_evict_start = __wt_clock(session);
    }

    /*
     * Enter the eviction and split generation. If we re-enter eviction, leave the previous
     * generation (eviction or split) generation (which must be as low as the current generation),
     * untouched.
     */
    WT_ENTER_GENERATION(session, WT_GEN_EVICT);
    WT_ENTER_GENERATION(session, WT_GEN_SPLIT);

    /*
     * Immediately increment the forcible eviction counter, we might do an in-memory split and not
     * an eviction, which skips the other statistics.
     */
    if (LF_ISSET(WT_EVICT_CALL_URGENT)) {
        FLD_SET(stats_flags, WT_EVICT_STATS_URGENT);
        WT_STAT_CONN_INCR(session, eviction_force);

        /*
         * Track history store pages being force evicted while holding a history store cursor open.
         */
        if (session->hs_cursor_counter > 0 && WT_IS_HS(session->dhandle)) {
            FLD_SET(stats_flags, WT_EVICT_STATS_FORCE_HS);
            WT_STAT_CONN_INCR(session, eviction_force_hs);
        }
    }

    /*
     * Two-phase eviction: reconcile under hazard-pointer-only protection (phase 1), then acquire
     * the exclusive lock only for the fast swap-out step (phase 2). This allows readers to continue
     * accessing the page during potentially long reconciliation I/O.
     *
     * All non-closing callers must arrive with a hazard pointer already set on the ref. The closing
     * path bypasses phase 1; the tree is already exclusively locked, so the ref arrives pre-locked
     * and we use the old exclusive-throughout path.
     *
     */
    if (!closing) {
        /*
         * Confirm the ref state hasn't changed since the caller sampled it. A concurrent split or
         * another eviction thread could have changed it.
         */
        if (WT_REF_GET_STATE(ref) != previous_state) {
            ret = __wt_set_return(session, EBUSY);
            goto err;
        }

        /*
         * We do not call __wti_evict_list_clear_page here because it asserts WT_REF_LOCKED and the
         * ref is still WT_REF_MEM in phase 1. The LRU queue removal happens after acquiring the
         * exclusive lock in phase 2.
         */
    } else {
        /*
         * Closing path: the tree is exclusively locked so no concurrent eviction can race. Lock the
         * ref directly then check for hazard pointers.
         */
        if (!WT_REF_CAS_STATE(session, ref, previous_state, WT_REF_LOCKED))
            WT_ERR(__wt_set_return(session, EBUSY));
        WT_ERR(__evict_exclusive(session, ref));
        __wti_evict_list_clear_page(session, ref);
    }

    if (F_ISSET_ATOMIC_16(page, WT_PAGE_PREFETCH))
        WT_STAT_CONN_INCR(session, eviction_consider_prefetch);

    /*
     * Review the page for conditions that would block its eviction. If the check fails (for
     * example, we find a page with active children), quit. Make this check for clean pages, too:
     * while unlikely eviction would choose an internal page with children, it's not disallowed.
     */
    WT_ERR(__evict_review(session, ref, flags, &inmem_split));

    /*
     * If we decide to do an in-memory split. Do it now. If an in-memory split completes, the page
     * stays in memory and the tree is left in the desired state: avoid the usual cleanup.
     */
    if (inmem_split) {
        /*
         * In-memory splits require WT_REF_LOCKED. On the non-closing path the ref is still
         * WT_REF_MEM (phase 1), so acquire exclusive access before handing off to
         * __wt_split_insert. Also clear the page from the LRU queue now that the ref is locked,
         * matching what the normal eviction path does after phase-2 CAS.
         */
        if (!closing) {
            WT_ERR(__evict_acquire_exclusive(session, ref, previous_state, &acquired));

            /*
             * Check for hazard pointers from other sessions before calling __wt_split_insert.
             * __wt_split_insert modifies the insert skiplist (sets prev_ins->next[0] = NULL)
             * without holding the leaf page lock. A concurrent inserter that set its hazard pointer
             * before our MEM->LOCKED CAS may still be executing __insert_simple_func or
             * __insert_serial_func with a stale ins_stack pointer; our skiplist modification would
             * cause its level-0 CAS to fail with WT_RESTART repeatedly, causing a hang. Return
             * EBUSY so the split is retried once all other hazard holders have released the page.
             */
            if (__wt_hazard_check(session, ref, NULL) != NULL) {
                WT_STAT_CONN_DSRC_INCR(session, cache_eviction_blocked_hazard);
                ret = __wt_set_return(session, EBUSY);
                goto err;
            }

            __wti_evict_list_clear_page(session, ref);
        }
        WT_ERR(__wt_split_insert(session, ref));
        goto done;
    }

    if (__wt_page_is_modified(page))
        is_dirty = true;

    /*
     * Pre-phase-1 hazard gate: if another session already holds a hazard pointer on this page,
     * phase-2 may fail after the expensive phase-1 reconcile completes. Avoid wasted work where
     * possible.
     *
     * Run this check on every attempt, including the first. Under high-concurrency write workloads
     * (bulk insert, find-one-and-update) app threads hold hazard pointers during B-tree traversal
     * and update for tens to hundreds of microseconds -- far longer than the phase-2 spin-wait cap
     * (WT_THOUSAND * WT_PAUSE ≈ 1-4 µs). Skipping the gate on the first attempt causes phase-1
     * reconciliation work to be discarded at a high rate: sys-perf data showed 79-116 pages/s
     * written and immediately restored in-memory, 89-98% forced-eviction failure rates, and
     * write-ticket exhaustion in 78% of samples for insert workloads. __wt_hazard_check scans all
     * sessions' hazard arrays but is cheap in practice (a few entries per call); the cost of one
     * pre-check is negligible compared to a wasted page reconcile.
     *
     * Background eviction workers: on the first WT_EVICT_HP_GATE_RETRY_LIMIT-1 gate firings,
     * prefer a cooldown over single-phase. Single-phase reconciles under WT_REF_LOCKED, blocking
     * all readers for the full reconcile duration. A short cooldown is cheaper: the HP is often
     * from a transient reader that will release within WT_EVICT_HP_COOLDOWN_PASSES passes, after
     * which the walk re-queues the page for two-phase eviction without reader interference. After
     * WT_EVICT_HP_GATE_RETRY_LIMIT deferrals the HP is treated as persistent and single-phase is
     * used to guarantee eventual eviction.
     *
     * App-thread assist eviction: do NOT fall back to single-phase here. The HP gate fires at a
     * point-in-time snapshot; in read-heavy workloads the HP is often from a transient reader that
     * will release well before phase-1 reconcile completes. Falling to single-phase blocks all
     * concurrent readers for the full reconcile duration, directly inflating read latency. Instead,
     * proceed with two-phase; phase-2 will bail immediately (app_thread_assist) if the HP persists
     * and increment evict_hp_gate_count. Once that counter reaches WT_EVICT_HP_GATE_RETRY_LIMIT
     * the HP is treated as persistent and single-phase is used to guarantee eventual eviction.
     *
     * Urgent eviction bypasses the cooldown to avoid cache-pressure livelock.
     */
    if (two_phase && __wt_hazard_check(session, ref, NULL) != NULL) {
        if (app_thread_assist) {
            /*
             * App-thread assist: proceed with two-phase unless the gate has fired
             * WT_EVICT_HP_GATE_RETRY_LIMIT times already, in which case fall to single-phase to
             * guarantee eventual eviction for a persistent hazard pointer.
             */
            if (page->evict_hp_gate_count >= WT_EVICT_HP_GATE_RETRY_LIMIT)
                two_phase = false;
        } else if (page->evict_hp_gate_count < WT_EVICT_HP_GATE_RETRY_LIMIT) {
            ++page->evict_hp_gate_count;
            /*
             * Clear WT_PAGE_EVICT_LRU so the walk can re-queue this page once the cooldown
             * expires. The queue slot was consumed when the worker called __evict_get_ref; the
             * flag is the only remaining marker that keeps the walk from re-queuing.
             */
            F_CLR_ATOMIC_16(ref->page, WT_PAGE_EVICT_LRU);
            __wt_atomic_store_uint64_relaxed(&page->evict_pass_gen,
              __wt_atomic_load_uint64_relaxed(&conn->evict->evict_pass_gen) +
                WT_EVICT_HP_COOLDOWN_PASSES);
            __wti_evict_read_gen_bump(session, page);
            ret = __wt_set_return(session, EBUSY);
            goto err;
        } else {
            two_phase = false;
        }
    }

    /*
     * Track the largest page size seen at eviction, it tells us something about our ability to
     * force pages out before they're larger than the cache. We don't care about races, it's just a
     * statistic.
     */
    page_size = __wt_atomic_load_size_relaxed(&page->memory_footprint);

    if (!is_dirty)
        /* Clean page */
        __wt_atomic_stats_max_uint64(
          &conn->evict->evict_max_clean_page_size_per_checkpoint, page_size);
    else
        /* Dirty page */
        __wt_atomic_stats_max_uint64(
          &conn->evict->evict_max_dirty_page_size_per_checkpoint, page_size);

    /* Check if the page has updates */
    if (page->modify != NULL)
        __wt_atomic_stats_max_uint64(
          &conn->evict->evict_max_updates_page_size_per_checkpoint, page_size);

    /*
     * No need to reconcile the page if it is from a dead tree or it is clean. Stable tables on the
     * follower are never modified, and should never be reconciled.
     *
     * Phase 1: the ref stays in WT_REF_MEM during reconciliation. The WT_PAGE_LOCK acquired inside
     * __wt_reconcile prevents concurrent update-chain GC and in-memory child splits.
     *
     * Three page types are NOT reconciled in phase-1 (non-closing path) and are instead deferred
     * to phase-2 under WT_REF_LOCKED:
     *
     * (1) Internal pages: while the parent ref is WT_REF_MEM, readers can navigate to children,
     *     instantiate them from WT_REF_DISK into WT_REF_MEM, dirty them, and re-evict them.
     *     Phase-1 reconcile captures each child's ref->addr into the disk image buffer; if a child
     *     is then re-evicted, its old block is freed to the available list while the parent's
     *     buffer still holds the stale address. With the parent LOCKED in phase-2,
     *     __wt_page_in_func refuses to return a LOCKED ref, so child addresses are stable.
     *
     * (2) Garbage-collection (ingest) leaf pages: phase-1 reconciliation with a prune_timestamp
     *     that cannot fully prune all data results in update-restore (leave_dirty). The subsequent
     *     __wt_split_rewrite replaces the page with a reconciled disk image plus saved updates.
     *     This transformed page structure prevents future reconciliation from making GC progress
     *     even when the prune_timestamp advances. Deferring to phase-2 ensures the hazard check
     *     runs first, matching the old exclusive-lock-throughout eviction behavior.
     *
     * (3) History store pages: while the page is WT_REF_MEM, any session can insert new history
     *     entries onto it. Phase-1 reconcile captures rec_start_pinned_stable_ts at start; a
     *     concurrent HS insert can add an entry with upd_durable_ts > stable_ts, which
     *     rec_visibility.c asserts cannot exist on a HS page. Under WT_REF_LOCKED no new HS
     *     writes can land on the page, so deferring to phase-2 eliminates the race.
     *
     * (4) When RTS is active (two_phase == false): ALL page types skip phase-1 reconciliation to
     *     avoid races with concurrent RTS update-chain modifications. Reconciliation is deferred
     *     to phase-2 under WT_REF_LOCKED, where RTS will see the page as locked and skip it.
     *
     * The closing path already holds the tree exclusively (ref arrives pre-LOCKED), so neither
     * race exists; all page types are reconciled here on the closing path.
     */
    if (!tree_dead && is_dirty &&
      (closing ||
        (two_phase && !F_ISSET(ref, WT_REF_FLAG_INTERNAL) &&
          !F_ISSET(S2BT(session), WT_BTREE_GARBAGE_COLLECT) &&
          !F_ISSET(session->dhandle, WT_DHANDLE_HS)))) {
        WT_ASSERT(session, ref->page->disagg_info == NULL || conn->layered_table_manager.leader);
        ret = __evict_reconcile(session, ref, flags);
        /*
         * If reconciliation returned EBUSY because RTS started during the reconcile setup (the
         * safety check inside __evict_reconcile), fall back to phase-2 reconciliation under
         * WT_REF_LOCKED. Do not go to err — the page is still valid and dirty.
         */
        if (ret == EBUSY) {
            ret = 0;
            two_phase = false;
            /*
             * Phase-1 reconcile was partially executed before RTS became active. That work is
             * discarded; count it against the dirty-gap budget so this page is not indefinitely
             * retried with a wasted phase-1 on every attempt while RTS remains active.
             */
            ++page->evict_dirty_gap_count;
        } else
            WT_ERR(ret);
    }

    /*
     * Phase 2: acquire exclusive access for the swap-out. CAS the ref from WT_REF_MEM to
     * WT_REF_LOCKED and clear our own hazard pointer. After this point the only allowed failure is
     * EBUSY.
     */
    if (!closing) {
        WT_ERR(__evict_acquire_exclusive(session, ref, previous_state, &acquired));

        /*
         * Mark the page as being in Phase-2. This is a guard for __evict_push_candidate: after this
         * flag is set, any racing walk thread that reads flags_atomic will either see
         * WT_PAGE_EVICT_PHASE2 directly and bail out, or will have read flags_atomic before we set
         * it and their subsequent CAS to set WT_PAGE_EVICT_LRU will fail atomically (because
         * flags_atomic changed). This closes the race window completely.
         */
        F_SET_ATOMIC_16(ref->page, WT_PAGE_EVICT_PHASE2);

        /*
         * Now that the ref is LOCKED and PHASE2 is set, remove it from the LRU eviction queue. We
         * have to do this before freeing the page memory or otherwise touching the reference
         * because eviction paths assume a non-NULL reference on the queue is pointing at valid
         * memory. After __wti_evict_list_clear_page returns, no new caller can set
         * WT_PAGE_EVICT_LRU (PHASE2 prevents it), so __wt_page_out's assertion is safe.
         */
        __wti_evict_list_clear_page(session, ref);

        /*
         * Check for hazard pointers from other sessions. Readers that published their hazard
         * pointer before our CAS may still hold it. Those readers are actively using the page and
         * we cannot swap it out yet.
         *
         * The ref is already LOCKED, so no new hazard pointers can be published. Spin briefly
         * waiting for existing holders to release their pointers; they will do so as soon as they
         * finish their current operation. A short pause avoids discarding all phase-1
         * reconciliation work for transient readers. Cap the spin to avoid burning CPU when a
         * reader is slow (e.g. sleeping inside a cursor operation).
         *
         * App-thread assist eviction (non-internal, non-urgent): do not spin at all. The app
         * thread must return to its caller promptly; a multi-microsecond spin per failed page
         * compounds into large CPU overhead at high eviction rates (FTDC showed 243% of one core
         * consumed by app-thread eviction on TPCC out-of-cache). Background eviction workers and
         * urgent eviction spin up to WT_THOUSAND iterations to preserve phase-1 work for transient
         * readers.
         *
         * On any cap-out (app-thread or worker): defer re-selection by advancing
         * page->evict_pass_gen to a future generation (WT_EVICT_HP_COOLDOWN_PASSES ahead). The
         * eviction walk skips pages whose evict_pass_gen exceeds the current pass, preventing the
         * server from repeatedly selecting a page it cannot swap out. Also bump read_gen so the
         * page sorts to the back of the LRU queue in clean-eviction mode.
         *
         * For urgent (force-evict) pages: reconciliation has already freed obsolete updates via
         * __rec_save_delete_hs_upd_and_free_obs_updates, reducing the page's memory footprint
         * below the threshold that triggered forced eviction. Re-set WT_READGEN_EVICT_SOON so the
         * eviction server picks up the already-reconciled page as soon as hazard pointers drain,
         * rather than waiting for normal LRU scoring.
         */
        for (uint32_t haz_spin = 0; __wt_hazard_check(session, ref, NULL) != NULL; ++haz_spin) {
            WT_STAT_CONN_DSRC_INCR(session, cache_eviction_blocked_hazard);

            if (haz_spin > WT_THOUSAND || app_thread_assist) {
                if (app_thread_assist) {
                    /*
                     * Track phase-2 HP bails so the pre-phase-1 gate switches to single-phase once
                     * the HP is deemed persistent.
                     */
                    ++page->evict_hp_gate_count;
                    WT_STAT_CONN_DSRC_INCR(session, cache_eviction_blocked_hazard_app_thread);
                }
                if (LF_ISSET(WT_EVICT_CALL_URGENT) && is_dirty)
                    __wt_evict_page_soon(session, ref);
                else {
                    __wt_atomic_store_uint64_relaxed(&page->evict_pass_gen,
                      __wt_atomic_load_uint64_relaxed(&conn->evict->evict_pass_gen) +
                        WT_EVICT_HP_COOLDOWN_PASSES);
                    __wti_evict_read_gen_bump(session, page);
                }
                ret = __wt_set_return(session, EBUSY);
                goto err;
            }
            WT_PAUSE();
        }

        /*
         * When RTS is active, phase-1 reconciliation was skipped for ALL page types. Reconcile
         * everything here under WT_REF_LOCKED. RTS will see the page as locked and skip it,
         * avoiding races with concurrent update-chain modifications.
         *
         * Re-check whether the page is dirty: in the old single-phase model the CAS to LOCKED
         * happened before __evict_review, so is_dirty was always read under the exclusive lock.
         * In the two-phase model is_dirty is read while the ref is still WT_REF_MEM. RTS (or any
         * other concurrent writer) can dirty a previously clean page in the window between the
         * is_dirty read and the CAS above. Without this re-check, we would call
         * __evict_page_dirty_update on a page with rec_result == 0, causing a panic.
         */
        if (!two_phase && !tree_dead) {
            is_dirty = is_dirty || __wt_page_is_modified(page);
            if (is_dirty) {
                if (F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
                    ret = __evict_child_check(session, ref);
                    if (ret != 0) {
                        WT_STAT_CONN_INCR(
                          session, eviction_fail_active_children_on_an_internal_page);
                        goto err;
                    }
                }
                WT_ERR(__evict_reconcile(session, ref, flags));
            }
        }

        /*
         * The remaining phase-2 checks are specific to two-phase eviction. When RTS is active
         * (!two_phase), all reconciliation was already handled in the block above.
         *
         * For internal pages: re-run the child check now that we hold the exclusive lock.
         *
         * __evict_review (including __evict_child_check) ran during phase-1 while the ref was
         * still WT_REF_MEM. Between that check and our phase-2 CAS above, a reader may have
         * transitioned a child from WT_REF_DISK to WT_REF_MEM, setting a hazard pointer on that
         * child's WT_REF struct. If we proceeded to evict the parent now, we would free the page
         * index (which contains the child WT_REF structs) while that reader holds a live pointer
         * to one of them.
         *
         * We now hold WT_REF_LOCKED, so no new readers can navigate to this page. Re-running the
         * child check here is safe and ensures no in-memory children exist at swap-out time.
         */
        if (two_phase && F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
            /* __wt_evict already holds the split generation; call directly. */
            ret = __evict_child_check(session, ref);
            if (ret != 0) {
                WT_STAT_CONN_INCR(session, eviction_fail_active_children_on_an_internal_page);
                goto err;
            }

            /*
             * Reconcile the dirty internal page now, under WT_REF_LOCKED. Phase-1 skipped this
             * reconcile to avoid the child-address race (see the phase-1 comment above). Now that
             * the parent is LOCKED and all children are WT_REF_DISK, no new reader can navigate
             * here to instantiate a child, so child addresses captured into the disk image are
             * stable for the life of this reconcile.
             *
             * Also handles the dirty-gap: if the internal page was clean at review time but was
             * dirtied (e.g., by a concurrent page split whose new children were then evicted to
             * DISK before __evict_child_check ran), reconcile it now under LOCKED.
             */
            is_dirty = is_dirty || __wt_page_is_modified(page);
            if (!tree_dead && is_dirty) {
                WT_ASSERT(
                  session, ref->page->disagg_info == NULL || conn->layered_table_manager.leader);
                WT_ERR(__evict_reconcile(session, ref, flags));
            }
        }

        /*
         * All three non-internal leaf-page cases share the outer guard. Skip entirely for dead
         * trees: their pages are discarded without writing by __evict_page_clean_update, so
         * reconciling here would produce I/O that is immediately thrown away.
         */
        if (two_phase && !F_ISSET(ref, WT_REF_FLAG_INTERNAL) && !tree_dead) {
            if (F_ISSET(S2BT(session), WT_BTREE_GARBAGE_COLLECT)) {
                /*
                 * Garbage-collection (ingest) leaf pages: reconcile under WT_REF_LOCKED. Phase-1
                 * skipped this to avoid the update-restore/split-rewrite cycle that prevents GC
                 * progress (see the phase-1 comment). The hazard check above has already confirmed
                 * no other session holds the page, so reconciliation with the current
                 * prune_timestamp can proceed safely.
                 *
                 * Re-check whether the page is dirty to cover pages that became dirty between the
                 * is_dirty read (under WT_REF_MEM) and our CAS above.
                 */
                is_dirty = is_dirty || __wt_page_is_modified(page);
                if (is_dirty)
                    WT_ERR(__evict_reconcile(session, ref, flags));
            } else if (F_ISSET(session->dhandle, WT_DHANDLE_HS)) {
                /*
                 * History store pages: reconcile under WT_REF_LOCKED. Phase-1 skipped this to avoid
                 * a race with concurrent HS insertions (see the phase-1 comment). Under the
                 * exclusive lock no new HS entries can be written to this page, so reconciliation
                 * is safe.
                 *
                 * Re-check whether the page is dirty: it may have been clean at review time but had
                 * new HS entries written to it between then and our CAS above.
                 */
                is_dirty = is_dirty || __wt_page_is_modified(page);
                if (is_dirty)
                    WT_ERR(__evict_reconcile(session, ref, flags));
            } else {
                /*
                 * Dirty-gap check for regular leaf pages: between the end of phase-1
                 * reconciliation (WT_PAGE_LOCK released inside __wt_reconcile) and our CAS above,
                 * a writer may have added new updates to the page. Those updates are not in the
                 * reconciled on-disk image and not in the history store. Re-reconcile under the
                 * exclusive lock so all updates are captured before the page is swapped out.
                 *
                 * Two cases require re-reconciliation:
                 *
                 * (a) Phase-1 ran. __wt_reconcile resets page_state to WT_PAGE_DIRTY_FIRST at
                 *     the start and writers atomically increment it. After the CAS
                 *     (DIRTY_FIRSTCLEAN) in __rec_write_page_status, a concurrent writer
                 *     increments page_state from CLEAN (0) back to DIRTY_FIRST (1). Detect a
                 *     genuine dirty-gap (writer incremented page_state above DIRTY_FIRST during
                 *     phase-1, causing the CAS to fail) vs. a leave_dirty page (reconcile
                 *     intentionally kept the page dirty for update-restore):
                 *     - page_state > DIRTY_FIRST: a writer was concurrent with phase-1 (CAS
                 *       failed). Their updates may not be in the disk image. Re-reconcile.
                 *     - page_state == DIRTY_FIRST: use rec_leave_dirty to distinguish leave_dirty
                 *       (true: skip re-reconcile, in-memory update chain is intentionally
                 *       preserved) from a single post-CAS writer that incremented CLEAN to
                 *       DIRTY_FIRST (false: re-reconcile).
                 *
                 * (b) Phase-1 was skipped entirely (is_dirty was false). Any current modification
                 *     is a genuine dirty-gap: we must reconcile before calling
                 *     __evict_page_dirty_update or it will assert ref->addr == NULL.
                 *
                 * Internal pages and GC leaf pages are reconciled in the branches above; this
                 * dirty-gap check only runs for regular leaf pages reconciled in phase-1.
                 */
                uint32_t pg_state = page->modify != NULL ?
                  __wt_atomic_load_uint32_acquire(&page->modify->page_state) :
                  WT_PAGE_CLEAN;
                bool dirty_gap = false;
                if (!is_dirty)
                    dirty_gap = (pg_state != WT_PAGE_CLEAN);
                else if (pg_state > WT_PAGE_DIRTY_FIRST)
                    dirty_gap = true;
                else if (pg_state == WT_PAGE_DIRTY_FIRST)
                    /* Use rec_leave_dirty to distinguish leave_dirty from post-CAS single writer. */
                    dirty_gap = (page->modify != NULL && !page->modify->rec_leave_dirty);

                if (dirty_gap) {
                    is_dirty = true;
                    /*
                     * Phase-1 reconcile is being discarded: a concurrent writer modified the page
                     * between the phase-1 lock release and our phase-2 CAS. Count this against the
                     * dirty-gap budget so write-hot pages fall back to single-phase after
                     * WT_EVICT_TWO_PHASE_RETRY_LIMIT wasted reconciles.
                     */
                    ++page->evict_dirty_gap_count;
                    WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_gap_rereconcile);
                    WT_ERR(__evict_reconcile(session, ref, flags));
                }
            }
        }
    }

    /* After this spot, the only recoverable failure is EBUSY. */
    ebusy_only = true;

    /*
     * Check we are not evicting an accessible internal page with an active split generation. We
     * should be able to evict anything if we are closing the dhandle, when the dhandle is already
     * dead, or when we have exclusive access to the dhandle.
     */
    WT_ASSERT(session,
      closing || !F_ISSET(ref, WT_REF_FLAG_INTERNAL) ||
        F_ISSET(session->dhandle, WT_DHANDLE_DEAD | WT_DHANDLE_EXCLUSIVE) ||
        !__wt_gen_active(session, WT_GEN_SPLIT, page->pg_intl_split_gen));

    /* Count evictions of internal pages during normal operation. */
    if (!closing && F_ISSET(ref, WT_REF_FLAG_INTERNAL))
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_internal);

    /* Figure out whether reconciliation was done on the page */
    if (__wt_page_evict_clean(page)) {
        evict_clean = true;
        FLD_SET(stats_flags, WT_EVICT_STATS_CLEAN);
    }

    /* Update the reference and discard the page. */
    if (__wt_ref_is_root(ref))
        __wt_ref_out(session, ref);
    else if ((evict_clean && !F_ISSET(S2BT(session), WT_BTREE_IN_MEMORY)) || tree_dead)
        /*
         * Pages that belong to dead trees never write back to disk and can't support page splits.
         */
        WT_ERR(__evict_page_clean_update(session, ref, flags));
    else
        WT_ERR(__evict_page_dirty_update(session, ref, flags));

    /*
     * We have loaded the new disk image and updated the tree structure. We can no longer fail after
     * this point.
     */

    if (0) {
err:
        ++page->evict_page_attempts;
        __wt_atomic_stats_max_uint16(
          &conn->evict->evict_max_evict_page_attempts, page->evict_page_attempts);

        /*
         * Restore the ref state. We hold WT_REF_LOCKED in exactly two cases:
         *   (a) closing path: the tree is exclusively locked so the CAS at entry always succeeds.
         *   (b) non-closing path: __evict_acquire_exclusive succeeded (acquired == true).
         * In all other non-closing failure paths (state changed, review failed before phase-2 CAS)
         * the ref is still WT_REF_MEM and must NOT be unlocked here.
         *
         * Clear the Phase-2 guard flag before restoring the ref state. WT_PAGE_EVICT_PHASE2 is set
         * at the start of the normal phase-2 block. If we fail inside that block (e.g., EBUSY from
         * the hazard check), the page goes back to WT_REF_MEM and must be eligible for future
         * eviction. In the inmem_split path, acquired is also true but PHASE2 is never set; the CLR
         * is a no-op in that case and is safe.
         */
        if (acquired)
            F_CLR_ATOMIC_16(ref->page, WT_PAGE_EVICT_PHASE2);
        if (closing || acquired)
            __evict_exclusive_clear(session, ref, previous_state);

        if (ebusy_only && ret != EBUSY)
            WT_RET_PANIC(session, ret, "eviction failed when only EBUSY is allowed");
    }

done:
    /* On the non-closing path, clear our hazard pointer if phase-2 never claimed exclusive access. */
    if (!closing && !acquired)
        WT_IGNORE_RET(__wt_hazard_clear(session, ref));

    if (ret == 0)
        FLD_SET(stats_flags, WT_EVICT_STATS_SUCCESS);
    __evict_stats_update(session, stats_flags);

    /* Leave any local eviction generation. */
    WT_LEAVE_GENERATION(session, WT_GEN_SPLIT);
    WT_LEAVE_GENERATION(session, WT_GEN_EVICT);

    return (ret);
}

/*
 * __evict_delete_ref --
 *     Mark a page reference deleted and check if the parent can reverse split.
 */
static int
__evict_delete_ref(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags)
{
    WT_DECL_RET;
    WT_PAGE *parent;
    WT_PAGE_INDEX *pindex;
    uint32_t ndeleted;

    if (__wt_ref_is_root(ref))
        return (0);

    /*
     * Avoid doing reverse splits when closing the file, it is wasted work and some structures may
     * have already been freed.
     */
    if (!LF_ISSET(WT_EVICT_CALL_NO_SPLIT | WT_EVICT_CALL_CLOSING)) {
        parent = ref->home;
        WT_INTL_INDEX_GET(session, parent, pindex);
        ndeleted = __wt_atomic_add_uint32_v(&pindex->deleted_entries, 1);

        /*
         * If more than 10% of the parent references are deleted, try a reverse split. Don't bother
         * if there is a single deleted reference: the internal page is empty and we have to wait
         * for eviction to notice.
         *
         * This will consume the deleted ref (and eventually free it). If the reverse split can't
         * get the access it needs because something is busy, be sure that the page still ends up
         * marked deleted.
         *
         * Don't do it if we are a VLCS tree and the child we're deleting is the leftmost child. The
         * reverse split will automatically remove the page entirely, creating a namespace gap at
         * the beginning of the internal page, and that leaves search nowhere to go. Note that the
         * situation will be handled safely if another child gets deleted, or if eviction comes for
         * a visit.
         */
        if (ndeleted > pindex->entries / 10 && pindex->entries > 1) {
            if (S2BT(session)->type == BTREE_COL_VAR && ref == pindex->index[0])
                WT_STAT_CONN_DSRC_INCR(session, cache_reverse_splits_skipped_vlcs);
            else {
                if ((ret = __wt_split_reverse(session, ref)) == 0) {
                    WT_STAT_CONN_DSRC_INCR(session, cache_reverse_splits);
                    return (0);
                }
                WT_RET_BUSY_OK(ret);

                /*
                 * The child must be locked after a failed reverse split.
                 */
                WT_ASSERT(session, WT_REF_GET_STATE(ref) == WT_REF_LOCKED);
            }
        }
    }

    WT_REF_SET_STATE(ref, WT_REF_DELETED);
    return (0);
}

/*
 * __evict_page_clean_update --
 *     Update a clean page's reference on eviction.
 */
static int
__evict_page_clean_update(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags)
{
    WT_DECL_RET;
    bool closing, instantiated, tree_dead;

    closing = FLD_ISSET(flags, WT_EVICT_CALL_CLOSING);
    tree_dead = F_ISSET(session->dhandle, WT_DHANDLE_DEAD);

    /*
     * We might discard an instantiated deleted page, because instantiated pages are not marked
     * dirty by default. Check this before discarding the modify structure in __wt_ref_out.
     */
    if (ref->page->modify != NULL && ref->page->modify->instantiated)
        instantiated = true;
    else {
        WT_ASSERT(session, ref->page_del == NULL);
        instantiated = false;
    }

    if (!instantiated && !tree_dead && !F_ISSET(S2C(session), WT_CONN_IN_MEMORY) &&
      !F_ISSET(S2BT(session), WT_BTREE_IN_MEMORY) && !closing)
        __evict_page_victim_cache(session, ref);

    /*
     * Discard the page and update the reference structure. A leaf page without a disk address is a
     * deleted page that either was created empty and never written out, or had its on-disk page
     * discarded already after the deletion became globally visible. It is not immediately clear if
     * it's possible to get an internal page without a disk address here, but if one appears it can
     * be deleted. (Note that deleting an internal page implicitly turns it into a leaf.)
     *
     * A page with a disk address is now on disk, unless it was deleted and instantiated and then
     * evicted unmodified, in which case it is still deleted. In the latter case set the state back
     * to WT_REF_DELETED.
     */
    __wt_ref_out(session, ref);
    if (ref->addr == NULL) {
        WT_WITH_PAGE_INDEX(session, ret = __evict_delete_ref(session, ref, flags));
        WT_RET_BUSY_OK(ret);
    } else
        WT_REF_SET_STATE(ref, instantiated ? WT_REF_DELETED : WT_REF_DISK);

    return (0);
}

/*
 * __evict_page_dirty_update --
 *     Update a dirty page's reference on eviction.
 */
static int
__evict_page_dirty_update(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t evict_flags)
{
    WT_ADDR *addr;
    WT_DECL_RET;
    WT_MULTI multi;
    WT_PAGE_MODIFY *mod;
    bool closing;
    void *tmp;

    mod = ref->page->modify;
    closing = FLD_ISSET(evict_flags, WT_EVICT_CALL_CLOSING);

    switch (mod->rec_result) {
    case WT_PM_REC_EMPTY:
        /*
         * Page is empty: Update the parent to reference a deleted page. Reconciliation left the
         * page "empty", so there's no older transaction in the system that might need to see an
         * earlier version of the page. There's no backing address, if we're forced to "read" into
         * that namespace, we instantiate a new page instead of trying to read from the backing
         * store.
         */
        __wt_ref_out(session, ref);
        WT_WITH_PAGE_INDEX(session, ret = __evict_delete_ref(session, ref, evict_flags));
        WT_RET_BUSY_OK(ret);
        break;
    case WT_PM_REC_MULTIBLOCK:
        /*
         * Multiple blocks: Either a split where we reconciled a page and it turned into a lot of
         * pages or an in-memory page that got too large, we forcibly evicted it, and there wasn't
         * anything to write.
         *
         * The latter is a special case of forced eviction. Imagine a thread updating a small set
         * keys on a leaf page. The page is too large or has too many deleted items, so we try and
         * evict it, but after reconciliation there's only a small amount of live data (so it's a
         * single page we can't split), and if there's an older reader somewhere, there's data on
         * the page we can't write (so the page can't be evicted). In that case, we end up here with
         * a single block that we can't write. Take advantage of the fact we have exclusive access
         * to the page and rewrite it in memory.
         */
        if (mod->mod_multi_entries == 1) {
            WT_ASSERT(session, closing == false);
            WT_RET(__wt_split_rewrite(session, ref, &mod->mod_multi[0], true));
        } else
            WT_RET(__wt_split_multi(session, ref, closing));
        break;
    case WT_PM_REC_REPLACE:
        /*
         * Eviction wants to keep this page if we have a disk image, re-instantiate the page in
         * memory, else discard the page.
         */
        if (mod->mod_disk_image == NULL) {
            /*
             * 1-for-1 page swap: Update the parent to reference the replacement page.
             *
             * It's possible to see an empty disk address if the previous reconciliation skipped
             * writing the page.
             */
            if (mod->mod_replace.block_cookie != NULL) {
                WT_ASSERT(session, ref->addr == NULL);
                WT_RET(__wt_calloc_one(session, &addr));
                *addr = mod->mod_replace;
                mod->mod_replace.block_cookie = NULL;
                mod->mod_replace.block_cookie_size = 0;
                ref->addr = addr;
            } else
                WT_ASSERT(
                  session, F_ISSET(S2BT(session), WT_BTREE_DISAGGREGATED) && ref->addr != NULL);
            __wt_page_modify_clear(session, ref->page);
            __wt_ref_out(session, ref);
            WT_REF_SET_STATE(ref, WT_REF_DISK);
        } else {
            /* The split code works with WT_MULTI structures, build one for the disk image. */
            memset(&multi, 0, sizeof(multi));
            multi.disk_image = mod->mod_disk_image;
            multi.addr = mod->mod_replace;
            if (ref->page->disagg_info != NULL) {
                WT_RET(__wt_calloc_one(session, &multi.block_meta));
                *multi.block_meta = ref->page->disagg_info->block_meta;
            }
            /*
             * Store the disk image to a temporary pointer in case we fail to rewrite the page and
             * we need to link the new disk image back to the old disk image.
             */
            tmp = mod->mod_disk_image;
            mod->mod_disk_image = NULL;
            ret = __wt_split_rewrite(session, ref, &multi, true);
            __wt_free(session, multi.block_meta);
            if (ret != 0) {
                mod->mod_disk_image = tmp;
                return (ret);
            }
        }

        break;
    default:
        return (__wt_illegal_value(session, mod->rec_result));
    }

    return (0);
}

/*
 * __evict_child_check --
 *     Review an internal page for active children.
 */
static int
__evict_child_check(WT_SESSION_IMPL *session, WT_REF *parent)
{
    WT_REF *child;
    bool busy, visible;

    busy = false;

    /*
     * There may be cursors in the tree walking the list of child pages. The parent is locked, so
     * all we care about is cursors already in the child pages, no thread can enter them. Any cursor
     * moving through the child pages must be hazard pointer coupling between pages, where the page
     * on which it currently has a hazard pointer must be in a state other than on-disk. Walk the
     * child list forward, then backward, to ensure we don't race with a cursor walking in the
     * opposite direction from our check.
     */
    WT_INTL_FOREACH_BEGIN (session, parent->page, child) {
        /* It isn't safe to evict if there is a child on the pre-fetch queue. */
        if (F_ISSET_ATOMIC_8(child, WT_REF_FLAG_PREFETCH)) {
            busy = true;
            break;
        }

        switch (WT_REF_GET_STATE(child)) {
        case WT_REF_DISK:    /* On-disk */
        case WT_REF_DELETED: /* On-disk, deleted */
            break;
        default:
            busy = true;
        }
        if (busy)
            break;
    }
    WT_INTL_FOREACH_END;

    if (busy)
        return (__wt_set_return(session, EBUSY));

    WT_INTL_FOREACH_REVERSE_BEGIN (session, parent->page, child) {
        switch (WT_REF_GET_STATE(child)) {
        case WT_REF_DISK:    /* On-disk */
        case WT_REF_DELETED: /* On-disk, deleted */
            break;
        default:
            return (__wt_set_return(session, EBUSY));
        }
    }
    WT_INTL_FOREACH_END;

    /*
     * It is always OK to evict pages from checkpoint cursor trees if they don't have children, and
     * visibility checks for pages found to be deleted in the checkpoint aren't needed (or correct
     * when done in eviction threads).
     */
    if (WT_READING_CHECKPOINT(session))
        return (0);

    /*
     * The fast check is done and there are no cursors in the child pages. Make sure the child
     * WT_REF structures pages can be discarded.
     */
    WT_INTL_FOREACH_BEGIN (session, parent->page, child) {

        switch (WT_REF_GET_STATE(child)) {
        case WT_REF_DISK: /* On-disk */
            break;
        case WT_REF_DELETED: /* On-disk, deleted */
                             /*
                              * If the child page was part of a truncate, transaction rollback might
                              * switch this page into its previous state at any time, so the delete
                              * must be resolved before the parent can be evicted.
                              *
                              * We have the internal page locked, which prevents a search from
                              * descending into it. However, a walk from an adjacent leaf page could
                              * attempt to hazard couple into a child page and free the page_del
                              * structure as we are examining it. Flip the state to locked to make
                              * this check safe: if that fails, we have raced with a read and should
                              * give up on evicting the parent.
                              */
            if (!WT_REF_CAS_STATE(session, child, WT_REF_DELETED, WT_REF_LOCKED))
                return (__wt_set_return(session, EBUSY));
            /*
             * Insert a read/acquire barrier so we're guaranteed the page_del state we read below
             * comes after the locking operation on the ref state and therefore after the previous
             * unlock of the ref. Otherwise we might read an inconsistent view of the page deletion
             * info, and while many combinations are harmless and would just lead us to falsely
             * refuse to evict, some (e.g. reading committed as true and a stale durable timestamp
             * from before it was set by commit) are not.
             *
             * Note that while ordinarily a lock acquire should have an acquire (read/any) barrier
             * after it, because we are only reading the write part is irrelevant and a read/read
             * barrier is sufficient.
             *
             * FIXME-WT-9780: this and the CAS should be rolled into a WT_REF_TRYLOCK macro.
             */
            WT_ACQUIRE_BARRIER();

            /*
             * We can evict any truncation that's committed. However, restrictions in reconciliation
             * mean that it needs to be visible to us when we get there. And unfortunately we are
             * upstream of the point where eviction threads get snapshots. Plus, application threads
             * doing eviction can see their own uncommitted truncations. So, use the following
             * logic:
             *     1. First check if the operation is committed. If not, it's not visible for these
             *        purposes.
             *     2. If we already have a snapshot, use it to check visibility.
             *     3. If we do not but we're an eviction thread, go ahead. We will get a snapshot
             *        shortly and any committed operation will be visible in it.
             *     4. Otherwise, check if the operation is globally visible.
             *
             * Even though we specifically can't evict prepared truncations, we don't need to deploy
             * the special-case logic for prepared transactions in __wt_page_del_visible; prepared
             * transactions aren't committed so they'll fail the first check.
             */
            if (!__wt_page_del_committed_set(child->page_del))
                visible = false;
            else if (F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT))
                visible = __wt_page_del_visible(session, child->page_del, false);
            else if (F_ISSET(session, WT_SESSION_EVICTION))
                visible = true;
            else
                visible = __wt_page_del_visible_all(session, child->page_del, false);
            WT_REF_SET_STATE(child, WT_REF_DELETED);
            if (!visible)
                return (__wt_set_return(session, EBUSY));
            break;
        default:
            return (__wt_set_return(session, EBUSY));
        }
    }
    WT_INTL_FOREACH_END;

    return (0);
}

/*
 * __evict_review_obsolete_time_window --
 *     Check whether the ref has obsolete time window information and mark it for dirty eviction to
 *     remove those obsolete data. An exclusive lock on the page has already been obtained by the
 *     caller.
 */
static int
__evict_review_obsolete_time_window(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_ADDR_COPY addr;
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_MULTI *multi;
    WT_PAGE_MODIFY *mod;
    WT_TIME_AGGREGATE newest_ta;
    uint32_t i;
    char time_string[WT_TIME_STRING_SIZE];

    btree = S2BT(session);
    conn = S2C(session);

    /* Too many pages have been cleaned for this btree. */
    if (__wt_atomic_load_uint32_relaxed(&btree->eviction_obsolete_tw_pages) >=
      conn->heuristic_controls.eviction_obsolete_tw_pages_dirty_max)
        return (0);

    /*
     * Pages that the application threads are evicting should not be included. Reconciliation must
     * be performed when converting a clean page to a dirty page, which can increase latency. This
     * check is bypassed if the session is configured with a debug option to evict the page when it
     * is released and no longer needed.
     */
    if (!F_ISSET(session, WT_SESSION_EVICTION) && !F_ISSET(session, WT_SESSION_DEBUG_RELEASE_EVICT))
        return (0);

    /* Do not perform any obsolete time window cleanup during the startup or shutdown phase. */
    if (F_ISSET(conn, WT_CONN_RECOVERING) || F_ISSET_ATOMIC_32(conn, WT_CONN_CLOSING_CHECKPOINT))
        return (0);

    /* If the file is being checkpointed, other threads can't evict dirty pages. */
    if (__wt_btree_syncing_by_other_session(session))
        return (0);

    /* The checkpoint cursor dhandle is read-only. Do not mark these pages as dirty. */
    if (F_ISSET(btree, WT_BTREE_READONLY))
        return (0);

    /*
     * Rewriting internal pages doesn't clean the obsolete time window until the leaf pages are
     * cleared from the obsolete time window.
     */
    WT_ASSERT(session, ref->page != NULL);
    if (WT_PAGE_IS_INTERNAL(ref->page))
        return (0);

    /* We are only interested in clean pages. */
    if (__wt_page_is_modified(ref->page))
        return (0);

    /* Limit the number of btrees that can be cleaned up. */
    if (__wt_atomic_load_uint32_relaxed(&btree->eviction_obsolete_tw_pages) == 0 &&
      __wt_atomic_load_uint32_relaxed(&btree->checkpoint_cleanup_obsolete_tw_pages) == 0 &&
      __wt_atomic_load_uint32_relaxed(&conn->heuristic_controls.obsolete_tw_btree_count) >=
        conn->heuristic_controls.obsolete_tw_btree_max)
        return (0);

    /* Don't add more cache pressure. */
    if (__wt_evict_needed(session, false, false, false, NULL) || __wt_evict_cache_stuck(session))
        return (0);

    /*
     * Initialize the time aggregate via the merge initialization, so that stop visibility is copied
     * across correctly. That is why we need the stop timestamp/transaction IDs to start as none,
     * otherwise we'd never mark anything as obsolete.
     */
    WT_TIME_AGGREGATE_INIT_MERGE(&newest_ta);

    mod = ref->page->modify;
    if (mod != NULL && mod->rec_result == WT_PM_REC_MULTIBLOCK) {
        /* Calculate the max stop time point by traversing all multi addresses. */
        for (multi = mod->mod_multi, i = 0; i < mod->mod_multi_entries; ++multi, ++i)
            WT_TIME_AGGREGATE_MERGE(session, &newest_ta, &multi->addr.ta);
    } else if (mod != NULL && mod->rec_result == WT_PM_REC_REPLACE)
        WT_TIME_AGGREGATE_COPY(&newest_ta, &mod->mod_replace.ta);
    else if (__wt_ref_addr_copy(session, ref, &addr))
        WT_TIME_AGGREGATE_COPY(&newest_ta, &addr.ta);

    /* The pages that are removed are eliminated during the checkpoint cleanup procedure. */
    if (WT_TIME_AGGREGATE_HAS_STOP(&newest_ta))
        return (0);

    /*
     * Mark the page as dirty to allow the page reconciliation to remove all information related to
     * an obsolete time window.
     */
    if (__wt_txn_has_newest_and_visible_all(session, newest_ta.newest_txn,
          WT_MAX(newest_ta.newest_start_durable_ts, newest_ta.newest_stop_durable_ts))) {
        __wt_verbose_debug2(session, WT_VERB_EVICTION,
          "%p in-memory page obsolete time window: time aggregate %s", (void *)ref,
          __wt_time_aggregate_to_string(&newest_ta, time_string));

        WT_RET(__wt_page_modify_init(session, ref->page));
        __wt_page_modify_set(session, ref->page);

        /*
         * Save that another tree has been processed if that's the first time it gets cleaned and
         * update the number of pages made dirty for that tree.
         */
        if (__wt_atomic_load_uint32_relaxed(&btree->eviction_obsolete_tw_pages) == 0 &&
          __wt_atomic_load_uint32_relaxed(&btree->checkpoint_cleanup_obsolete_tw_pages) == 0)
            __wt_atomic_add_uint32_relaxed(&conn->heuristic_controls.obsolete_tw_btree_count, 1);
        __wt_atomic_add_uint32_relaxed(&btree->eviction_obsolete_tw_pages, 1);
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_obsolete_tw);
    }

    return (0);
}

/*
 * __evict_review --
 *     Review the page and its subtree for conditions that would block its eviction.
 */
static int
__evict_review(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t evict_flags, bool *inmem_splitp)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_PAGE *page;
    bool closing, modified;

    *inmem_splitp = false;

    btree = S2BT(session);
    conn = S2C(session);
    page = ref->page;
    closing = FLD_ISSET(evict_flags, WT_EVICT_CALL_CLOSING);

    /*
     * Fail if an internal has active children, the children must be evicted first. The test is
     * necessary but shouldn't fire much: the eviction code is biased for leaf pages, an internal
     * page shouldn't be selected for eviction until all children have been evicted.
     */
    if (F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
        WT_WITH_PAGE_INDEX(session, ret = __evict_child_check(session, ref));
        if (ret != 0)
            WT_STAT_CONN_INCR(session, eviction_fail_active_children_on_an_internal_page);
        WT_RET(ret);
    }

    /* It is always OK to evict pages from dead trees if they don't have children. */
    if (F_ISSET(session->dhandle, WT_DHANDLE_DEAD))
        return (0);

    /* Review the obsolete time window information before eviction. */
    WT_RET(__evict_review_obsolete_time_window(session, ref));

    /*
     * Retrieve the modified state of the page. This must happen after the check for evictable
     * internal pages otherwise there is a race where a page could be marked modified due to a child
     * being transitioned to WT_REF_DISK after the modified check and before we visited the ref
     * while walking the parent index.
     */
    modified = __wt_page_is_modified(page);

    /*
     * Clean pages can't be evicted from in memory btrees. This should be uncommon - we don't add
     * clean pages to the queue.
     */
    if (F_ISSET(btree, WT_BTREE_IN_MEMORY) && !modified && !closing)
        return (__wt_set_return(session, EBUSY));

    /* Check if the page can be evicted. */
    if (!closing) {
        /*
         * Update the oldest ID to avoid wasted effort should it have fallen behind current.
         */
        if (modified)
            WT_RET(__wt_txn_update_oldest(session, WT_TXN_OLDEST_STRICT));

        if (!__wt_page_can_evict(session, ref, inmem_splitp))
            return (__wt_set_return(session, EBUSY));

        /* Check for an append-only workload needing an in-memory split. */
        if (*inmem_splitp)
            return (0);
    }

    /* If the page is clean, we're done and we can evict. */
    if (!modified)
        return (0);

    /*
     * If we are trying to evict a dirty page that does not belong to history store(HS) and
     * checkpoint is processing the HS file, avoid evicting the dirty non-HS page for now if the
     * cache is already dominated by dirty HS content.
     *
     * Evicting an non-HS dirty page can generate even more HS content. As we cannot evict HS pages
     * while checkpoint is operating on the HS file, we can end up in a situation where we exceed
     * the cache size limit.
     */
    if (__wt_tsan_suppress_load_bool_v(&conn->txn_global.checkpoint_running_hs) &&
      !WT_IS_HS(btree->dhandle) && __wti_evict_hs_dirty(session) && __wt_cache_full(session)) {
        WT_STAT_CONN_INCR(session, cache_eviction_blocked_checkpoint_hs);
        return (__wt_set_return(session, EBUSY));
    }

    if (!F_ISSET(session, WT_SESSION_DEBUG_RELEASE_EVICT) && F_ISSET(ref, WT_REF_FLAG_LEAF)) {
        if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT)) {
            /*
             * If garbage collection is enabled and this page was already reconciled at the current
             * prune timestamp, do not attempt reconciliation again. Repeating the reconciliation
             * without the prune timestamp advancing will yield no progress in garbage collection.
             */
            wt_timestamp_t prune_timestamp =
              __wt_atomic_load_uint64_acquire(&btree->prune_timestamp);
            if (prune_timestamp != WT_TS_NONE &&
              page->modify->rec_prune_timestamp >= prune_timestamp) {
                WT_STAT_CONN_INCR(session, cache_eviction_blocked_prune_timestamp);
                return (__wt_set_return(session, EBUSY));
            }
        } else if (F_ISSET(conn, WT_CONN_PRECISE_CHECKPOINT)) {
            /*
             * If precise checkpoints are enabled, and this page was already reconciled at a time
             * that services the checkpoint, don't try again. Reconciling the page again without the
             * timestamp moving would result in the same page being written out as last time.
             */
            wt_timestamp_t checkpoint_timestamp =
              __wt_atomic_load_uint64_acquire(&conn->txn_global.checkpoint_timestamp);
            if (checkpoint_timestamp != WT_TS_NONE &&
              page->modify->rec_pinned_stable_timestamp >= checkpoint_timestamp) {
                WT_STAT_CONN_INCR(session, cache_eviction_blocked_precise_checkpoint);
                return (__wt_set_return(session, EBUSY));
            }
        }
    }

    /*
     * If reconciliation is disabled for this thread (e.g., during an eviction that writes to the
     * history store or reading a checkpoint), give up.
     */
    if (F_ISSET(session, WT_SESSION_NO_RECONCILE))
        return (__wt_set_return(session, EBUSY));

    return (0);
}

/*
 * __evict_reconcile --
 *     Reconcile the page for eviction.
 */
static int
__evict_reconcile(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t evict_flags)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    uint32_t flags;
    bool closing, is_application_thread_snapshot_refreshed, is_eviction_thread,
      use_snapshot_for_app_thread;

    btree = S2BT(session);
    conn = S2C(session);
    flags = WT_REC_EVICT;
    closing = FLD_ISSET(evict_flags, WT_EVICT_CALL_CLOSING);

    evict = conn->evict;
    is_application_thread_snapshot_refreshed = false;

    /*
     * Urgent eviction and forced eviction want two different behaviors for inefficient update
     * restore evictions, pass this flag so that reconciliation knows which to use.
     */
    if (FLD_ISSET(evict_flags, WT_EVICT_CALL_URGENT))
        LF_SET(WT_REC_CALL_URGENT);

    /*
     * If we have an exclusive lock (we're discarding the tree), assert there are no updates we
     * cannot read.
     */
    if (closing)
        LF_SET(WT_REC_EVICT_CALL_CLOSING | WT_REC_VISIBILITY_ERR);
    /*
     * Don't set any other flags for internal pages: there are no update lists to be saved and
     * restored, changes can't be written into the history store table, nor can we re-create
     * internal pages in memory.
     *
     * Don't set any other visibility flags for history store table as all the content is evictable.
     */
    else if (F_ISSET(ref, WT_REF_FLAG_INTERNAL) || WT_IS_HS(btree->dhandle))
        ;
    /* Always do update restore for in-memory btrees. */
    else if (F_ISSET(btree, WT_BTREE_IN_MEMORY))
        LF_SET(WT_REC_IN_MEMORY | WT_REC_SCRUB);
    /* For data store leaf pages, write the history to history store except for metadata. */
    else if (!WT_IS_METADATA(btree->dhandle) && !WT_IS_DISAGG_META(btree->dhandle)) {
        LF_SET(WT_REC_HS);

        /*
         * Scrub and we're supposed to or toss it in sometimes if we are in debugging mode.
         *
         * Note that don't scrub if checkpoint is running on the tree.
         */
        if (!WT_SESSION_BTREE_SYNC(session)) {
            bool can_scrub = (F_ISSET(evict, WT_EVICT_CACHE_SCRUB) ||
              (FLD_ISSET(conn->debug_flags, WT_CONN_DEBUG_EVICT_AGGRESSIVE_MODE) &&
                __wt_random(&session->rnd_random) % 3 == 0));

            /*
             * Scrub only if cache is under the clean eviction target or the page has high read
             * generation (the page is hot and we want to keep it in cache).
             */
            if (can_scrub &&
              (!__wt_evict_clean_needed(session, NULL) ||
                ref->page->read_gen > __evict_read_gen(session))) {
                LF_SET(WT_REC_SCRUB);
            }
        }
    }

    /*
     * We must do scrub dirty eviction for disaggregated storage btrees as we cannot read back the
     * evicted page until they are materialized.
     */
    if (!closing && ref->page->disagg_info != NULL) {
        /*
         * We should not evict dirty internal pages for disaggregated storage as they cannot be
         * recreated in-memory and it doesn't effectively reduce cache usage.
         */
        WT_ASSERT_ALWAYS(session, F_ISSET(ref, WT_REF_FLAG_LEAF),
          "Evicting dirty internal pages for disaggregated storage is not allowed.");
        LF_SET(WT_REC_SCRUB);
    }

    /*
     * Acquire a snapshot if coming through the eviction thread route. Also, if we have entered
     * eviction through application threads then we save the existing snapshot and refresh to
     * acquire a new snapshot, once the application threads are done with eviction then we switch
     * back the snapshot to its original. Avoid using snapshots when application transactions are in
     * the final stages of commit or rollback as they have already released the snapshot. Otherwise,
     * it becomes harder in the later part of the code to detect updates that belonged to the last
     * running application transaction.
     */
    use_snapshot_for_app_thread = !F_ISSET(session, WT_SESSION_INTERNAL) &&
      !WT_IS_METADATA(session->dhandle) && F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT) &&
      !F_ISSET(conn, WT_CONN_PRECISE_CHECKPOINT);
    is_eviction_thread = F_ISSET(session, WT_SESSION_EVICTION);

    /* Make sure that both conditions above are not true at the same time. */
    WT_ASSERT(session, !use_snapshot_for_app_thread || !is_eviction_thread);

    /*
     * If checkpoint is running concurrently, set the checkpoint running flag and we will abort the
     * eviction if we detect any updates without timestamps.
     */
    if (__wt_atomic_load_bool_v_relaxed(&conn->txn_global.checkpoint_running))
        LF_SET(WT_REC_CHECKPOINT_RUNNING);

    /* Eviction thread doing eviction. */
    if (is_eviction_thread) {
        /*
         * Eviction threads do not need to pin anything in the cache. We have an exclusive lock for
         * the page being evicted so we are sure that the page will always be there while it is
         * being processed. Therefore, we use snapshot API that doesn't publish shared IDs to the
         * outside world.
         */
        if (F_ISSET(conn, WT_CONN_PRECISE_CHECKPOINT) && !F_ISSET(btree, WT_BTREE_IN_MEMORY)) {
            uint64_t btree_ckpt_gen, ckpt_gen;
            /*
             * If precise checkpoint is configured, only evict the updates that visible to the
             * ongoing checkpoint for trees haven't been visited by the checkpoint.
             */
            btree_ckpt_gen = __wt_atomic_load_uint64_acquire(&btree->checkpoint_gen);
            ckpt_gen = __wt_gen(session, WT_GEN_CHECKPOINT);
            if (btree_ckpt_gen < ckpt_gen)
                LF_SET(WT_REC_VISIBLE_NO_SNAPSHOT);
            else
                __wt_txn_bump_snapshot(session);
        } else
            __wt_txn_bump_snapshot(session);
    } else if (use_snapshot_for_app_thread) {
        /*
         * If we couldn't make progress with the application thread's existing snapshot, save the
         * existing snapshot and refresh to acquire a new one. Then try eviction again. Once the
         * application threads are done with eviction, the application thread's snapshot is switched
         * back to the original.
         */
        if (F_ISSET(session->txn, WT_TXN_REFRESH_SNAPSHOT)) {
            WT_RET(__wt_txn_snapshot_save_and_refresh(session));
            is_application_thread_snapshot_refreshed = true;
            WT_STAT_CONN_INCR(session, application_evict_snapshot_refreshed);
        }

        LF_SET(WT_REC_APP_EVICTION_SNAPSHOT);
    } else if (!WT_SESSION_BTREE_SYNC(session))
        LF_SET(WT_REC_VISIBLE_NO_SNAPSHOT);

    WT_ASSERT(
      session, LF_ISSET(WT_REC_VISIBLE_NO_SNAPSHOT) || F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT));

    /* We should not be trying to evict using a checkpoint-cursor transaction. */
    WT_ASSERT(session, !F_ISSET(session->txn, WT_TXN_IS_CHECKPOINT));

    /*
     * Reconcile the page. Force read-committed isolation level if we are using snapshots for
     * eviction workers or application threads.
     */
    if ((is_eviction_thread && F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT)) ||
      use_snapshot_for_app_thread)
        WT_WITH_TXN_ISOLATION(
          session, WT_ISO_READ_COMMITTED, ret = __wt_reconcile(session, ref, NULL, flags));
    else
        ret = __wt_reconcile(session, ref, NULL, flags);

    if (ret != 0)
        WT_STAT_CONN_INCR(session, eviction_fail_in_reconciliation);

    if (is_eviction_thread && F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT))
        __wt_txn_release_snapshot(session);
    else if (is_application_thread_snapshot_refreshed)
        __wt_txn_snapshot_release_and_restore(session);

    WT_RET(ret);

    /*
     * Success: assert that the page is clean or reconciliation was configured to save updates.
     *
     * In the two-phase eviction model (non-closing) a concurrent reconciler may have already
     * cleaned the page and we returned early from __wt_reconcile (no-op). A writer can then
     * re-dirty the page between the lock release and this assertion. The dirty-gap check in
     * __wt_evict handles that case, so we allow a dirty page here on the non-closing eviction path.
     */
    WT_ASSERT(session,
      !__wt_page_is_modified(ref->page) || LF_ISSET(WT_REC_HS | WT_REC_IN_MEMORY) ||
        WT_IS_METADATA(btree->dhandle) || WT_IS_DISAGG_META(btree->dhandle) ||
        (LF_ISSET(WT_REC_EVICT) && !LF_ISSET(WT_REC_EVICT_CALL_CLOSING)));

    return (0);
}
