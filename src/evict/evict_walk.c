/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Eviction walk: tree traversal to find and score candidate pages, push them into the LRU queues,
 * and manage per-tree walk state.
 *
 * __wti_evict_walk is the top-level entry point called by __wti_evict_lru_walk. It selects the
 * next data handle to walk via __evict_walk_choose_dhandle (round-robin with a bias toward trees
 * that dominate the cache), then delegates to __evict_walk_tree for the actual page scan.
 *
 * __evict_walk_tree traverses the btree from the saved walk point, calling __evict_try_queue_page
 * for each visited ref. Pages that meet dirty/update/read-gen criteria are scored by
 * __evict_entry_priority and inserted into the queue by __wti_evict_push_candidate. The walk
 * target (how many slots to fill per visit) is computed by __evict_walk_target and adjusted by
 * cache pressure flags.
 *
 * Walk position is preserved across passes using normalized positions (npos / soft pointers).
 * __evict_clear_walk saves the current ref's position before releasing it, and
 * __evict_try_restore_walk_position restores descent to approximately the same point on the next
 * pass. __wti_evict_clear_walk_and_saved_tree_if_current_locked and
 * __wti_evict_clear_all_walks_and_saved_tree are called from the exclusive-eviction path to
 * abandon all in-progress walks before a tree is locked for file eviction.
 */
#include "wt_internal.h"

/*
 * Why modified-page eviction is pointless on a btree right now. Shared by the eviction walker's
 * modified-page gate and the dirty-index drain so the two paths cannot drift: a block reason added
 * here applies to both.
 */
typedef enum {
    WTI_DIRTY_EVICT_OK = 0,     /* Dirty pages may be evicted. */
    WTI_DIRTY_EVICT_SYNCING,    /* The btree is being checkpointed. */
    WTI_DIRTY_EVICT_DISAGG_CKPT /* Disagg btree already visited by the running checkpoint. */
} WTI_DIRTY_EVICT_BLOCK;

static int __evict_clear_walk(WT_SESSION_IMPL *, bool);
static WTI_DIRTY_EVICT_BLOCK __evict_dirty_tree_block(WT_SESSION_IMPL *, WT_BTREE *, bool);
static u_int __evict_dirty_index_drain(
  WT_SESSION_IMPL *, WT_BTREE *, WTI_EVICT_QUEUE *, u_int, u_int *);
static u_int __evict_dirty_index_drain_ring(WT_SESSION_IMPL *, WT_BTREE *, WTI_DIRTY_INDEX *,
  WTI_EVICT_QUEUE *, u_int, u_int *, wt_timestamp_t *, wt_timestamp_t *);
static void __evict_try_queue_page(
  WT_SESSION_IMPL *, WTI_EVICT_QUEUE *, WT_REF *, WT_PAGE *, WTI_EVICT_ENTRY *, bool *, bool *);
static int __evict_walk_tree(WT_SESSION_IMPL *, WTI_EVICT_QUEUE *, u_int, u_int *);

/*
 * __evict_dirty_tree_block --
 *     Return why dirty eviction is pointless on this btree, or WTI_DIRTY_EVICT_OK. A tree being
 *     checkpointed cannot reconcile a page a second time; a disaggregated tree already visited by
 *     the running checkpoint holds only pages bound for the next checkpoint, which fail the
 *     post-lock recheck --
 *     unless eviction is aggressive, when any candidate beats starving the cache. The caller
 *     applies the eviction-mode gate (this matters only when looking for dirty pages) and maps the
 *     reason to its own statistic.
 */
static WTI_DIRTY_EVICT_BLOCK
__evict_dirty_tree_block(WT_SESSION_IMPL *session, WT_BTREE *btree, bool aggressive)
{
    if (WT_BTREE_SYNCING(btree))
        return (WTI_DIRTY_EVICT_SYNCING);
    if (!aggressive && __wt_btree_disagg_checkpointed(session, btree))
        return (WTI_DIRTY_EVICT_DISAGG_CKPT);
    return (WTI_DIRTY_EVICT_OK);
}

/*
 * __evict_drain_stable_blocked --
 *     True while the pinned stable timestamp sits below the median commit timestamp the last drain
 *     pass found blocking the ring. Under a precise checkpoint those pages cannot be evicted, so
 *     the drain would only re-examine and re-insert refs it cannot queue; the walker still evicts
 *     any page that falls below stable in the meantime.
 */
static WT_INLINE bool
__evict_drain_stable_blocked(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    wt_timestamp_t block_ts;

    block_ts = __wt_atomic_load_uint64_relaxed(&btree->drain_stable_block_ts);
    return (block_ts != WT_TS_NONE && __wt_txn_pinned_stable_timestamp(session) < block_ts);
}

/*
 * __evict_dirty_index_drain --
 *     Drain the per-btree ring(s) into the eviction queue as a fast path alongside the tree walk.
 *     Drains the per-btree ring into the eviction queue. Returns the number of refs queued.
 */
static u_int
__evict_dirty_index_drain(WT_SESSION_IMPL *session, WT_BTREE *btree, WTI_EVICT_QUEUE *queue,
  u_int max_entries, u_int *slotp)
{
    WT_CONNECTION_IMPL *conn;
    WTI_DIRTY_INDEX *idx;
    wt_timestamp_t ts_max, ts_min;
    u_int drained;

    if ((idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL)
        return (0);
    if (*slotp >= max_entries)
        return (0);

    conn = S2C(session);
    if (!__wt_atomic_load_bool_relaxed(&conn->evict->eviction_dirty_index))
        return (0);

    /*
     * The drain produces modified-page candidates, so skip a btree whose modified pages cannot be
     * evicted right now: pages queued from it are blocked by the candidacy filter, burn the
     * per-tree budget the walker needs, and the trim path re-inserts the same refs. Same predicate
     * as the walker's modified-page gate; the drain is implicitly limited to modified pages, so it
     * needs no additional eviction-mode gate.
     */
    switch (__evict_dirty_tree_block(session, btree, __wt_evict_aggressive(session))) {
    case WTI_DIRTY_EVICT_SYNCING:
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_drain_skipped_checkpoint);
        return (0);
    case WTI_DIRTY_EVICT_DISAGG_CKPT:
        WT_STAT_CONN_DSRC_INCR(
          session, cache_eviction_dirty_index_drain_skipped_disagg_checkpointed);
        return (0);
    case WTI_DIRTY_EVICT_OK:
        break;
    }

    ts_min = WT_TS_MAX;
    ts_max = WT_TS_NONE;
    drained = __evict_dirty_index_drain_ring(
      session, btree, idx, queue, max_entries, slotp, &ts_min, &ts_max);

    /*
     * Re-arm the stable-lag gate at the median of the commit timestamps that blocked this pass --
     * approximated by the midpoint of their range, which is O(1) and tracks the median for the
     * roughly uniform commit-timestamp spread under steady load. The walker then parks the drain
     * until the pinned stable timestamp crosses it, by which point about half the ring is evictable
     * again. Clear the gate when nothing was stable-blocked.
     */
    if (ts_min <= ts_max)
        __wt_atomic_store_uint64_relaxed(
          &btree->drain_stable_block_ts, ts_min + (ts_max - ts_min) / 2);
    else
        __wt_atomic_store_uint64_relaxed(&btree->drain_stable_block_ts, WT_TS_NONE);
    return (drained);
}

/*
 * __evict_dirty_index_drain_ring --
 *     Pop refs from one ring into the eviction queue. Each ref is protected by a short-lived hazard
 *     pointer so concurrent teardown cannot free the page while it is examined. Writes into the
 *     queue's slots, advancing *slotp; returns the number of refs queued.
 */
static u_int
__evict_dirty_index_drain_ring(WT_SESSION_IMPL *session, WT_BTREE *btree, WTI_DIRTY_INDEX *idx,
  WTI_EVICT_QUEUE *queue, u_int max_entries, u_int *slotp, wt_timestamp_t *ts_minp,
  wt_timestamp_t *ts_maxp)
{
    WT_DECL_RET;
    WT_REF *ref;
    WTI_DIRTY_INDEX_SLOT *di_slot;
    wt_timestamp_t pinned_stable, ts;
    uint64_t pos, seq;
    uint32_t already_queued, drained, filtered_total, hazard_total, queued_total;
    uint32_t scanned, seen_clean, seen_dirty, seen_updates, slot, stale_total;
    bool busy, precise_ckpt, queued, urgent_queued;

    if (*slotp >= max_entries)
        return (0);

    /*
     * A precise checkpoint cannot evict a dirty page whose newest commit timestamp is ahead of the
     * pinned stable timestamp. Fold such pages' commit timestamps into the caller's running range
     * (the candidacy filter's own predicate) so it can park the drain until stable catches up.
     * Garbage-collecting trees gate on a different (prune) timestamp; leave those to the filter.
     */
    precise_ckpt = F_ISSET(S2C(session), WT_CONN_PRECISE_CHECKPOINT) &&
      !F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT);
    pinned_stable = precise_ckpt ? __wt_txn_pinned_stable_timestamp(session) : WT_TS_NONE;

    drained = filtered_total = hazard_total = queued_total = scanned = stale_total = 0;
    already_queued = seen_clean = seen_dirty = seen_updates = 0;

    /*
     * Hold the split generation across the loop. A concurrent split that frees a ref still in the
     * ring stashes it at a generation >= ours, and the stash cannot reclaim until we leave the
     * generation.
     */
    WT_ENTER_GENERATION(session, WT_GEN_SPLIT);

    pos = __wt_atomic_load_uint64_relaxed(&idx->tail);
    while (*slotp < max_entries) {
        slot = (uint32_t)pos & idx->mask;
        di_slot = &idx->slots[slot];
        seq = __wt_atomic_load_uint64_acquire(&di_slot->sequence);
        if (seq != pos + 1)
            break;
        ref = __wt_atomic_load_ptr_acquire(&di_slot->ref);
        ++scanned;
        if (ref == NULL) {
            __wt_atomic_store_uint64_release(&di_slot->sequence, pos + idx->capacity);
            __wt_atomic_store_uint64_release(&idx->tail, ++pos);
            continue;
        }

        /* Hazard pointer protects the page from concurrent teardown for the rest of the iteration.
         */
        ret = __wt_hazard_set(session, ref, &busy);
        if (ret != 0 || busy) {
            /*
             * Busy (eviction or split in progress). Drop the entry from the ring so a fully drained
             * ring is genuinely empty -- the auto-grow reclaim path relies on tail == head meaning
             * no slot still references a ref. Clear only the slot, not the page back-pointer:
             * without a hazard pointer the page may be mid-eviction and cannot be safely read, and
             * the thread holding it busy clears the back-pointer through clear_page.
             */
            ++hazard_total;
            continue;
        }

        /*
         * The hazard pointer succeeded, so the page is pinned in WT_REF_MEM and ref->page is valid.
         * The only remaining ineligibility is a clean page: the ring holds pages with updates, so
         * if the page lost its modify structure, drop the stale entry and clear the back-pointer,
         * letting the producer re-insert when the page is next dirtied.
         */
        if (ref->page->modify == NULL) {
            (void)__wt_atomic_cas_uint32(&ref->page->dirty_index_slot, slot + 1, 0);
            ++seen_clean;
            ++stale_total;
            goto release;
        }
        ++seen_dirty;
        if (__evict_page_updates_candidate(ref->page))
            ++seen_updates;

        /*
         * Clear the ring slot before the back-pointer. The back-pointer is the gate that suppresses
         * a second insert of the same page, so a producer that observes it zero and re-inserts on
         * the next modify is guaranteed to see the slot already cleared, never a transient
         * double-reference.
         */
        (void)__wt_atomic_cas_uint32(&ref->page->dirty_index_slot, slot + 1, 0);

        /* Already on the eviction queue; the existing entry will drive eviction. */
        if (F_ISSET_ATOMIC_16(ref->page, WT_PAGE_EVICT_LRU) ||
          F_ISSET_ATOMIC_16(ref->page, WT_PAGE_EVICT_LRU_URGENT)) {
            ++already_queued;
            ++stale_total;
            goto release;
        }

        /*
         * Apply the full per-page candidacy filter the walker uses; the drain differs from the
         * walker only in the source of refs. The queue-attempt accounting lives inside it, so both
         * callers stay in step.
         */
        queued = urgent_queued = false;
        __evict_try_queue_page(
          session, queue, ref, NULL, queue->evict_queue + *slotp, &urgent_queued, &queued);

        if (queued) {
            ++(*slotp);
            ++drained;
        }
        if (queued || urgent_queued)
            ++queued_total;
        else {
            ++filtered_total;
            /*
             * Track a page held un-evictable by the stable timestamp under a precise checkpoint:
             * fold its commit timestamp into the running range so the caller can park the drain
             * until stable crosses the median of these timestamps.
             */
            if (precise_ckpt &&
              (ts = __wt_atomic_load_uint64_relaxed(&ref->page->modify->newest_commit_timestamp)) >
                pinned_stable) {
                if (ts < *ts_minp)
                    *ts_minp = ts;
                if (ts > *ts_maxp)
                    *ts_maxp = ts;
            }
            /*
             * The page did not qualify under the current pressure mode (e.g. a page with no tracked
             * update bytes under updates-only pressure). Re-insert at the back of the active ring
             * so it gets another look when the pressure mode shifts. dirty_index_slot is already 0
             * (cleared above); the insert's back-pointer CAS guards against a concurrent producer
             * racing to re-insert the same page. Count only a genuine re-insert (the insert returns
             * false if the ring is now full).
             */
            if (__wt_dirty_index_insert(session, btree, ref))
                WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_drain_reinserted);
        }

release:
        __wt_atomic_store_ptr_release(&di_slot->ref, NULL);
        __wt_atomic_store_uint64_release(&di_slot->sequence, pos + idx->capacity);
        __wt_atomic_store_uint64_release(&idx->tail, ++pos);
        WT_IGNORE_RET(__wt_hazard_clear(session, ref));
    }

    WT_LEAVE_GENERATION(session, WT_GEN_SPLIT);

    /* Flush accumulated per-slot counters once to avoid hammering the shared stat buckets. */
    if (scanned > 0)
        WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_dirty_index_drain_scanned, scanned);
    if (stale_total > 0)
        WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_dirty_index_drain_stale, stale_total);
    if (hazard_total > 0)
        WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_dirty_index_drain_hazard, hazard_total);
    if (queued_total > 0)
        WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_dirty_index_drain_queued, queued_total);
    if (filtered_total > 0)
        WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_dirty_index_drain_filtered, filtered_total);

    /*
     * Credit the shared eviction-discovery stats. The drain examines, classifies, and queues pages
     * for eviction the same way the walker's tree scan does, so these counters reflect total
     * eviction discovery rather than walker-only work. "Pages walked for eviction" (eviction_walk)
     * is the exception: it measures tree traversal, which the drain does not perform -- it pops
     * ring slots -- so the drain's examined-page count belongs in its own drain_scanned counter,
     * not here. Keeping eviction_walk walker-only lets analysis separate walker traversal from
     * drain volume; total discovery is eviction_walk + drain_scanned.
     */
    if (seen_clean + seen_dirty > 0)
        WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen, seen_clean + seen_dirty);
    if (seen_clean > 0)
        WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_clean, seen_clean);
    if (seen_dirty > 0)
        WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_dirty, seen_dirty);
    if (seen_updates > 0)
        WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_updates, seen_updates);
    if (already_queued > 0)
        WT_STAT_CONN_INCRV(session, eviction_pages_already_queued, already_queued);
    if (drained > 0)
        WT_STAT_CONN_INCRV(session, eviction_pages_ordinary_queued, drained);
    return (drained);
}

/*
 * __evict_clear_walk --
 *     Clear a single walk point and remember its position as a soft pointer if clear_pos is unset.
 */
static int
__evict_clear_walk(WT_SESSION_IMPL *session, bool clear_pos)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_REF *ref;
#define PATH_STR_MAX 1024
    char path_str[PATH_STR_MAX];
    const char *where;
    size_t path_str_offset;
    double pos;

    btree = S2BT(session);
    evict = S2C(session)->evict;

    WT_ASSERT(session, FLD_ISSET(session->lock_flags, WT_SESSION_LOCKED_PASS));

    if ((ref = __wt_atomic_load_ptr_relaxed(&btree->evict_ref)) == NULL)
        return (0);

    if (!evict->use_npos_in_pass || clear_pos)
        WT_STAT_CONN_INCR(session, eviction_walks_abandoned);

    /*
     * Clear evict_ref before releasing it in case that forces eviction (we assert that we never try
     * to evict the current eviction walk point).
     */
    __wt_atomic_store_ptr_relaxed(&btree->evict_ref, NULL);

    if (evict->use_npos_in_pass) {
        /* If soft pointers are in use, remember the page's position unless clear_pos is set. */
        if (clear_pos)
            __wt_evict_clear_npos(btree);
        else {
            /*
             * Remember the last position before clearing it so that we can restart from about the
             * same point later. evict_saved_ref_check is used as an opaque page id to compare with
             * it upon restoration for the purpose of stats.
             */
            btree->evict_saved_ref_check = (uint64_t)ref;

            if (F_ISSET(ref, WT_REF_FLAG_LEAF)) {
                /* If we're at a leaf page, use the middle of the page. */
                pos = WT_NPOS_MID;
                where = "MIDDLE";
            } else {
                /*
                 * If we're at an internal page, then we've just finished all its leafs, so get the
                 * position of the very beginning or the very end of it depending on the direction
                 * of walk.
                 */
                if (btree->evict_start_type == WT_EVICT_WALK_NEXT ||
                  btree->evict_start_type == WT_EVICT_WALK_RAND_NEXT) {
                    pos = WT_NPOS_RIGHT;
                    where = "RIGHT";
                } else {
                    pos = WT_NPOS_LEFT;
                    where = "LEFT";
                }
            }
            if (!WT_VERBOSE_LEVEL_ISSET(session, WT_VERB_EVICTION, WT_VERBOSE_DEBUG_1))
                btree->evict_pos = __wt_page_npos(session, ref, pos, NULL, NULL, 0);
            else {
                btree->evict_pos =
                  __wt_page_npos(session, ref, pos, path_str, &path_str_offset, PATH_STR_MAX);
                __wt_verbose_debug1(session, WT_VERB_EVICTION,
                  "Evict walk point memorized at position %lf %s of %s page %s ref %p",
                  btree->evict_pos, where, F_ISSET(ref, WT_REF_FLAG_INTERNAL) ? "INTERNAL" : "LEAF",
                  path_str, (void *)ref);
            }
        }
    }

    WT_WITH_DHANDLE(evict->walk_session, session->dhandle,
      (ret = __wt_page_release(evict->walk_session, ref, WT_READ_NO_EVICT)));
    return (ret);
#undef PATH_STR_MAX
}

/*
 * __wti_evict_clear_all_walks_and_saved_tree --
 *     Clear the eviction walk points for all files a session is waiting on.
 */
int
__wti_evict_clear_all_walks_and_saved_tree(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;

    conn = S2C(session);

    TAILQ_FOREACH (dhandle, &conn->dhqh, q)
        if (WT_DHANDLE_BTREE(dhandle))
            WT_WITH_DHANDLE(session, dhandle, WT_TRET(__evict_clear_walk(session, true)));
    __wti_evict_set_saved_walk_tree(session, NULL);
    return (ret);
}

/*
 * __wti_evict_clear_walk_and_saved_tree_if_current_locked --
 *     Clear single walk points and clear the walk tree if it's the current session's dhandle.
 */
int
__wti_evict_clear_walk_and_saved_tree_if_current_locked(WT_SESSION_IMPL *session)
{
    WT_ASSERT_SPINLOCK_OWNED(session, &S2C(session)->evict->evict_pass_lock);
    if (session->dhandle == S2C(session)->evict->walk_tree)
        __wti_evict_set_saved_walk_tree(session, NULL);
    return (__evict_clear_walk(session, false));
}

/*
 * __evict_entry_priority --
 *     Get the adjusted read generation for an eviction entry.
 */
static WT_INLINE uint64_t
__evict_entry_priority(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_BTREE *btree;
    WT_PAGE *page;
    uint64_t read_gen;

    btree = S2BT(session);
    page = ref->page;

    /* Any page set to the evict_soon or wont_need generation should be discarded. */
    if (__wti_evict_readgen_is_soon_or_wont_need(&page->read_gen))
        return (WT_READGEN_EVICT_SOON);

    /* Any page from a dead tree is a great choice. */
    if (F_ISSET(btree->dhandle, WT_DHANDLE_DEAD))
        return (WT_READGEN_EVICT_SOON);

    /* Any empty page (leaf or internal), is a good choice. */
    if (__wt_page_is_empty(page))
        return (WT_READGEN_EVICT_SOON);

    /* Any large page in memory is likewise a good choice. */
    if (__wt_atomic_load_size_relaxed(&page->memory_footprint) > btree->splitmempage)
        return (WT_READGEN_EVICT_SOON);

    /*
     * The base read-generation is skewed by the eviction priority. Internal pages are also
     * adjusted, we prefer to evict leaf pages.
     */
    if (page->modify != NULL && F_ISSET(S2C(session)->evict, WT_EVICT_CACHE_DIRTY) &&
      !F_ISSET(S2C(session)->evict, WT_EVICT_CACHE_CLEAN))
        read_gen = __wt_atomic_load_uint64_relaxed(&page->modify->update_txn);
    else
        read_gen = __wt_atomic_load_uint64_relaxed(&page->read_gen);

    read_gen += btree->evict_priority;

#define WT_EVICT_INTL_SKEW WT_THOUSAND
    if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
        read_gen += WT_EVICT_INTL_SKEW;

    return (read_gen);
}

/*
 * __evict_walk_choose_dhandle --
 *     Randomly select a dhandle for the next eviction walk
 */
static void
__evict_walk_choose_dhandle(WT_SESSION_IMPL *session, WT_DATA_HANDLE **dhandle_p)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    u_int dh_bucket_count, rnd_bucket, rnd_dh;

    conn = S2C(session);

    WT_ASSERT(session, __wt_rwlock_islocked(session, &conn->dhandle_lock));

#undef RANDOM_DH_SELECTION_ENABLED

#ifdef RANDOM_DH_SELECTION_ENABLED
    *dhandle_p = NULL;

    /*
     * If we don't have many dhandles, most hash buckets will be empty. Just pick a random dhandle
     * from the list in that case.
     */
    if (conn->dhandle_count < conn->dh_hash_size / 4) {
        rnd_dh = __wt_random(&session->rnd_random) % conn->dhandle_count;
        dhandle = TAILQ_FIRST(&conn->dhqh);
        for (; rnd_dh > 0; rnd_dh--)
            dhandle = TAILQ_NEXT(dhandle, q);
        *dhandle_p = dhandle;
        return;
    }

    /*
     * Keep picking up a random bucket until we find one that is not empty.
     */
    do {
        rnd_bucket = __wt_random(&session->rnd_random) & (conn->dh_hash_size - 1);
    } while ((dh_bucket_count = conn->dh_bucket_count[rnd_bucket]) == 0);

    /* We can't pick up an empty bucket with a non zero bucket count. */
    WT_ASSERT(session, !TAILQ_EMPTY(&conn->dhhash[rnd_bucket]));

    /* Pick a random dhandle in the chosen bucket. */
    rnd_dh = __wt_random(&session->rnd_random) % dh_bucket_count;
    dhandle = TAILQ_FIRST(&conn->dhhash[rnd_bucket]);
    for (; rnd_dh > 0; rnd_dh--)
        dhandle = TAILQ_NEXT(dhandle, hashq);
#else
    /* Just step through dhandles. */
    dhandle = *dhandle_p;
    if (dhandle != NULL)
        dhandle = TAILQ_NEXT(dhandle, q);
    if (dhandle == NULL) {
        dhandle = TAILQ_FIRST(&conn->dhqh);
        WT_STAT_CONN_INCR(session, eviction_dhandle_complete_walk);
    }

    WT_UNUSED(dh_bucket_count);
    WT_UNUSED(rnd_bucket);
    WT_UNUSED(rnd_dh);
#endif

    *dhandle_p = dhandle;
}

/*
 * __evict_btree_dominating_cache --
 *     Return if a single btree is occupying at least half of any of our target's cache usage.
 */
static WT_INLINE bool
__evict_btree_dominating_cache(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_CACHE *cache;
    WT_EVICT *evict;
    uint64_t bytes_dirty;
    uint64_t bytes_max;

    cache = S2C(session)->cache;
    evict = S2C(session)->evict;
    bytes_max = S2C(session)->cache_size + 1;

    if (__wt_cache_bytes_plus_overhead(
          cache, __wt_atomic_load_uint64_relaxed(&btree->bytes_inmem)) >
      (uint64_t)(0.5 * evict->eviction_target *
        (bytes_max + __wt_atomic_load_uint64_relaxed(&cache->bytes_shared_dsk_duplicate))) /
        100)
        return (true);

    bytes_dirty = __wt_atomic_load_uint64_relaxed(&btree->bytes_dirty_intl) +
      __wt_atomic_load_uint64_relaxed(&btree->bytes_dirty_leaf);
    if (__wt_cache_bytes_plus_overhead(cache, bytes_dirty) >
      (uint64_t)(0.5 * evict->eviction_dirty_target * bytes_max) / 100)
        return (true);
    if (__wt_cache_bytes_plus_overhead(
          cache, __wt_atomic_load_uint64_relaxed(&btree->bytes_updates)) >
      (uint64_t)(0.5 * evict->eviction_updates_target * bytes_max) / 100)
        return (true);

    return (false);
}

/*
 * __evict_disagg_btree_skip_count --
 *     Count the number of skipped ingest btrees and stable btrees in disagg
 */
static WT_INLINE void
__evict_disagg_btree_skip_count(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    if (__wt_conn_is_disagg(session)) {
        if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT))
            WT_STAT_CONN_INCR(session, eviction_server_skip_ingest_trees);
        else if (F_ISSET(btree, WT_BTREE_DISAGGREGATED))
            WT_STAT_CONN_INCR(session, eviction_server_skip_stable_trees);
    }
}

/*
 * __wti_evict_walk --
 *     Fill in the array by walking the next set of pages.
 */
int
__wti_evict_walk(WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue)
{
    WT_BTREE *btree;
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_TRACK_OP_DECL;
    uint32_t evict_walk_period;
    u_int loop_count, max_entries, retries, slot, start_slot;
    u_int total_candidates;
    bool aggressive, dhandle_list_locked;

    WT_TRACK_OP_INIT(session);

    conn = S2C(session);
    cache = conn->cache;
    evict = conn->evict;
    btree = NULL;
    dhandle = NULL;
    dhandle_list_locked = false;
    retries = 0;

    /*
     * Set the starting slot in the queue and the maximum pages added per walk.
     */
    start_slot = slot = queue->evict_entries;
    max_entries = WT_MIN(slot + WTI_EVICT_WALK_INCR, evict->evict_slots);

    /*
     * Another pathological case: if there are only a tiny number of candidate pages in cache, don't
     * put all of them on one queue.
     */
    total_candidates = (u_int)(F_ISSET(evict, WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_UPDATES) ?
        __wt_cache_pages_inuse(cache) :
        __wt_atomic_load_uint64_relaxed(&cache->pages_dirty_leaf));
    max_entries = WT_MIN(max_entries, 1 + total_candidates / 2);

retry:
    loop_count = 0;
    while (slot < max_entries && loop_count++ < conn->dhandle_count) {
        /* We're done if shutting down or reconfiguring. */
        if (F_ISSET_ATOMIC_32(conn, WT_CONN_CLOSING))
            break;

        /* Eviction server will be suspended if cache pool is reconfiguring. */
        if (F_ISSET_ATOMIC_32(conn, WT_CONN_RECONFIGURING_CACHE_POOL))
            break;

        /*
         * If another thread is waiting on the eviction server to clear the walk point in a tree,
         * give up.
         */
        if (__wt_atomic_load_uint32_v_relaxed(&evict->pass_intr) != 0)
            WT_ERR(EBUSY);

        /*
         * Lock the dhandle list to find the next handle and bump its reference count to keep it
         * alive while we sweep.
         */
        if (!dhandle_list_locked) {
            WT_ERR(__wti_evict_lock_handle_list(session));
            dhandle_list_locked = true;
        }

        if (dhandle == NULL) {
            /*
             * On entry, continue from wherever we got to in the scan last time through. If we don't
             * have a saved handle, pick one randomly from the list.
             */
            if ((dhandle = evict->walk_tree) != NULL)
                __wti_evict_set_saved_walk_tree(session, NULL);
            else
                __evict_walk_choose_dhandle(session, &dhandle);
        } else {
            __wti_evict_set_saved_walk_tree(session, NULL);
            __evict_walk_choose_dhandle(session, &dhandle);
        }

        /* If we couldn't find any dhandle, we're done. */
        if (dhandle == NULL)
            break;

        /* Ignore non-btree handles, or handles that aren't open. */
        if (!WT_DHANDLE_BTREE(dhandle) || !F_ISSET(dhandle, WT_DHANDLE_OPEN))
            continue;

        /* Skip files that don't allow eviction. */
        btree = dhandle->handle;
        if (btree->evict_disabled > 0) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_trees_eviction_disabled);
            __evict_disagg_btree_skip_count(session, btree);
            continue;
        }

        /* Skip read-only btrees if we are not looking for clean/updates pages. */
        if (F_ISSET(btree, WT_BTREE_READONLY) &&
          !F_ISSET(evict, WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_UPDATES)) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_trees_read_only);
            __evict_disagg_btree_skip_count(session, btree);
            continue;
        }

        /*
         * When looking only for dirty pages, skip a tree whose dirty pages cannot be evicted right
         * now: one being checkpointed, or a disaggregated tree already visited by the ongoing
         * checkpoint (its modified pages belong to the next checkpoint and would fail the post-lock
         * recheck, so walking only inflates the worker failure rate). When looking for clean or
         * update pages, or when eviction is aggressive, walk anyway: any candidates the workers can
         * lay hands on are better than starving the cache. The drain shares this predicate.
         */
        aggressive = __wt_evict_aggressive(session);
        if (!F_ISSET(evict, WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_UPDATES)) {
            switch (__evict_dirty_tree_block(session, btree, aggressive)) {
            case WTI_DIRTY_EVICT_SYNCING:
                WT_STAT_CONN_INCR(session, eviction_server_skip_checkpointing_trees);
                __evict_disagg_btree_skip_count(session, btree);
                continue;
            case WTI_DIRTY_EVICT_DISAGG_CKPT:
                WT_STAT_CONN_INCR(session, eviction_server_skip_disagg_trees_checkpointed);
                __evict_disagg_btree_skip_count(session, btree);
                continue;
            case WTI_DIRTY_EVICT_OK:
                break;
            }
        }

        /*
         * Skip files that are configured to stick in cache until we become aggressive.
         *
         * If the file is contributing heavily to our cache usage then ignore the "stickiness" of
         * its pages.
         */
        if (btree->evict_priority != 0 && !aggressive &&
          !__evict_btree_dominating_cache(session, btree)) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_trees_stick_in_cache);
            __evict_disagg_btree_skip_count(session, btree);
            continue;
        }

        if (!evict->use_npos_in_pass) {
            /*
             * Skip files if we have too many active walks.
             *
             * This used to be limited by the configured maximum number of hazard pointers per
             * session. Even though that ceiling has been removed, we need to test eviction with
             * huge numbers of active trees before allowing larger numbers of hazard pointers in the
             * walk session.
             */
            if (__wt_atomic_load_ptr_relaxed(&btree->evict_ref) == NULL &&
              session->hazards.num_active > WTI_EVICT_MAX_TREES) {
                WT_STAT_CONN_INCR(session, eviction_server_skip_trees_too_many_active_walks);
                __evict_disagg_btree_skip_count(session, btree);
                continue;
            }
        }

        /*
         * If the cache walk flags have changed since the prior eviction pass on this tree then
         * reset the walk effectiveness tracking. Imagine a case where only dirty content has been
         * looked for and this tree doesn't have much dirty content. Then eviction starts looking
         * for clean content - this tree might be a cornucopia of good clean candidate pages.
         * Specific for disaggregated connections, where we are using WT_EVICT_MODIFY_COUNT_MIN and
         * WT_DIRTY_PAGE_LOW_PRESSURE_THRESHOLD values to change the priority for this heuristic.
         */
        if (__wt_conn_is_disagg(session) && btree->last_evict_walk_flags != evict->flags) {
            __wt_atomic_store_uint32_relaxed(&btree->evict_walk_period, 0);
            btree->last_evict_walk_flags = evict->flags;
        }

        /*
         * If we are filling the queue, skip files that haven't been useful in the past.
         */
        evict_walk_period = __wt_atomic_load_uint32_relaxed(&btree->evict_walk_period);
        if (evict_walk_period != 0 && btree->evict_walk_skips++ < evict_walk_period) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_trees_not_useful_before);
            __evict_disagg_btree_skip_count(session, btree);
            continue;
        }

        /*
         * For in-memory btrees, if we are not evicting dirty pages or pages with active updates,
         * walking them serves no purpose. Such pages are not eligible for clean eviction, making
         * the operation unnecessary.
         */
        if (__wt_btree_stays_in_memory(btree) &&
          !F_ISSET(evict, WT_EVICT_CACHE_DIRTY | WT_EVICT_CACHE_UPDATES)) {
            __evict_disagg_btree_skip_count(session, btree);
            continue;
        }

        btree->evict_walk_skips = 0;

        __wti_evict_set_saved_walk_tree(session, dhandle);
        __wt_readunlock(session, &conn->dhandle_lock);
        dhandle_list_locked = false;

        /*
         * Re-check the "no eviction" flag, used to enforce exclusive access when a handle is being
         * closed.
         *
         * Only try to acquire the lock and simply continue if we fail; the lock is held while the
         * thread turning off eviction clears the tree's current eviction point, and part of the
         * process is waiting on this thread to acknowledge that action.
         *
         * If a handle is being discarded, it will still be marked open, but won't have a root page.
         */
        if (btree->evict_disabled == 0 && !__wt_spin_trylock(session, &evict->evict_walk_lock)) {
            if (btree->evict_disabled == 0 && btree->root.page != NULL) {
                WT_WITH_DHANDLE(
                  session, dhandle, ret = __evict_walk_tree(session, queue, max_entries, &slot));

                WT_ASSERT(session, __wt_session_gen(session, WT_GEN_SPLIT) == 0);
            }
            __wt_spin_unlock(session, &evict->evict_walk_lock);
            WT_ERR(ret);
            /*
             * If there is a checkpoint thread gathering handles, which means it is holding the
             * schema lock, then there is often contention on the evict walk lock with that thread.
             * If eviction is not in aggressive mode, sleep a bit to give the checkpoint thread a
             * chance to gather its handles.
             */
            if (F_ISSET_ATOMIC_32(conn, WT_CONN_CKPT_GATHER) && !aggressive) {
                __wt_sleep(0, 10);
                WT_STAT_CONN_INCR(session, eviction_walk_sleeps);
            }
        }
    }

    /*
     * Repeat the walks a few times if we don't find enough pages. Give up when we have some
     * candidates and we aren't finding more.
     */
    if (slot < max_entries &&
      (retries < 2 ||
        (retries < WT_RETRY_MAX && (slot == queue->evict_entries || slot > start_slot)))) {
        start_slot = slot;
        ++retries;
        goto retry;
    }

err:
    if (dhandle_list_locked)
        __wt_readunlock(session, &conn->dhandle_lock);

    /*
     * If we didn't find any entries on a walk when we weren't interrupted, let our caller know.
     */
    if (queue->evict_entries == slot && __wt_atomic_load_uint32_v_relaxed(&evict->pass_intr) == 0)
        ret = WT_NOTFOUND;

    queue->evict_entries = slot;
    WT_TRACK_OP_END(session);
    return (ret);
}

/*
 * __wti_evict_push_candidate --
 *     Initialize a WTI_EVICT_ENTRY structure with a given page.
 */
bool
__wti_evict_push_candidate(
  WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue, WTI_EVICT_ENTRY *evict_entry, WT_REF *ref)
{
    uint16_t new_flags, orig_flags;
    u_int slot;

    /*
     * Threads can race to queue a page (e.g., an ordinary LRU walk can race with a page being
     * queued for urgent eviction).
     */
    orig_flags = new_flags = ref->page->flags_atomic;
    FLD_SET(new_flags, WT_PAGE_EVICT_LRU);
    if (orig_flags == new_flags ||
      !__wt_atomic_cas_uint16(&ref->page->flags_atomic, orig_flags, new_flags)) {
        WT_STAT_CONN_INCR(session, eviction_server_push_pages_failed_when_flaging);
        return (false);
    }

    /* Keep track of the maximum slot we are using. */
    slot = (u_int)(evict_entry - queue->evict_queue);
    if (slot >= queue->evict_max)
        queue->evict_max = slot + 1;

    if (evict_entry->ref != NULL)
        __evict_list_clear(session, evict_entry);

    evict_entry->btree = S2BT(session);
    evict_entry->ref = ref;
    evict_entry->score = __evict_entry_priority(session, ref);

    /* Adjust for size when doing dirty eviction. */
    if (F_ISSET(S2C(session)->evict, WT_EVICT_CACHE_DIRTY) &&
      evict_entry->score != WT_READGEN_EVICT_SOON && evict_entry->score != UINT64_MAX &&
      !__wt_page_is_modified(ref->page))
        evict_entry->score += WT_MEGABYTE -
          WT_MIN(WT_MEGABYTE, __wt_atomic_load_size_relaxed(&ref->page->memory_footprint));

    return (true);
}

/*
 * __evict_walk_target --
 *     Calculate how many pages to queue for a given tree.
 */
static uint32_t
__evict_walk_target(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_EVICT *evict;
    uint64_t btree_clean_inuse, btree_dirty_inuse, btree_updates_inuse, bytes_per_slot, cache_inuse;
    uint32_t target_pages, target_pages_clean, target_pages_dirty, target_pages_updates;
    bool want_tree;

    cache = S2C(session)->cache;
    evict = S2C(session)->evict;
    btree_clean_inuse = btree_dirty_inuse = btree_updates_inuse = 0;
    target_pages_clean = target_pages_dirty = target_pages_updates = 0;

/*
 * The minimum number of pages we should consider per tree.
 */
#define MIN_PAGES_PER_TREE 10

    /*
     * The target number of pages for this tree is proportional to the space it is taking up in
     * cache. Round to the nearest number of slots so we assign all of the slots to a tree filling
     * 99+% of the cache (and only have to walk it once).
     */
    if (F_ISSET(evict, WT_EVICT_CACHE_CLEAN)) {
        btree_clean_inuse = __wt_btree_bytes_evictable(session);
        cache_inuse = __wt_cache_bytes_inuse(cache) +
          __wt_cache_bytes_plus_overhead(
            cache, __wt_atomic_load_uint64_relaxed(&cache->bytes_shared_dsk_duplicate));
        bytes_per_slot = 1 + cache_inuse / evict->evict_slots;
        target_pages_clean = (uint32_t)((btree_clean_inuse + bytes_per_slot / 2) / bytes_per_slot);
    }

    if (F_ISSET(evict, WT_EVICT_CACHE_DIRTY)) {
        btree_dirty_inuse = __wt_btree_dirty_leaf_inuse(session);
        cache_inuse = __wt_cache_dirty_leaf_inuse(cache);
        bytes_per_slot = 1 + cache_inuse / evict->evict_slots;
        target_pages_dirty = (uint32_t)((btree_dirty_inuse + bytes_per_slot / 2) / bytes_per_slot);
    }

    if (F_ISSET(evict, WT_EVICT_CACHE_UPDATES)) {
        btree_updates_inuse = __wt_btree_bytes_updates(session);
        cache_inuse = __wt_cache_bytes_updates(cache);
        bytes_per_slot = 1 + cache_inuse / evict->evict_slots;
        target_pages_updates =
          (uint32_t)((btree_updates_inuse + bytes_per_slot / 2) / bytes_per_slot);
    }

    target_pages = WT_MAX(target_pages_clean, target_pages_dirty);
    target_pages = WT_MAX(target_pages, target_pages_updates);

    /*
     * Walk trees with a small fraction of the cache in case there are so many trees that none of
     * them use enough of the cache to be allocated slots. Only skip a tree if it has no bytes of
     * interest.
     */
    if (target_pages == 0) {
        want_tree = (F_ISSET(evict, WT_EVICT_CACHE_CLEAN) && (btree_clean_inuse > 0)) ||
          (F_ISSET(evict, WT_EVICT_CACHE_DIRTY) && (btree_dirty_inuse > 0)) ||
          (F_ISSET(evict, WT_EVICT_CACHE_UPDATES) && (btree_updates_inuse > 0));

        if (!want_tree) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_unwanted_tree);
            return (0);
        }
    }

    /*
     * There is some cost associated with walking a tree. If we're going to visit this tree, always
     * look for a minimum number of pages.
     */
    if (target_pages < MIN_PAGES_PER_TREE)
        target_pages = MIN_PAGES_PER_TREE;

    /* If the tree is dead, take a lot of pages. */
    if (F_ISSET(session->dhandle, WT_DHANDLE_DEAD))
        target_pages *= 10;

    return (target_pages);
}

/*
 * __evict_skip_dirty_candidate --
 *     Check if eviction should skip the dirty page.
 */
static WT_INLINE bool
__evict_skip_dirty_candidate(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_CONNECTION_IMPL *conn;
    WT_TXN *txn;

    conn = S2C(session);
    txn = session->txn;

    /*
     * If the global transaction state hasn't changed since the last time we tried eviction, it's
     * unlikely we can make progress. This heuristic avoids repeated attempts to evict the same
     * page.
     */
    if (!__wt_page_evict_retry(session, page)) {
        WT_STAT_CONN_INCR(session, eviction_server_skip_pages_retry);
        return (true);
    }

    /*
     * If we are under cache pressure, allow evicting pages with newly committed updates to free
     * space. Otherwise, avoid doing that as it may thrash the cache.
     */
    if (F_ISSET(conn->evict, WT_EVICT_CACHE_DIRTY_HARD | WT_EVICT_CACHE_UPDATES_HARD) &&
      F_ISSET(txn, WT_TXN_HAS_SNAPSHOT)) {
        if (!__wt_txn_visible_id(
              session, __wt_atomic_load_uint64_relaxed(&page->modify->update_txn)))
            return (true);
    } else if (__wt_atomic_load_uint64_relaxed(&page->modify->update_txn) >=
      __wt_atomic_load_uint64_v_relaxed(&conn->txn_global.last_running)) {
        WT_STAT_CONN_INCR(session, eviction_server_skip_pages_last_running);
        return (true);
    } else if (F_ISSET(conn, WT_CONN_PRECISE_CHECKPOINT)) {
        WT_BTREE *btree = S2BT(session);
        wt_timestamp_t newest_commit_timestamp =
          __wt_atomic_load_uint64_relaxed(&page->modify->newest_commit_timestamp);
        if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT)) {
            wt_timestamp_t prune_timestamp =
              __wt_atomic_load_uint64_relaxed(&btree->prune_timestamp);
            if (prune_timestamp != WT_TS_NONE) {
                if (newest_commit_timestamp > prune_timestamp) {
                    WT_STAT_CONN_INCR(session, eviction_server_skip_pages_prune_timestamp);
                    return (true);
                }
                if (page->modify->rec_prune_timestamp >= prune_timestamp) {
                    WT_STAT_CONN_INCR(session, eviction_server_skip_pages_prune_timestamp_not_move);
                    return (true);
                }
            }
        } else {
            if (newest_commit_timestamp > __wt_txn_pinned_stable_timestamp(session)) {
                WT_STAT_CONN_INCR(session, eviction_server_skip_pages_checkpoint_timestamp);
                return (true);
            }
        }
    }

    /*
     * For pages that are getting random updates (often index pages), try not to reconcile them too
     * often. It makes better use of I/O if they accumulate more changes between reconciliations
     */
#define WT_EVICT_MODIFY_COUNT_MIN 15 /* Number of modifications since the prior reconciliation */
    /*
     * If the cache is dirty, but not under pressure skip pages with just a few modifications
     * hopefully they can accumulate more changes before being reconciled. The cache has low
     * pressure if cache usage is less than 90% of the eviction dirty trigger threshold. Currently
     * only for disaggregated storage.
     */
#define WT_DIRTY_PAGE_LOW_PRESSURE_THRESHOLD \
    0.9 /* Cache usage below 90% of the eviction trigger threshold is considered low pressure */
    if (__wt_conn_is_disagg(session) &&
      __wt_atomic_load_uint32_relaxed(&page->modify->page_state) < WT_EVICT_MODIFY_COUNT_MIN) {
        double pct_dirty = 0.0, pct_updates = 0.0;
        bool high_pressure = false;

        if (F_ISSET(conn->evict, WT_EVICT_CACHE_DIRTY)) {
            WT_IGNORE_RET(__wt_evict_dirty_needed(session, &pct_dirty));
            high_pressure = (pct_dirty >
              (conn->evict->eviction_dirty_trigger * WT_DIRTY_PAGE_LOW_PRESSURE_THRESHOLD));
        }

        if (!high_pressure && F_ISSET(conn->evict, WT_EVICT_CACHE_UPDATES)) {
            WT_IGNORE_RET(__wti_evict_updates_needed(session, &pct_updates));
            high_pressure = (pct_updates >
              (__wt_atomic_load_double_relaxed(&conn->evict->eviction_updates_trigger) *
                WT_DIRTY_PAGE_LOW_PRESSURE_THRESHOLD));
        }

        if (!high_pressure)
            return (true);
    }
    return (false);
}

/*
 * __evict_get_target_pages --
 *     Calculate the target pages to add to the queue.
 */
static WT_INLINE uint32_t
__evict_get_target_pages(WT_SESSION_IMPL *session, u_int max_entries, uint32_t slot)
{
    WT_BTREE *btree;
    uint32_t remaining_slots, target_pages;

    btree = S2BT(session);

    /*
     * Figure out how many slots to fill from this tree. Note that some care is taken in the
     * calculation to avoid overflow.
     */
    remaining_slots = max_entries - slot;

    /*
     * For this handle, calculate the number of target pages to evict. If the number of target pages
     * is zero, then simply return early from this function.
     *
     * If the progress has not met the previous target, continue using the previous target.
     */
    target_pages = __evict_walk_target(session);

    if ((target_pages == 0) || btree->evict_walk_progress >= btree->evict_walk_target) {
        btree->evict_walk_target = target_pages;
        btree->evict_walk_progress = 0;
    }
    target_pages = btree->evict_walk_target - btree->evict_walk_progress;

    if (target_pages > remaining_slots)
        target_pages = remaining_slots;

    /*
     * Reduce the number of pages to be selected from btrees other than the history store (HS) if
     * the cache pressure is high and HS content dominates the cache. Evicting unclean non-HS pages
     * can generate even more HS content and will not help with the cache pressure, and will
     * probably just amplify it further.
     */
    if (!WT_IS_HS(btree->dhandle) && __wti_evict_hs_dirty(session)) {
        /* If target pages are less than 10, keep it like that. */
        if (target_pages >= 10) {
            target_pages = target_pages / 10;
            WT_STAT_CONN_DSRC_INCR(session, cache_eviction_target_page_reduced);
        }
    }

    if (target_pages != 0) {
        /*
         * These statistics generate a histogram of the number of pages targeted for eviction each
         * round. The range of values here start at MIN_PAGES_PER_TREE as this is the smallest
         * number of pages we can target, unless there are fewer slots available. The aim is to
         * cover the likely ranges of target pages in as few statistics as possible to reduce the
         * overall overhead.
         */
        if (target_pages < MIN_PAGES_PER_TREE) {
            WT_STAT_CONN_INCR(session, cache_eviction_target_page_lt10);
            WT_STAT_DSRC_INCR(session, cache_eviction_target_page_lt10);
        } else if (target_pages < 32) {
            WT_STAT_CONN_INCR(session, cache_eviction_target_page_lt32);
            WT_STAT_DSRC_INCR(session, cache_eviction_target_page_lt32);
        } else if (target_pages < 64) {
            WT_STAT_CONN_INCR(session, cache_eviction_target_page_lt64);
            WT_STAT_DSRC_INCR(session, cache_eviction_target_page_lt64);
        } else if (target_pages < 128) {
            WT_STAT_CONN_INCR(session, cache_eviction_target_page_lt128);
            WT_STAT_DSRC_INCR(session, cache_eviction_target_page_lt128);
        } else {
            WT_STAT_CONN_INCR(session, cache_eviction_target_page_ge128);
            WT_STAT_DSRC_INCR(session, cache_eviction_target_page_ge128);
        }
    }

    return (target_pages);
}

/*
 * __evict_get_min_pages --
 *     Calculate the minimum pages to visit.
 */
static WT_INLINE uint64_t
__evict_get_min_pages(WT_SESSION_IMPL *session, uint32_t target_pages)
{
    WT_EVICT *evict;
    uint64_t min_pages;

    evict = S2C(session)->evict;

    /*
     * Examine at least a reasonable number of pages before deciding whether to give up. When we are
     * not looking for clean pages, search the tree for longer.
     */
    min_pages = 10 * (uint64_t)target_pages;
    if (F_ISSET(evict, WT_EVICT_CACHE_CLEAN))
        WT_STAT_CONN_INCR(session, eviction_target_strategy_clean);
    else
        min_pages *= 10;
    if (F_ISSET(evict, WT_EVICT_CACHE_UPDATES))
        WT_STAT_CONN_INCR(session, eviction_target_strategy_updates);
    if (F_ISSET(evict, WT_EVICT_CACHE_DIRTY))
        WT_STAT_CONN_INCR(session, eviction_target_strategy_dirty);

    return (min_pages);
}

/*
 * __evict_try_restore_walk_position --
 *     Try to restore the eviction walk position from saved soft pos. If we can't restore a saved
 *     position, clear the eviction walk position instead.
 */
static WT_INLINE int
__evict_try_restore_walk_position(WT_SESSION_IMPL *session, WT_BTREE *btree, uint32_t walk_flags)
{
#define PATH_STR_MAX 1024
    WT_REF *evict_ref;
    char path_str[PATH_STR_MAX];
    size_t path_str_offset;
    double unused; /* GCC fails to WT_UNUSED() :( */

    if ((evict_ref = __wt_atomic_load_ptr_relaxed(&btree->evict_ref)) != NULL)
        return (0); /* We've got a pointer already */
    if (WT_NPOS_IS_INVALID(btree->evict_pos))
        return (0); /* No restore point */
    WT_RET_ONLY(
      __wt_page_from_npos_for_eviction(session, &evict_ref, btree->evict_pos, 0, walk_flags),
      WT_PANIC);
    __wt_atomic_store_ptr_relaxed(&btree->evict_ref, evict_ref);

    if (evict_ref != NULL &&
      WT_VERBOSE_LEVEL_ISSET(session, WT_VERB_EVICTION, WT_VERBOSE_DEBUG_1)) {
        WT_UNUSED(unused = __wt_page_npos(
                    session, evict_ref, WT_NPOS_MID, path_str, &path_str_offset, PATH_STR_MAX));
        __wt_verbose_debug1(session, WT_VERB_EVICTION,
          "Evict walk point recalled from position %lf %s page %s ref %p", btree->evict_pos,
          F_ISSET(evict_ref, WT_REF_FLAG_INTERNAL) ? "INTERNAL" : "LEAF", path_str,
          (void *)evict_ref);
    }

    WT_STAT_CONN_INCR(session, eviction_restored_pos);
    if (btree->evict_saved_ref_check != 0 && btree->evict_saved_ref_check != (uint64_t)evict_ref)
        WT_STAT_CONN_INCR(session, eviction_restored_pos_differ);

    return (0);
#undef PATH_STR_MAX
}

/*
 * __evict_walk_prepare --
 *     Choose the walk direction and descend to the initial walk point.
 */
static WT_INLINE int
__evict_walk_prepare(WT_SESSION_IMPL *session, uint32_t *walk_flagsp)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_REF *evict_ref;

    btree = S2BT(session);

    *walk_flagsp = WT_READ_EVICT_WALK_FLAGS;
    if (!F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT))
        FLD_SET(*walk_flagsp, WT_READ_VISIBLE_ALL);

    WT_RET(__evict_try_restore_walk_position(session, btree, *walk_flagsp));

    evict_ref = __wt_atomic_load_ptr_relaxed(&btree->evict_ref);
    if (evict_ref != NULL)
        WT_STAT_CONN_INCR(session, eviction_walk_saved_pos);
    else
        WT_STAT_CONN_INCR(session, eviction_walk_from_root);

    /*
     * Choose a random point in the tree if looking for candidates in a tree with no starting point
     * set. This is mostly aimed at ensuring eviction fairly visits all pages in trees with a lot of
     * in-cache content.
     */
    switch (btree->evict_start_type) {
    case WT_EVICT_WALK_NEXT:
        /* Each time when evict_ref is null, alternate between linear and random walk */
        if (!S2C(session)->evict_config.legacy_page_visit_strategy && evict_ref == NULL &&
          (++btree->linear_walk_restarts) & 1) {
            if (S2C(session)->evict->use_npos_in_pass)
                /* Alternate with rand_prev so that the start of the tree is visited more often */
                goto rand_prev;
            else
                goto rand_next;
        }
        break;
    case WT_EVICT_WALK_PREV:
        /* Each time when evict_ref is null, alternate between linear and random walk */
        if (!S2C(session)->evict_config.legacy_page_visit_strategy && evict_ref == NULL &&
          (++btree->linear_walk_restarts) & 1) {
            if (S2C(session)->evict->use_npos_in_pass)
                /* Alternate with rand_next so that the end of the tree is visited more often */
                goto rand_next;
            else
                goto rand_prev;
        }
        FLD_SET(*walk_flagsp, WT_READ_PREV);
        break;
    case WT_EVICT_WALK_RAND_PREV:
rand_prev:
        FLD_SET(*walk_flagsp, WT_READ_PREV);
    /* FALLTHROUGH */
    case WT_EVICT_WALK_RAND_NEXT:
rand_next:
        if (evict_ref == NULL) {
            for (;;) {
                /* Ensure internal pages indexes remain valid */
                WT_WITH_PAGE_INDEX(session,
                  ret = __wt_random_descent(
                    session, &evict_ref, WT_READ_EVICT_READ_FLAGS, &session->rnd_random));
                if (ret != WT_RESTART)
                    break;
                WT_STAT_CONN_INCR(session, eviction_walk_restart);
            }
            WT_RET_NOTFOUND_OK(ret);
            __wt_atomic_store_ptr_relaxed(&btree->evict_ref, evict_ref);

            if (evict_ref == NULL)
                WT_STAT_CONN_INCR(session, eviction_walk_random_returns_null_position);
        }
        break;
    }

    return (ret);
}

/*
 * __evict_should_give_up_walk --
 *     Check if we should give up on the current walk.
 */
static WT_INLINE bool
__evict_should_give_up_walk(WT_SESSION_IMPL *session, uint64_t pages_seen, uint64_t pages_queued,
  uint64_t min_pages, uint32_t target_pages)
{
    WT_BTREE *btree;
    bool give_up;

    btree = S2BT(session);

    /*
     * Check whether we're finding a good ratio of candidates vs pages seen. Some workloads create
     * "deserts" in trees where no good eviction candidates can be found. Abandon the walk if we get
     * into that situation.
     */
    give_up = !__wt_evict_aggressive(session) && !WT_IS_HS(btree->dhandle) &&
      pages_seen > min_pages &&
      (pages_queued == 0 || (pages_seen / pages_queued) > (min_pages / target_pages));
    if (give_up) {
        /*
         * Try a different walk start point next time if a walk gave up.
         */
        switch (btree->evict_start_type) {
        case WT_EVICT_WALK_NEXT:
            btree->evict_start_type = WT_EVICT_WALK_PREV;
            break;
        case WT_EVICT_WALK_PREV:
            btree->evict_start_type = WT_EVICT_WALK_RAND_PREV;
            break;
        case WT_EVICT_WALK_RAND_PREV:
            btree->evict_start_type = WT_EVICT_WALK_RAND_NEXT;
            break;
        case WT_EVICT_WALK_RAND_NEXT:
            btree->evict_start_type = WT_EVICT_WALK_NEXT;
            break;
        }

        /*
         * We differentiate the reasons we gave up on this walk and increment the stats accordingly.
         */
        if (pages_queued == 0)
            WT_STAT_CONN_INCR(session, eviction_walks_gave_up_no_targets);
        else
            WT_STAT_CONN_INCR(session, eviction_walks_gave_up_ratio);
    }

    return (give_up);
}

/*
 * __evict_try_queue_page --
 *     Check if we should queue the page for eviction. Queue it to the urgent queue or the regular
 *     queue.
 */
static WT_INLINE void
__evict_try_queue_page(WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue, WT_REF *ref,
  WT_PAGE *last_parent, WTI_EVICT_ENTRY *evict_entry, bool *urgent_queuedp, bool *queuedp)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    WT_PAGE *page;
    bool evict_clean, evict_dirty, evict_updates, modified, should_evict_page;

    btree = S2BT(session);
    conn = S2C(session);
    evict = conn->evict;
    page = ref->page;
    modified = __wt_page_is_modified(page);
    *queuedp = false;

    /* Record this queue attempt -- every attempt, queued or filtered, for both callers. */
    ++page->evict_queue_attempts;
    __wt_atomic_stats_max_uint16(
      &evict->evict_max_eviction_queue_attempts, page->evict_queue_attempts);

    /* Don't queue dirty pages in trees during checkpoints. */
    if (modified && WT_BTREE_SYNCING(btree)) {
        WT_STAT_CONN_INCR(session, eviction_server_skip_dirty_pages_during_checkpoint);
        return;
    }

    /*
     * It's possible (but unlikely) to visit a page without a read generation, if we race with the
     * read instantiating the page. Set the page's read generation here to ensure a bug doesn't
     * somehow leave a page without a read generation.
     */
    if (__wt_atomic_load_uint64_relaxed(&page->read_gen) == WT_READGEN_NOTSET)
        __wti_evict_read_gen_new(session, page);

    /*
     * Don't queue clean history store pages for updates eviction targets while a precise checkpoint
     * is running, it tends to evict history pages that are needed soon.
     */
    if (WT_IS_HS(btree->dhandle) && F_ISSET(conn, WT_CONN_PRECISE_CHECKPOINT)) {
        if (F_ISSET(evict, WT_EVICT_CACHE_UPDATES) &&
          !F_ISSET(evict, WT_EVICT_CACHE_UPDATES_HARD) && !F_ISSET(evict, WT_EVICT_CACHE_CLEAN) &&
          !modified && __wt_atomic_load_bool_v_relaxed(&conn->txn_global.checkpoint_running)) {
            WT_STAT_CONN_INCR(
              session, eviction_server_skip_history_store_pages_with_updates_during_checkpoint);
            return;
        }
    }

    /* Pages being forcibly evicted go on the urgent queue. */
    if (modified &&
      (__wt_atomic_load_uint64_relaxed(&page->read_gen) == WT_READGEN_EVICT_SOON ||
        __wt_atomic_load_size_relaxed(&page->memory_footprint) >= btree->splitmempage)) {
        WT_STAT_CONN_INCR(session, eviction_pages_queued_oldest);
        if (__wt_evict_page_urgent(session, ref))
            *urgent_queuedp = true;
        return;
    }

    /*
     * If history store dirty content is dominating the cache, we want to prioritize evicting
     * history store pages over other btree pages. This helps in keeping cache contents below the
     * configured cache size during checkpoints where reconciling non-HS pages can generate a
     * significant amount of HS dirty content very quickly.
     */
    if (WT_IS_HS(btree->dhandle) && __wti_evict_hs_dirty(session)) {
        WT_STAT_CONN_INCR(session, eviction_pages_queued_urgent_hs_dirty);
        if (__wt_evict_page_urgent(session, ref))
            *urgent_queuedp = true;
        return;
    }

    /* Pages that are empty or from dead trees are fast-tracked. */
    if (__wt_page_is_empty(page) || F_ISSET(session->dhandle, WT_DHANDLE_DEAD))
        goto fast;

    evict_clean =
      F_ISSET(evict, WT_EVICT_CACHE_CLEAN) && !__wt_btree_stays_in_memory(btree) && !modified;
    evict_dirty = F_ISSET(evict, WT_EVICT_CACHE_DIRTY) && modified;
    evict_updates = F_ISSET(evict, WT_EVICT_CACHE_UPDATES) && __evict_page_updates_candidate(page);
    should_evict_page = evict_clean || evict_dirty || evict_updates;
    /* Skip pages we don't want. */
    if (!should_evict_page) {
        WT_STAT_CONN_INCR(session, eviction_server_skip_unwanted_pages);
        return;
    }

    /*
     * Do not evict a clean metadata page that contains historical data needed to satisfy a reader.
     * Since there is no history store for metadata, we won't be able to serve an older reader if we
     * evict this page.
     */
    if (WT_IS_METADATA(session->dhandle) && F_ISSET(evict, WT_EVICT_CACHE_CLEAN_HARD) &&
      F_ISSET(ref, WT_REF_FLAG_LEAF) && !modified && page->modify != NULL &&
      !__wt_txn_visible_all(session, page->modify->rec_max_txn, page->modify->rec_max_timestamp)) {
        WT_STAT_CONN_INCR(session, eviction_server_skip_metatdata_with_history);
        return;
    }

    /*
     * Don't attempt eviction of internal pages with children in cache (indicated by seeing an
     * internal page that is the parent of the last page we saw).
     *
     * Also skip internal page unless we get aggressive, the tree is idle (indicated by the tree
     * being skipped for walks), or we are in eviction debug mode. The goal here is that if trees
     * become completely idle, we eventually push them out of cache completely.
     */
    if (!FLD_ISSET(conn->debug.flags, WT_CONN_DEBUG_EVICT_AGGRESSIVE_MODE) &&
      F_ISSET(ref, WT_REF_FLAG_INTERNAL)) {
        if (page == last_parent) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_intl_page_with_active_child);
            return;
        }
        if (__wt_atomic_load_uint32_relaxed(&btree->evict_walk_period) == 0 &&
          !__wt_evict_aggressive(session)) {
            WT_STAT_CONN_INCR(session, eviction_server_skip_intl_page_non_aggressive);
            return;
        }
    }

    /* Evaluate dirty page candidacy, when eviction is not aggressive. */
    if (!__wt_evict_aggressive(session) && modified && __evict_skip_dirty_candidate(session, page))
        return;

fast:
    /* If the page can't be evicted, give up. */
    if (!__wt_page_can_evict(session, ref, NULL))
        return;

    WT_ASSERT(session, evict_entry->ref == NULL);
    if (!__wti_evict_push_candidate(session, queue, evict_entry, ref))
        return;

    *queuedp = true;
    __wt_verbose_debug2(session, WT_VERB_EVICTION, "walk select: %p, size %" WT_SIZET_FMT,
      (void *)page, __wt_atomic_load_size_relaxed(&page->memory_footprint));

    return;
}

/*
 * __evict_walk_tree --
 *     Get a few page eviction candidates from a single underlying file.
 */
static int
__evict_walk_tree(WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue, u_int max_entries, u_int *slotp)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WTI_EVICT_ENTRY *end, *evict_entry, *start;
    WT_PAGE *last_parent, *page;
    WT_REF *ref;
    WT_TXN *txn;
    uint64_t internal_pages_already_queued, internal_pages_queued, internal_pages_seen;
    uint64_t min_pages, pages_already_queued, pages_queued, pages_seen, refs_walked;
    uint64_t pages_seen_clean, pages_seen_dirty, pages_seen_updates;
    uint64_t pass_gen, root_pages_skipped;
    uint32_t drain_queued, evict_walk_period, target_pages, walk_flags;
    int restarts;
    bool give_up, queued, should_drain, urgent_queued;

    conn = S2C(session);
    btree = S2BT(session);
    evict = conn->evict;
    drain_queued = 0;
    last_parent = NULL;
    restarts = 0;
    give_up = urgent_queued = false;
    txn = session->txn;

    WT_ASSERT_SPINLOCK_OWNED(session, &evict->evict_walk_lock);

    target_pages = __evict_get_target_pages(session, max_entries, *slotp);

    /* If we don't want any pages from this tree, move on. */
    if (target_pages == 0)
        return (0);

    /* Per-btree slot budget; drain fills the leading portion, walker fills the remainder. */
    start = queue->evict_queue + *slotp;
    end = start + target_pages;

    /*
     * Drain on every pass, then walk. The earlier odd-pass alternation existed because draining
     * removed candidates the walker would have found and they were lost; now the LRU-trim re-insert
     * keeps those pages in the ring, so the drain is no longer extractive. Drain first to feed the
     * pre-identified dirty candidates, then fall through to the walk for the rest of the budget.
     * Disabled trees still probe periodically to detect a shift back to write-heavy.
     */
    pass_gen = __wt_atomic_load_uint64_relaxed(&evict->evict_pass_gen);
    if (__evict_drain_stable_blocked(session, btree)) {
        /*
         * The precise checkpoint cannot evict most of the ring until the pinned stable timestamp
         * crosses the median commit timestamp the last drain pass found blocking it. Skip the drain
         * entirely rather than park its cadence: re-examining the ring would only re-pay the
         * candidacy filter and re-insert refs it cannot queue, and a probe pass re-drains the whole
         * backlog so parking does not reduce that volume. The walker still evicts any page that has
         * since fallen below stable.
         */
        should_drain = false;
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_drain_skipped_stable_lag);
    } else if (__wt_atomic_load_bool_relaxed(&btree->drain_disabled))
        should_drain = (pass_gen % WTI_DRAIN_PROBE_INTERVAL) == 0;
    else if (F_ISSET(evict, WT_EVICT_CACHE_CLEAN))
        /*
         * The ring is leaf-only, so only the walker queues internal pages. When the drain has
         * filled the whole budget for WTI_DRAIN_PROBE_INTERVAL consecutive passes the walker has no
         * free slots and the internal tier goes stale and accumulates in cache. Skip the drain this
         * pass so the walker gets the full budget and reclaims internal pages.
         *
         * Only force this when the overall cache is over its target (WT_EVICT_CACHE_CLEAN).
         * Internal pages need eviction only under whole-cache pressure; gating on dirty/updates
         * pressure alone would fire during the insert-heavy load phase -- where the cache is still
         * filling and the forced tree walk just steals write throughput from a drain that is
         * keeping up.
         */
        should_drain =
          __wt_atomic_load_uint32_relaxed(&btree->drain_filled_skips) < WTI_DRAIN_PROBE_INTERVAL;
    else
        should_drain = true;

    if (should_drain && __wt_atomic_load_ptr_acquire(&btree->dirty_index) != NULL &&
      F_ISSET(evict, WT_EVICT_CACHE_DIRTY | WT_EVICT_CACHE_UPDATES)) {
        if (F_ISSET(evict, WT_EVICT_CACHE_NOKEEP)) {
            /*
             * WT_EVICT_CACHE_NOKEEP means the cache is more than halfway to the eviction trigger;
             * clean-page demand is dominant. Pre-queuing dirty pages from the ring competes for the
             * same queue slots the walker needs for clean eviction. Skip without touching
             * drain_consecutive_empty so the drain resumes as soon as pressure drops.
             */
            WT_STAT_CONN_DSRC_INCR(
              session, cache_eviction_dirty_index_drain_skipped_clean_pressure);
        } else {
            drain_queued = __evict_dirty_index_drain(
              session, btree, queue, (u_int)(end - queue->evict_queue), slotp);

            /*
             * Update the adaptive switch. Atomic accesses are used because eviction passes from
             * different threads may visit this btree; the values carry no ordering dependency. A
             * drain skipped because the tree is checkpointing also returns zero -- exclude that
             * case so a long checkpoint does not park the drain on a tree whose ring is full, not
             * empty.
             */
            if (drain_queued > 0) {
                __wt_atomic_store_uint32(&btree->drain_consecutive_empty, 0);
                if (__wt_atomic_load_bool_relaxed(&btree->drain_disabled))
                    __wt_atomic_store_bool(&btree->drain_disabled, false);
            } else if (!WT_BTREE_SYNCING(btree) &&
              __wt_atomic_add_uint32(&btree->drain_consecutive_empty, 1) >=
                WTI_DRAIN_EMPTY_THRESHOLD)
                __wt_atomic_store_bool(&btree->drain_disabled, true);
        }
    }
    if (drain_queued >= target_pages) {
        /*
         * The drain filled this tree's budget; skip the walker this pass. Return before touching
         * the saved walk position: falling through would load and release it, count an abandoned
         * walk, and restart from the root next pass. On a read-mixed single-tree workload that walk
         * churn contends with the application cursors. Leave the position for the next visit and
         * keep the visit period hot, since the drain is productive here. Count the consecutive
         * drain-filled passes so the should-drain gate above eventually yields a pass to the
         * walker.
         */
        __wt_atomic_add_uint32(&btree->drain_filled_skips, 1);
        __wt_atomic_store_uint32_relaxed(&btree->evict_walk_period, 0);
        WT_STAT_CONN_DSRC_INCR(session, eviction_walk_passes);
        WT_STAT_CONN_INCR(session, eviction_walk_skipped_drain_filled);
        return (0);
    }

    /*
     * The walker is about to run -- either the drain was skipped this pass or it did not fill the
     * budget. Either way the internal tier gets a look, so reset the drain-filled run length.
     */
    __wt_atomic_store_uint32_relaxed(&btree->drain_filled_skips, 0);
    start = queue->evict_queue + *slotp;

    min_pages = __evict_get_min_pages(session, target_pages);

    WT_RET_NOTFOUND_OK(__evict_walk_prepare(session, &walk_flags));

    /*
     * Get some more eviction candidate pages, starting at the last saved point. Clear the saved
     * point immediately, we assert when discarding pages we're not discarding an eviction point, so
     * this clear must be complete before the page is released.
     */
    ref = __wt_atomic_load_ptr_relaxed(&btree->evict_ref);
    __wt_atomic_store_ptr_relaxed(&btree->evict_ref, NULL);
    /* Clear the saved position just in case we never put it back. */
    __wt_evict_clear_npos(btree);

    /*
     * Get the snapshot for the eviction server when we want to evict dirty content under cache
     * pressure. This snapshot is used to check for the visibility of the last modified transaction
     * id on the page.
     */
    if (F_ISSET(evict, WT_EVICT_CACHE_DIRTY_HARD | WT_EVICT_CACHE_UPDATES_HARD))
        __wt_txn_bump_snapshot(session);

    /*
     * !!! Take care terminating this loop.
     *
     * Don't make an extra call to __wt_tree_walk after we hit the end of a
     * tree: that will leave a page pinned, which may prevent any work from
     * being done.
     *
     * Once we hit the page limit, do one more step through the walk in
     * case we are appending and only the last page in the file is live.
     */
    internal_pages_already_queued = internal_pages_queued = internal_pages_seen = 0;
    pages_seen_clean = pages_seen_dirty = pages_seen_updates = 0;
    root_pages_skipped = 0;
    for (evict_entry = start, pages_already_queued = pages_queued = pages_seen = refs_walked = 0;
      evict_entry < end && (ret == 0 || ret == WT_NOTFOUND);
      last_parent = ref == NULL ? NULL : ref->home,
        ret = __wt_tree_walk_count(session, &ref, &refs_walked, walk_flags)) {

        /*
         * Credit the candidates the dirty-index drain already queued for this tree. Without this,
         * the walker counts only what it queued itself and abandons a tree the ring just serviced
         * as a "desert", starving the eviction queue on write-heavy trees.
         */
        if ((give_up = __evict_should_give_up_walk(
               session, pages_seen, pages_queued + drain_queued, min_pages, target_pages)))
            break;

        if (ref == NULL) {
            WT_STAT_CONN_INCR(session, eviction_walks_ended);

            if (++restarts == 2) {
                WT_STAT_CONN_INCR(session, eviction_walks_stopped);
                break;
            }
            WT_STAT_CONN_INCR(session, eviction_walks_started);
            continue;
        }

        ++pages_seen;

        /* Ignore root pages entirely. */
        if (__wt_ref_is_root(ref)) {
            ++root_pages_skipped;
            continue;
        }

        page = ref->page;

        /*
         * Update the maximum evict pass generation gap seen at time of eviction. This helps track
         * how long it's been since a page was last queued for eviction. We need to update the
         * statistic here during the walk and not at __evict_page because the evict_pass_gen is
         * reset here.
         */
        if (page->evict_pass_gen == 0) {
            const uint64_t gen_gap =
              __wt_atomic_load_uint64_relaxed(&evict->evict_pass_gen) - page->cache_create_gen;
            __wt_atomic_stats_max_uint64(&evict->evict_max_unvisited_gen_gap, gen_gap);
            __wt_atomic_stats_max_uint64(
              &evict->evict_max_unvisited_gen_gap_per_checkpoint, gen_gap);
        } else {
            const uint64_t gen_gap =
              __wt_atomic_load_uint64_relaxed(&evict->evict_pass_gen) - page->evict_pass_gen;
            __wt_atomic_stats_max_uint64(&evict->evict_max_visited_gen_gap, gen_gap);
            __wt_atomic_stats_max_uint64(&evict->evict_max_visited_gen_gap_per_checkpoint, gen_gap);
        }

        page->evict_pass_gen = __wt_atomic_load_uint64_relaxed(&evict->evict_pass_gen);

        if (__wt_page_is_modified(page))
            ++pages_seen_dirty;
        else
            ++pages_seen_clean;

        if (__evict_page_updates_candidate(page))
            ++pages_seen_updates;

        /* Count internal pages seen. */
        if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
            internal_pages_seen++;

        /* Use the EVICT_LRU flag to avoid putting pages onto the list multiple times. */
        if (F_ISSET_ATOMIC_16(page, WT_PAGE_EVICT_LRU)) {
            pages_already_queued++;
            if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
                internal_pages_already_queued++;
            continue;
        }

        __evict_try_queue_page(
          session, queue, ref, last_parent, evict_entry, &urgent_queued, &queued);
        if (queued) {
            ++evict_entry;
            ++pages_queued;
            ++btree->evict_walk_progress;

            /* Count internal pages queued. */
            if (F_ISSET(ref, WT_REF_FLAG_INTERNAL))
                internal_pages_queued++;
        }
    }
    if (F_ISSET(txn, WT_TXN_HAS_SNAPSHOT))
        __wt_txn_release_snapshot(session);
    WT_RET_NOTFOUND_OK(ret);

    *slotp += (u_int)(evict_entry - start);
    WT_STAT_CONN_INCRV(session, eviction_pages_ordinary_queued, (u_int)(evict_entry - start));

    __wt_verbose_debug2(session, WT_VERB_EVICTION,
      "%s walk: target %" PRIu32 ", seen %" PRIu64 ", queued %" PRIu64, session->dhandle->name,
      target_pages, pages_seen, pages_queued);

    /*
     * If we couldn't find the number of pages we were looking for, skip the tree next time. Count
     * the candidates the dirty-index drain queued for this tree as well: the throttle decides how
     * often the eviction server visits the tree, and the drain runs only as part of that visit, so
     * backing off a tree the ring is actively servicing would starve both the walker and the drain.
     */
    evict_walk_period = __wt_atomic_load_uint32_relaxed(&btree->evict_walk_period);
    if (pages_queued + drain_queued < target_pages / 2 && !urgent_queued)
        __wt_atomic_store_uint32_relaxed(
          &btree->evict_walk_period, WT_MIN(WT_MAX(1, 2 * evict_walk_period), 100));
    else if (pages_queued + drain_queued >= target_pages) {
        __wt_atomic_store_uint32_relaxed(&btree->evict_walk_period, 0);
        /*
         * If there's a chance the Btree was fully evicted, update the evicted flag in the handle.
         */
        if (__wt_btree_bytes_evictable(session) == 0)
            FLD_SET(session->dhandle->advisory_flags, WT_DHANDLE_ADVISORY_EVICTED);
    } else if (evict_walk_period > 0)
        __wt_atomic_store_uint32_relaxed(&btree->evict_walk_period, evict_walk_period / 2);

    /*
     * Give up the walk occasionally.
     *
     * If we happen to end up on the root page or a page requiring urgent eviction, clear it. We
     * have to track hazard pointers, and the root page complicates that calculation.
     *
     * Likewise if we found no new candidates during the walk: there is no point keeping a page
     * pinned, since it may be the only candidate in an idle tree.
     *
     * If we land on a page requiring forced eviction, or that isn't an ordinary in-memory page,
     * move until we find an ordinary page: we should not prevent exclusive access to the page until
     * the next walk.
     */
    if (ref != NULL) {
        if (__wt_ref_is_root(ref) || evict_entry == start || give_up ||
          __wt_atomic_load_size_relaxed(&ref->page->memory_footprint) >= btree->splitmempage) {
            if (restarts == 0)
                WT_STAT_CONN_INCR(session, eviction_walks_abandoned);
            WT_RET(__wt_page_release(evict->walk_session, ref, walk_flags));
            ref = NULL;
        } else {
            while (ref != NULL &&
              (WT_REF_GET_STATE(ref) != WT_REF_MEM ||
                __wti_evict_readgen_is_soon_or_wont_need(&ref->page->read_gen)))
                WT_RET_NOTFOUND_OK(__wt_tree_walk_count(session, &ref, &refs_walked, walk_flags));
        }
        /*
         * The current ref is obtained from tree walk and is protected by a hazard pointer, so there
         * is no concurrency conflict with __wt_ref_out reading evict_ref in the eviction process.
         * Using atomic store here is for code consistency with other accesses to btree->evict_ref.
         */
        __wt_atomic_store_ptr_relaxed(&btree->evict_ref, ref);
        if (evict->use_npos_in_pass)
            WT_TRET(__evict_clear_walk(session, false));
    }

    WT_STAT_CONN_INCRV(session, eviction_walk, refs_walked);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen, pages_seen);
    WT_STAT_CONN_INCRV(session, eviction_pages_already_queued, pages_already_queued);
    WT_STAT_CONN_INCRV(session, eviction_internal_pages_seen, internal_pages_seen);
    WT_STAT_CONN_INCRV(
      session, eviction_internal_pages_already_queued, internal_pages_already_queued);
    WT_STAT_CONN_INCRV(session, eviction_internal_pages_queued, internal_pages_queued);
    WT_STAT_CONN_DSRC_INCR(session, eviction_walk_passes);
    WT_STAT_CONN_INCRV(session, eviction_root_pages_skipped, root_pages_skipped);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_clean, pages_seen_clean);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_dirty, pages_seen_dirty);
    WT_STAT_CONN_DSRC_INCRV(session, cache_eviction_pages_seen_updates, pages_seen_updates);
    return (0);
}
