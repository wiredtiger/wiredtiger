/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * Per-btree dirty-page index.
 *
 * A cursor modify feeds the dirtied leaf ref into a per-btree ring. The eviction walker drains the
 * ring to queue candidates without re-walking the tree. The walker remains the source of truth and
 * runs alongside the drain.
 *
 * Dedup is via the page's back-pointer: a non-zero slot means the page is already in the ring. The
 * consumer is the eviction server (one thread per btree visit).
 */

/*
 * __evict_dirty_index_capacity --
 *     Pick a slot count from the file size hint. Clamped to [MIN, MAX] and rounded to a power of
 *     two so the producer and consumer can mask-index into the slot array.
 */
static uint32_t
__evict_dirty_index_capacity(WT_BTREE *btree, uint64_t size_hint)
{
    uint32_t capacity;

    /*
     * The history store is a high-volume sink for every timestamped update across the connection,
     * but at first checkpoint its on-disk size is effectively zero -- the size-hint path would
     * clamp it to MIN and the ring would saturate permanently. Size it at MAX up front; it is
     * per-connection so the larger ring is a single fixed-cost allocation, not per-table overhead.
     */
    if (WT_IS_HS(btree->dhandle))
        return (WTI_DIRTY_INDEX_MAX_CAPACITY);

    /*
     * Divisor capped at 64 KB so an unusually large configured leaf page does not under-size the
     * ring.
     */
    capacity =
      (uint32_t)WT_MIN(UINT32_MAX, size_hint / WT_MAX(1u, WT_MIN(btree->maxleafpage, 64u * 1024)));

    capacity = WT_CLAMP(capacity, WTI_DIRTY_INDEX_MIN_CAPACITY, WTI_DIRTY_INDEX_MAX_CAPACITY);

    /* Round up to next power of two. The clamp bounds are already powers of two. */
    --capacity;
    capacity |= capacity >> 1;
    capacity |= capacity >> 2;
    capacity |= capacity >> 4;
    capacity |= capacity >> 8;
    capacity |= capacity >> 16;
    return (++capacity);
}

/*
 * __wt_dirty_index_alloc --
 *     Allocate the per-btree ring at btree open, before the handle is published for eviction, so
 *     there is a single allocator and neither a producer nor the drain races the initial store.
 *     Sized from the on-disk file size; the dirty footprint is zero at open so it adds nothing.
 *     Auto-grow may resize the ring later, serialized under the eviction walk lock.
 */
int
__wt_dirty_index_alloc(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_DECL_RET;
    WTI_DIRTY_INDEX *idx;
    wt_off_t bm_file_size;
    uint32_t capacity;

    idx = NULL;

    if (!S2C(session)->evict->eviction_dirty_index)
        return (0);

    if (__wt_atomic_load_ptr_acquire(&btree->dirty_index) != NULL)
        return (0);

    /* Metadata, including the disaggregated shared metadata, has its own dedicated eviction path.
     */
    if (WT_IS_METADATA(btree->dhandle) || WT_IS_DISAGG_META(btree->dhandle))
        return (0);

    /*
     * Disaggregated trees are intentionally NOT excluded. A leader's writable stable and ingest
     * btrees take normal cursor writes and benefit from the ring; the drain runs their pages
     * through the same candidacy filter as the walker, which applies the materialization / prune
     * rules, so eviction stays correct. A follower opens the stable tree as a checkpoint handle,
     * which the never-evicted opt-out below skips.
     *
     * Trees whose pages are never evicted would only hold an unused ring: read-only and
     * checkpoint-cursor handles, permanently cache-resident (no-evict) trees, and the transient
     * salvage and verify opens. A freshly created (still bulk-loadable) tree is not excluded here:
     * it takes normal writes and needs the ring. Skipping the rest keeps the feature free on
     * read-only working sets.
     */
    if (F_ISSET(
          btree, WT_BTREE_READONLY | WT_BTREE_NO_EVICT | WT_BTREE_SALVAGE | WT_BTREE_VERIFY) ||
      WT_DHANDLE_IS_CHECKPOINT(btree->dhandle))
        return (0);

    bm_file_size = 0;
    if (btree->bm != NULL)
        WT_RET(btree->bm->size(btree->bm, session, &bm_file_size));
    capacity = __evict_dirty_index_capacity(btree, (uint64_t)bm_file_size);

    WT_RET(__wt_calloc_one(session, &idx));
    idx->capacity = capacity;
    idx->mask = capacity - 1;
    WT_ERR(__wt_calloc_def(session, capacity, &idx->slots));

    /* Release-store pairs with the consumer's acquire load of the ring pointer. */
    __wt_atomic_store_ptr_release(&btree->dirty_index, idx);
    return (0);

err:
    if (idx != NULL) {
        __wt_free(session, idx->slots);
        __wt_free(session, idx);
    }
    return (ret);
}

/*
 * __wt_dirty_index_destroy --
 *     Free the per-btree dirty-page ring.
 */
void
__wt_dirty_index_destroy(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WTI_DIRTY_INDEX *idx, *next, *old;

    /*
     * Called at btree close with no concurrent access; free the active ring and the old-ring list
     * retained by auto-grows.
     */
    for (old = __wt_atomic_load_ptr_acquire(&btree->dirty_index_old); old != NULL; old = next) {
        next = old->next_old;
        __wt_free(session, old->slots);
        __wt_free(session, old);
    }
    __wt_atomic_store_ptr_release(&btree->dirty_index_old, NULL);

    if ((idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL)
        return;

    __wt_atomic_store_ptr_release(&btree->dirty_index, NULL);
    __wt_free(session, idx->slots);
    __wt_free(session, idx);
}

/*
 * __wt_dirty_index_insert --
 *     Record a leaf ref into the btree's ring. Concurrent cursors race on slot reservation via an
 *     atomic fetch-add; the consumer advances tail and the producer abandons a reserved slot if it
 *     would lap the consumer. Returns true only when a new ring entry was created, so callers that
 *     account for their own inserts are not misled by the already-present, ring-full, and contended
 *     bails.
 */
bool
__wt_dirty_index_insert(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref)
{
    WT_EVICT *evict;
    WTI_DIRTY_INDEX *idx;
    WT_PAGE *page;
    uint64_t head, occupancy, tail;
    uint32_t slot;

    if ((idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL)
        return (false);
    evict = S2C(session)->evict;

    /*
     * Honor a runtime disable. The ring is allocated at open and is not freed when the feature is
     * reconfigured off, so a connection that opened with it on keeps its rings; the drain stands
     * down on the same flag, so without this check producers would go on filling rings nothing
     * drains. Relaxed: the flag is read-mostly and a stale read only lets a single ref slip in.
     */
    if (!__wt_atomic_load_bool_relaxed(&evict->eviction_dirty_index))
        return (false);

    /*
     * No reclamation barrier is needed even under auto-grow: a ring retired by a grow is never
     * freed mid-life (retired rings are kept on the btree's old-ring list and freed only at close),
     * so a producer that loaded the ring just before a swap may still write to it safely -- its
     * entry is drained from the now-old ring. The back-pointer carries the ring's generation so the
     * consumer resolves which ring a page is parked in.
     */

    /* Leaf refs only. Scratch refs from the split path lack the flag and must not enter. */
    if (!F_ISSET(ref, WT_REF_FLAG_LEAF))
        return (false);

    page = ref->page;
    if (page == NULL)
        return (false);

    /*
     * Dedup via the page-side back-pointer. Acquire ordering pairs with the consumer's release-
     * store when it clears the back-pointer, so a page the consumer has finished with is never
     * wrongly skipped. A page parked in a retired ring also has a non-zero back-pointer, so it is
     * never inserted twice across a grow.
     */
    if (__wt_atomic_load_uint32_acquire(&page->dirty_index_slot) != 0)
        return (false);

    /*
     * Cheap saturation bail. A producer that found the ring full set this hint; reading it here
     * lets later producers skip the head/tail overflow check below -- specifically the
     * consumer-owned tail load, a coherence miss while the ring stays full. Counted as a ring-full
     * drop like the authoritative paths. The consumer reads the same hint to decide when a ring is
     * regularly full and should grow. Advisory only: those checks remain the correctness boundary.
     */
    if (__wt_atomic_load_uint8_relaxed(&idx->saturated)) {
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_ring_full);
        return (false);
    }

    /*
     * Fast-path bail when the ring is already saturated. Both loads are relaxed: the check is a
     * hint, not a correctness boundary. The post-fetch-add acquire re-check below catches the race.
     */
    head = __wt_atomic_load_uint64_relaxed(&idx->head);
    tail = __wt_atomic_load_uint64_relaxed(&idx->tail);
    /*
     * Guard the subtraction: the two loads are independent and relaxed, so a consumer advancing
     * tail between them can leave tail ahead of the stale head and underflow the unsigned occupancy
     * to a huge value -- which would pin the no_clear high-water stat and spuriously trip the
     * capacity check. Clamp to zero; a transient low reading is harmless for an advisory hint.
     */
    occupancy = head > tail ? head - tail : 0;
    /* Connection-level high-water occupancy across all rings (advisory). */
    __wt_atomic_stats_max_uint64(&evict->dirty_index_ring_peak_occupancy, occupancy);
    if (occupancy >= idx->capacity) {
        __wt_atomic_store_uint8_relaxed(&idx->saturated, 1);
        __wt_atomic_stats_max_uint64(&evict->dirty_index_ring_full_capacity_max, idx->capacity);
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_ring_full);
        return (false);
    }

    /* Reserve a slot with an atomic fetch-add. */
    head = __wt_atomic_add_uint64(&idx->head, 1) - 1;

    /*
     * Re-check saturation: between the fast-path check and the fetch-add, other producers may have
     * taken the remaining room. Without this a burst of concurrent producers could write past the
     * tail boundary.
     */
    tail = __wt_atomic_load_uint64_acquire(&idx->tail);
    occupancy = head > tail ? head - tail : 0;
    if (occupancy >= idx->capacity) {
        __wt_atomic_store_uint8_relaxed(&idx->saturated, 1);
        __wt_atomic_stats_max_uint64(&evict->dirty_index_ring_full_capacity_max, idx->capacity);
        /*
         * Record peak here too: the fast-path sample above used the pre-fetch-add head, so a ring
         * that only crosses full after the fetch-add would otherwise never log its full occupancy.
         */
        __wt_atomic_stats_max_uint64(&evict->dirty_index_ring_peak_occupancy, occupancy);
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_ring_full);
        return (false);
    }

    slot = (uint32_t)(head & idx->mask);
    /*
     * Plain release-store, not a CAS: the fetch-add reserved this slot index exclusively, so no
     * other producer writes it and the consumer has not yet reached it (head just moved past tail).
     * The clear paths (clear_page / clear_ref) do use CAS because they race with a producer or
     * consumer reusing the slot, so they must only null the entry if it still holds the expected
     * ref.
     */
    __wt_atomic_store_ptr_release(&idx->slots[slot], ref);

    /*
     * CAS the back-pointer; if a concurrent producer won, null our slot and back out. The
     * back-pointer carries this ring's generation so the consumer can later tell which ring a page
     * is parked in. With auto-grow off the generation is zero and this is a bare slot+1.
     */
    if (__wt_atomic_cas_uint32(
          &page->dirty_index_slot, 0, WTI_DIRTY_BP_MAKE(idx->generation, slot))) {
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert);
        return (true);
    }

    __wt_atomic_store_ptr_release(&idx->slots[slot], NULL);
    WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_contended);
    return (false);
}

/*
 * __wt_dirty_index_clear_page --
 *     Invalidate the ring entry for a page that's about to be torn down. The caller holds the ref
 *     locked, so the ref and page pointers are stable for the CAS. Acquire ordering pairs with the
 *     producer's release-store on the back-pointer.
 */
void
__wt_dirty_index_clear_page(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref, WT_PAGE *page)
{
    WTI_DIRTY_INDEX *idx;
    uint32_t bp, gen;

    WT_UNUSED(session);

    if (page == NULL)
        return;
    bp = __wt_atomic_load_uint32_acquire(&page->dirty_index_slot);
    if (bp == 0)
        return;

    /*
     * The page may be parked in the active ring or in a ring retired by an auto-grow. Resolve the
     * owning ring by the generation stamped in the back-pointer: the active ring, then the short
     * old-ring list. Retired rings are freed only at btree close, so no reclamation barrier is
     * needed -- the resolved ring is always live. Off the auto-grow path the generation is zero,
     * the active ring matches immediately, and the old-ring list is empty.
     */
    gen = WTI_DIRTY_BP_GEN(bp);
    idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index);
    if (idx == NULL || idx->generation != gen) {
        idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index_old);
        while (idx != NULL && idx->generation != gen)
            idx = __wt_atomic_load_ptr_acquire(&idx->next_old);
    }

    /*
     * If a just-retired ring is not yet visible on the old-ring list the slot clear is skipped; the
     * drain drops the stale entry later. Either way clear the back-pointer so the page can re-enter
     * on its next modify.
     */
    if (idx != NULL)
        (void)__wt_atomic_cas_ptr(&idx->slots[WTI_DIRTY_BP_SLOT(bp)], ref, NULL);
    __wt_atomic_store_uint32_release(&page->dirty_index_slot, 0);
}
