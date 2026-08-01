/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define WTI_DIRTY_INDEX_ESTIMATED_PAGE_SIZE_MAX (64u * 1024)

/*
 * __evict_dirty_index_capacity --
 *     Return a power-of-two slot count based on the btree's file size.
 */
static uint32_t
__evict_dirty_index_capacity(WT_BTREE *btree, uint64_t size_hint)
{
    uint32_t capacity;

    if (WT_IS_HS(btree->dhandle))
        return (WTI_DIRTY_INDEX_MAX_CAPACITY);

    capacity = (uint32_t)WT_MIN(UINT32_MAX,
      size_hint / WT_MAX(1u, WT_MIN(btree->maxleafpage, WTI_DIRTY_INDEX_ESTIMATED_PAGE_SIZE_MAX)));
    capacity = WT_CLAMP(capacity, WTI_DIRTY_INDEX_MIN_CAPACITY, WTI_DIRTY_INDEX_MAX_CAPACITY);
    --capacity;
    capacity |= capacity >> 1;
    capacity |= capacity >> 2;
    capacity |= capacity >> 4;
    capacity |= capacity >> 8;
    capacity |= capacity >> 16;
    return (++capacity);
}

/* Allocate and publish the slot array once, after a producer passes eligibility checks. */
static bool
__evict_dirty_index_ensure_slots(WT_SESSION_IMPL *session, WTI_DIRTY_INDEX *idx)
{
    WTI_DIRTY_INDEX_SLOT *slots;
    uint32_t i;

    if (__wt_atomic_load_ptr_acquire(&idx->slots) != NULL)
        return (true);
    if (__wt_calloc_def(session, idx->capacity, &slots) != 0)
        return (false);
    for (i = 0; i < idx->capacity; ++i)
        __wt_atomic_store_uint64_release(&slots[i].sequence, i);

    if (!__wt_atomic_cas_ptr(&idx->slots, NULL, slots))
        __wt_free(session, slots);
    return (true);
}

/*
 * __wt_dirty_index_alloc --
 *     Allocate the per-btree dirty-page ring before the handle is published for eviction.
 */
int
__wt_dirty_index_alloc(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WTI_DIRTY_INDEX *idx;
    wt_off_t file_size;
    uint32_t capacity;

    if (!__wt_atomic_load_bool_relaxed(&S2C(session)->evict->eviction_dirty_index) ||
      WT_IS_METADATA(btree->dhandle) || WT_IS_DISAGG_META(btree->dhandle) ||
      (F_ISSET(btree, WT_BTREE_DISAGGREGATED) &&
        !__wt_atomic_load_bool_relaxed(&S2C(session)->evict->eviction_dirty_index_disagg)) ||
      F_ISSET(btree, WT_BTREE_READONLY | WT_BTREE_NO_EVICT | WT_BTREE_SALVAGE | WT_BTREE_VERIFY) ||
      WT_DHANDLE_IS_CHECKPOINT(btree->dhandle))
        return (0);
    if (__wt_atomic_load_ptr_acquire(&btree->dirty_index) != NULL)
        return (0);

    file_size = 0;
    if (btree->bm != NULL)
        WT_RET(btree->bm->size(btree->bm, session, &file_size));
    capacity = __evict_dirty_index_capacity(btree, (uint64_t)file_size);

    WT_RET(__wt_calloc_one(session, &idx));
    idx->capacity = capacity;
    idx->mask = capacity - 1;
    __wt_atomic_store_ptr_release(&btree->dirty_index, idx);
    return (0);
}

/*
 * __wt_dirty_index_destroy --
 *     Free the per-btree dirty-page ring when the btree handle closes.
 */
void
__wt_dirty_index_destroy(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WTI_DIRTY_INDEX *idx;
    WTI_DIRTY_INDEX_SLOT *slots;

    if ((idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL)
        return;
    __wt_atomic_store_ptr_release(&btree->dirty_index, NULL);
    slots = __wt_atomic_load_ptr_acquire(&idx->slots);
    __wt_free(session, slots);
    __wt_free(session, idx);
}

/*
 * __wt_dirty_index_insert --
 *     Publish a leaf ref into the per-btree ring without blocking. Producers reserve a position by
 *     CAS-advancing head, but only once the target slot's sequence counter confirms the previous
 *     occupant has already been drained; a sequence lagging the reserved position means the ring is
 *     full, one ahead of it means another producer just took this position, and either way this
 *     producer retries or bails rather than wait. Winning the head CAS still isn't ownership: the
 *     producer must also win the page's one-owner back-pointer CAS, then recheck the ref/page/state
 *     did not change underneath it while doing so. Losing any of these races abandons the slot
 *     instead of blocking, since the ring is best-effort and the eviction walker remains the source
 *     of truth.
 */
bool
__wt_dirty_index_insert(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref)
{
    WT_EVICT *evict;
    WTI_DIRTY_INDEX *idx;
    WTI_DIRTY_INDEX_SLOT *slotp;
    WT_PAGE *page;
    uint64_t pos, seq;
    int64_t dif;
    uint32_t slot;

    evict = S2C(session)->evict;
    if (!__wt_atomic_load_bool_relaxed(&evict->eviction_dirty_index) ||
      !F_ISSET(ref, WT_REF_FLAG_LEAF) || WT_REF_GET_STATE(ref) != WT_REF_MEM ||
      (page = __wt_atomic_load_ptr_acquire(&ref->page)) == NULL || page->modify == NULL ||
      (idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL)
        return (false);
    if (F_ISSET(btree, WT_BTREE_DISAGGREGATED) &&
      !__wt_atomic_load_bool_relaxed(&evict->eviction_dirty_index_disagg))
        return (false);
    if (__wt_atomic_load_uint32_relaxed(&page->dirty_index_slot) != WTI_DIRTY_BP_NONE)
        return (false);
    if (!__evict_dirty_index_ensure_slots(session, idx))
        return (false);
    idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index);
    if (idx == NULL || __wt_atomic_load_ptr_acquire(&idx->slots) == NULL)
        return (false);

    pos = __wt_atomic_load_uint64_relaxed(&idx->head);
    for (;;) {
        slotp = &__wt_atomic_load_ptr_acquire(&idx->slots)[(uint32_t)pos & idx->mask];
        seq = __wt_atomic_load_uint64_acquire(&slotp->sequence);
        dif = (int64_t)(seq - pos);
        if (dif == 0) {
            if (__wt_atomic_cas_uint64_relaxed(&idx->head, pos, pos + 1))
                break;
            WT_PAUSE();
            pos = __wt_atomic_load_uint64_relaxed(&idx->head);
        } else if (dif < 0) {
            if (WT_STAT_ENABLED(session)) {
                __wt_atomic_stats_max_uint64(
                  &evict->dirty_index_ring_full_capacity_max, idx->capacity);
                __wt_atomic_stats_max_uint64(
                  &evict->dirty_index_ring_peak_occupancy, idx->capacity);
            }
            WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_ring_full);
            return (false);
        } else
            pos = __wt_atomic_load_uint64_relaxed(&idx->head);
    }

    slot = (uint32_t)pos & idx->mask;
    /*
     * Winning the head CAS makes this slot exclusively ours: no other producer can reach position
     * pos again until the drain consumes it and the ring wraps a full lap. It does not make the
     * page ours -- another producer racing to insert the same page, or the drain re-inserting it
     * after a filtered pop, may already own the page's back-pointer, so the CAS below can still
     * lose even though the slot itself was never contended. A hazard pointer rules out concurrent
     * teardown, but not this kind of race: it grants shared access, not exclusive ownership of the
     * back-pointer.
     */
    if (!__wt_atomic_cas_uint32(
          &page->dirty_index_slot, WTI_DIRTY_BP_NONE, WTI_DIRTY_BP_MAKE(slot))) {
        __wt_atomic_store_ptr_release(&slotp->ref, NULL);
        /*
         * Publish the slot as consumed even though it stayed empty: the drain only advances tail
         * once it sees sequence == pos + 1, so leaving it at pos would wedge the ring here.
         */
        __wt_atomic_store_uint64_release(&slotp->sequence, pos + 1);
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_contended);
        return (false);
    }

    __wt_atomic_store_ptr_release(&slotp->ref, ref);
    if (__wt_atomic_load_uint32_acquire(&page->dirty_index_slot) != WTI_DIRTY_BP_MAKE(slot) ||
      __wt_atomic_load_ptr_acquire(&ref->page) != page || WT_REF_GET_STATE(ref) != WT_REF_MEM) {
        (void)__wt_atomic_cas_ptr(&slotp->ref, ref, NULL);
        /* Same wedge-avoidance publish as the back-pointer CAS failure above. */
        __wt_atomic_store_uint64_release(&slotp->sequence, pos + 1);
        /* Release ownership of the back-pointer; a no-op if it was already cleared elsewhere. */
        (void)__wt_atomic_cas_uint32(
          &page->dirty_index_slot, WTI_DIRTY_BP_MAKE(slot), WTI_DIRTY_BP_NONE);
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_contended);
        return (false);
    }
    __wt_atomic_store_uint64_release(&slotp->sequence, pos + 1);
    WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert);
    return (true);
}

/*
 * __wt_dirty_index_block_page --
 *     Block dirty-index producers while a ref is being retired. If a producer owns the page
 *     back-pointer but has not published its ref yet, wait for publication before taking the block.
 *     The expected ref is conditionally removed after the block is acquired.
 */
bool
__wt_dirty_index_block_page(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref, WT_PAGE *page)
{
    WTI_DIRTY_INDEX *idx;
    WTI_DIRTY_INDEX_SLOT *slotp;
    WT_REF *published_ref;
    uint32_t bp;

    WT_UNUSED(session);
    if (page == NULL || (idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL ||
      __wt_atomic_load_ptr_acquire(&idx->slots) == NULL)
        return (false);

    for (;;) {
        bp = __wt_atomic_load_uint32_acquire(&page->dirty_index_slot);
        if (bp == WTI_DIRTY_BP_BLOCKED)
            return (true);
        if (bp == WTI_DIRTY_BP_NONE) {
            if (__wt_atomic_cas_uint32(&page->dirty_index_slot, WTI_DIRTY_BP_NONE,
                  WTI_DIRTY_BP_BLOCKED))
                return (true);
            continue;
        }

        if (WTI_DIRTY_BP_SLOT(bp) >= idx->capacity)
            continue;
        slotp = &__wt_atomic_load_ptr_acquire(&idx->slots)[WTI_DIRTY_BP_SLOT(bp)];
        published_ref = __wt_atomic_load_ptr_acquire(&slotp->ref);
        if (published_ref == NULL) {
            WT_PAUSE();
            continue;
        }
        if (!__wt_atomic_cas_uint32(&page->dirty_index_slot, bp, WTI_DIRTY_BP_BLOCKED))
            continue;

        /* Do not clear a newer entry if the old ref has already been replaced. */
        if (!__wt_atomic_cas_ptr(&slotp->ref, ref, NULL)) {
            published_ref = __wt_atomic_load_ptr_acquire(&slotp->ref);
            if (published_ref != NULL && published_ref != ref)
                __wt_dirty_index_unblock_page(page);
        }
        return (true);
    }
}

/*
 * __wt_dirty_index_unblock_page --
 *     Allow a page retained by a usable replacement ref to re-enter the dirty index.
 */
void
__wt_dirty_index_unblock_page(WT_PAGE *page)
{
    if (page != NULL)
        (void)__wt_atomic_cas_uint32(
          &page->dirty_index_slot, WTI_DIRTY_BP_BLOCKED, WTI_DIRTY_BP_NONE);
}

/*
 * __wt_dirty_index_clear_page --
 *     Invalidate a page's published entry without waiting for the eviction consumer. Idempotent and
 *     safe to call more than once for the same page: a page never has two simultaneously-live ring
 *     entries (the back-pointer names at most one slot), so a second call after the first has
 *     already cleared it is a no-op. Split retirement uses __wt_dirty_index_block_page instead so
 *     the page cannot acquire a new entry between cleanup and the ref state transition.
 */
void
__wt_dirty_index_clear_page(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref, WT_PAGE *page)
{
    WTI_DIRTY_INDEX *idx;
    WTI_DIRTY_INDEX_SLOT *slotp;
    uint32_t bp;

    WT_UNUSED(session);
    /* Check the page's own back-pointer first: zero means it never entered the ring. */
    if (page == NULL ||
      (bp = __wt_atomic_load_uint32_acquire(&page->dirty_index_slot)) == WTI_DIRTY_BP_NONE)
        return;
    if (bp == WTI_DIRTY_BP_BLOCKED)
        return;
    if ((idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL ||
      __wt_atomic_load_ptr_acquire(&idx->slots) == NULL ||
      WTI_DIRTY_BP_SLOT(bp) >= idx->capacity)
        return;

    slotp = &__wt_atomic_load_ptr_acquire(&idx->slots)[WTI_DIRTY_BP_SLOT(bp)];
    /* Only clear the page back-pointer if this ref still owns the slot. */
    if (__wt_atomic_cas_ptr(&slotp->ref, ref, NULL))
        (void)__wt_atomic_cas_uint32(&page->dirty_index_slot, bp, WTI_DIRTY_BP_NONE);
}
