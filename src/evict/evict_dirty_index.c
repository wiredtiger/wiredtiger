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

/*
 * __wt_dirty_index_alloc --
 *     Allocate the per-btree dirty-page ring before the handle is published for eviction.
 */
int
__wt_dirty_index_alloc(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_DECL_RET;
    WTI_DIRTY_INDEX *idx;
    wt_off_t file_size;
    uint32_t capacity, i;

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
    WT_ERR(__wt_calloc_def(session, capacity, &idx->slots));
    for (i = 0; i < capacity; ++i)
        __wt_atomic_store_uint64_release(&idx->slots[i].sequence, i);
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
 *     Free the per-btree dirty-page ring when the btree handle closes.
 */
void
__wt_dirty_index_destroy(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WTI_DIRTY_INDEX *idx;

    if ((idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL)
        return;
    __wt_atomic_store_ptr_release(&btree->dirty_index, NULL);
    __wt_free(session, idx->slots);
    __wt_free(session, idx);
}

/*
 * __wt_dirty_index_insert --
 *     Publish a leaf ref without waiting. A producer abandons the fast path when the bounded ring
 *     is full or another producer wins the page back-pointer.
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
    if (__wt_atomic_load_uint32_relaxed(&page->dirty_index_slot) != 0)
        return (false);

    pos = __wt_atomic_load_uint64_relaxed(&idx->head);
    for (;;) {
        slotp = &idx->slots[(uint32_t)pos & idx->mask];
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
    if (!__wt_atomic_cas_uint32(&page->dirty_index_slot, 0, WTI_DIRTY_BP_MAKE(slot))) {
        __wt_atomic_store_ptr_release(&slotp->ref, NULL);
        __wt_atomic_store_uint64_release(&slotp->sequence, pos + 1);
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_contended);
        return (false);
    }

    __wt_atomic_store_ptr_release(&slotp->ref, ref);
    if (__wt_atomic_load_uint32_acquire(&page->dirty_index_slot) != WTI_DIRTY_BP_MAKE(slot) ||
      __wt_atomic_load_ptr_acquire(&ref->page) != page || WT_REF_GET_STATE(ref) != WT_REF_MEM) {
        (void)__wt_atomic_cas_ptr(&slotp->ref, ref, NULL);
        __wt_atomic_store_uint64_release(&slotp->sequence, pos + 1);
        /* Release ownership of the back-pointer; a no-op if it was already cleared elsewhere. */
        (void)__wt_atomic_cas_uint32(&page->dirty_index_slot, WTI_DIRTY_BP_MAKE(slot), 0);
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_contended);
        return (false);
    }
    __wt_atomic_store_uint64_release(&slotp->sequence, pos + 1);
    WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert);
    return (true);
}

/*
 * __wt_dirty_index_clear_page --
 *     Invalidate a page's published entry without waiting for the eviction consumer.
 */
void
__wt_dirty_index_clear_page(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref, WT_PAGE *page)
{
    WTI_DIRTY_INDEX *idx;
    WTI_DIRTY_INDEX_SLOT *slotp;
    uint32_t bp;

    WT_UNUSED(session);
    /* Check the page's own back-pointer first: zero means it never entered the ring. */
    if (page == NULL || (bp = __wt_atomic_load_uint32_acquire(&page->dirty_index_slot)) == 0)
        return;
    if ((idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL ||
      WTI_DIRTY_BP_SLOT(bp) >= idx->capacity)
        return;

    slotp = &idx->slots[WTI_DIRTY_BP_SLOT(bp)];
    /* Only clear the page back-pointer if this ref still owns the slot. */
    if (__wt_atomic_cas_ptr(&slotp->ref, ref, NULL))
        (void)__wt_atomic_cas_uint32(&page->dirty_index_slot, bp, 0);
}

/*
 * __wt_dirty_index_clear_ref --
 *     Remove a ref that is about to be freed from its ring slot, if any. Splits replace a WT_REF
 *     while retaining its page, so by the time this runs the page back-pointer may no longer
 *     identify the ref the caller is discarding. The caller's ordering (clear_page for the old ref,
 *     then WT_REF_SET_STATE to a non-WT_REF_MEM state, then this call) guarantees no producer can
 *     insert the old ref afterward, so a page never has two simultaneously-live ring entries --
 *     the back-pointer, if still nonzero at this point, can only name the one slot that could still
 *     reference it. That is exactly what clear_page checks, so delegate to it rather than scanning
 *     the ring.
 */
void
__wt_dirty_index_clear_ref(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref, WT_PAGE *page)
{
    __wt_dirty_index_clear_page(session, btree, ref, page);
}
