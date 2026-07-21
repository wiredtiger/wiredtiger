/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

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

    capacity =
      (uint32_t)WT_MIN(UINT32_MAX, size_hint / WT_MAX(1u, WT_MIN(btree->maxleafpage, 64u * 1024)));
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
    uint32_t capacity;

    if (!S2C(session)->evict->eviction_dirty_index || WT_IS_METADATA(btree->dhandle) ||
      WT_IS_DISAGG_META(btree->dhandle) ||
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
    WT_ERR(__wt_spin_init(session, &idx->lock, "dirty index"));
    WT_ERR(__wt_calloc_def(session, capacity, &idx->slots));
    __wt_atomic_store_ptr_release(&btree->dirty_index, idx);
    return (0);

err:
    if (idx != NULL) {
        __wt_spin_destroy(session, &idx->lock);
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
    __wt_spin_destroy(session, &idx->lock);
    __wt_free(session, idx->slots);
    __wt_free(session, idx);
}

/*
 * __wt_dirty_index_insert --
 *     Record a leaf ref in the ring. A producer that cannot acquire the ring lock abandons this
 *     best-effort fast path; the eviction walker remains the source of truth.
 */
bool
__wt_dirty_index_insert(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref)
{
    WT_EVICT *evict;
    WTI_DIRTY_INDEX *idx;
    WT_PAGE *page;
    uint32_t occupancy, slot;

    if ((idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL ||
      !__wt_atomic_load_bool_relaxed(&S2C(session)->evict->eviction_dirty_index) ||
      !F_ISSET(ref, WT_REF_FLAG_LEAF) || (page = ref->page) == NULL || page->modify == NULL)
        return (false);

    evict = S2C(session)->evict;
    if (__wt_spin_trylock(session, &idx->lock) != 0) {
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_contended);
        return (false);
    }
    if (page->dirty_index_slot != 0)
        goto done;

    occupancy = idx->head - idx->tail;
    if (occupancy >= idx->capacity) {
        __wt_atomic_stats_max_uint64(&evict->dirty_index_ring_full_capacity_max, idx->capacity);
        WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert_ring_full);
        goto done;
    }

    slot = idx->head++ & idx->mask;
    idx->slots[slot] = ref;
    page->dirty_index_slot = WTI_DIRTY_BP_MAKE(slot);
    __wt_atomic_stats_max_uint64(&evict->dirty_index_ring_peak_occupancy, occupancy + 1);
    WT_STAT_CONN_DSRC_INCR(session, cache_eviction_dirty_index_insert);
    __wt_spin_unlock(session, &idx->lock);
    return (true);

done:
    __wt_spin_unlock(session, &idx->lock);
    return (false);
}

/*
 * __wt_dirty_index_clear_page --
 *     Remove a page from the ring before its memory is released. The ring lock protects the slot
 *     and page back-pointer as one operation.
 */
void
__wt_dirty_index_clear_page(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref, WT_PAGE *page)
{
    WTI_DIRTY_INDEX *idx;
    uint32_t bp;

    if (page == NULL || (idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL)
        return;

    __wt_spin_lock(session, &idx->lock);
    bp = page->dirty_index_slot;
    if (bp != 0) {
        if (WTI_DIRTY_BP_SLOT(bp) < idx->capacity && idx->slots[WTI_DIRTY_BP_SLOT(bp)] == ref)
            idx->slots[WTI_DIRTY_BP_SLOT(bp)] = NULL;
        page->dirty_index_slot = 0;
    }
    __wt_spin_unlock(session, &idx->lock);
}
