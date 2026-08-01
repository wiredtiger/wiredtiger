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
 * __evict_dirty_index_ensure_slots --
 *     Allocate and publish the slot array once, after a producer passes eligibility checks.
 */
static bool
__evict_dirty_index_ensure_slots(WT_SESSION_IMPL *session, WTI_DIRTY_INDEX *idx)
{
    WTI_DIRTY_INDEX_SLOT *slots;
    uint32_t i;

    if (__wt_atomic_load_ptr_acquire(&idx->slots) != NULL)
        return (true);
    if (__wt_calloc_def(session, idx->capacity, &slots) != 0)
        return (false);
    /*
     * Plain stores: the array is private until the publishing compare-and-swap below, and that is a
     * full barrier, so a consumer that observes the array also observes the seeded counters.
     */
    for (i = 0; i < idx->capacity; ++i)
        slots[i].sequence = i;

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

    if (WT_IS_METADATA(btree->dhandle) || WT_IS_DISAGG_META(btree->dhandle) ||
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

    /*
     * Producers and the drain load the ring without taking a reference to it, so this relies on the
     * caller having already made the handle unreachable: nothing here can wait out a thread that is
     * mid-insert. Only reached while discarding the handle, after the last session reference drops.
     */
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
    WTI_DIRTY_INDEX_SLOT *slots, *slotp;
    WT_PAGE *page;
    uint64_t pos, seq;
    int64_t dif;
    uint32_t retries, slot;

    evict = S2C(session)->evict;
    if (!__wt_atomic_load_bool_relaxed(&evict->eviction_dirty_index) ||
      !F_ISSET(ref, WT_REF_FLAG_LEAF) || __wt_atomic_load_ptr_relaxed(&ref->home) == NULL ||
      WT_REF_GET_STATE(ref) != WT_REF_MEM ||
      (page = __wt_atomic_load_ptr_acquire(&ref->page)) == NULL || page->modify == NULL ||
      __wt_atomic_load_uint32_relaxed(&page->dirty_index_slot) != WTI_DIRTY_BP_NONE)
        return (false);
    if ((idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL)
        return (false);
    if (WTI_DIRTY_INDEX_IS_DISAGG(btree) &&
      !__wt_atomic_load_bool_relaxed(&evict->eviction_dirty_index_disagg))
        return (false);
    if (!__evict_dirty_index_ensure_slots(session, idx))
        return (false);
    idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index);
    if (idx == NULL || (slots = __wt_atomic_load_ptr_acquire(&idx->slots)) == NULL)
        return (false);

    pos = __wt_atomic_load_uint64_relaxed(&idx->head);
    retries = 0;
    for (;;) {
        slotp = &slots[(uint32_t)pos & idx->mask];
        seq = __wt_atomic_load_uint64_acquire(&slotp->sequence);
        dif = (int64_t)(seq - pos);
        if (dif == 0) {
            if (__wt_atomic_cas_uint64_relaxed(&idx->head, pos, pos + 1))
                break;
            WT_PAUSE();
            if (++retries >= WTI_DIRTY_INDEX_MAX_RESERVATION_RETRIES)
                return (false);
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
        } else {
            WT_PAUSE();
            if (++retries >= WTI_DIRTY_INDEX_MAX_RESERVATION_RETRIES)
                return (false);
            pos = __wt_atomic_load_uint64_relaxed(&idx->head);
        }
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
 * __evict_dirty_index_scan_clear --
 *     Search the ring for a ref and drop it. The fallback for a ref the page back-pointer does not
 *     lead to; the caller must have exhausted the back-pointer first.
 *
 * Only the live span is searched, which is what keeps this affordable: a split retires every
 *     deleted ref it finds, and those carry no page to follow, so a mostly-empty ring costs almost
 *     nothing instead of a pass over every slot. Read the tail before the head so both races widen
 *     the span rather than narrow it, and cap it at one lap, beyond which every slot has been seen.
 *     The caller's ref cannot be published while this runs --
 *     it is retiring, and the producer rejects any ref that is not in WT_REF_MEM --
 *     so a span captured here cannot miss it.
 *
 * Load before the compare-and-swap: an unconditional compare-and-swap would take every line of the
 *     span exclusive and stall the producers spinning on it. A ref occupies at most one slot, so
 *     stop at the first match.
 */
static void
__evict_dirty_index_scan_clear(WTI_DIRTY_INDEX *idx, WTI_DIRTY_INDEX_SLOT *slots, WT_REF *ref)
{
    uint64_t head, pos, tail;
    uint32_t slot;

    tail = __wt_atomic_load_uint64_acquire(&idx->tail);
    head = __wt_atomic_load_uint64_acquire(&idx->head);
    if (head - tail > idx->capacity)
        head = tail + idx->capacity;

    for (pos = tail; pos != head; ++pos) {
        slot = (uint32_t)pos & idx->mask;
        if (__wt_atomic_load_ptr_acquire(&slots[slot].ref) == ref &&
          __wt_atomic_cas_ptr(&slots[slot].ref, ref, NULL))
            break;
    }
}

/*
 * __wt_dirty_index_block_page --
 *     Block dirty-index producers while a ref is being retired, and drop the ref from the ring. If
 *     a producer owns the page back-pointer but has not published its ref yet, wait for publication
 *     before taking the block.
 *
 * The ref must be gone from the ring by the time this returns: the caller frees it through the
 *     split stash, and a ring slot still naming it outlives that generation and hands the drain a
 *     dangling pointer. The back-pointer names at most one slot, and for a page with several refs
 *     it may not be the slot holding this one, so every path that cannot confirm the removal falls
 *     back to scanning.
 */
void
__wt_dirty_index_block_page(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref, WT_PAGE *page)
{
    WTI_DIRTY_INDEX *idx;
    WTI_DIRTY_INDEX_SLOT *slots, *slotp;
    WT_REF *published_ref;
    uint32_t bp;

    WT_UNUSED(session);
    if ((idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index)) == NULL ||
      (slots = __wt_atomic_load_ptr_acquire(&idx->slots)) == NULL)
        return;

    /* A split can clear ref->page before retiring the ref, leaving no back-pointer to follow. */
    if (page == NULL) {
        __evict_dirty_index_scan_clear(idx, slots, ref);
        return;
    }

    for (;;) {
        bp = __wt_atomic_load_uint32_acquire(&page->dirty_index_slot);
        /* An earlier retirement already took the block, so the back-pointer names nothing. */
        if (bp == WTI_DIRTY_BP_BLOCKED) {
            __evict_dirty_index_scan_clear(idx, slots, ref);
            return;
        }
        if (bp == WTI_DIRTY_BP_NONE) {
            if (__wt_atomic_cas_uint32(
                  &page->dirty_index_slot, WTI_DIRTY_BP_NONE, WTI_DIRTY_BP_BLOCKED))
                /*
                 * No back-pointer means no live ring entry for this page. The one window where a
                 * slot still holds the ref is a drain that has cleared the back-pointer but not yet
                 * emptied the slot, and it holds a hazard pointer throughout, which is what stops
                 * the eviction driving this retirement. No scan needed, which matters because a
                 * page that never entered the ring takes this path.
                 */
                return;
            WT_PAUSE();
            continue;
        }

        /*
         * The back-pointer only ever holds the two sentinels or a one-indexed slot, so an
         * out-of-range value means the ring was rebuilt underneath us; fall back to the scan.
         */
        if (WTI_DIRTY_BP_SLOT(bp) >= idx->capacity) {
            __evict_dirty_index_scan_clear(idx, slots, ref);
            return;
        }
        slotp = &slots[WTI_DIRTY_BP_SLOT(bp)];
        published_ref = __wt_atomic_load_ptr_acquire(&slotp->ref);
        if (published_ref == NULL) {
            WT_PAUSE();
            continue;
        }
        if (!__wt_atomic_cas_uint32(&page->dirty_index_slot, bp, WTI_DIRTY_BP_BLOCKED)) {
            WT_PAUSE();
            continue;
        }

        /*
         * Leave a newer entry alone, but the retiring ref still has to go: it is in some other slot
         * that the back-pointer never named.
         */
        if (!__wt_atomic_cas_ptr(&slotp->ref, ref, NULL))
            __evict_dirty_index_scan_clear(idx, slots, ref);
        return;
    }
}

/*
 * __wt_dirty_index_retire_gen --
 *     Return the split generation a retired ref must be stashed at while the ring is active.
 *
 * Generation safety normally rests on a reader that enters after the page index swap being unable
 *     to reach the old ref at all, which is why the retiring thread can stash at its own split
 *     generation. The ring breaks that: it keeps the ref reachable long after the swap, so a drain
 *     that entered later --
 *     and therefore at a generation the stash does not cover --
 *     can still pull the ref out of a slot and be holding it when the stash is reclaimed. The
 *     current generation covers it instead, because that drain published its own generation, no
 *     larger than this one, before it read the slot.
 *
 * The barrier orders the removal from the ring ahead of this read. That is what bounds the exposure
 *     to drains already in flight: one that starts afterwards cannot find the ref to begin with.
 */
uint64_t
__wt_dirty_index_retire_gen(WT_SESSION_IMPL *session)
{
    WT_FULL_BARRIER();
    return (__wt_gen(session, WT_GEN_SPLIT));
}

/*
 * __wti_dirty_index_unlink_page --
 *     Drop the drain's claim on a popped page, and report whether this call is what dropped it. A
 *     false return means the back-pointer had already moved on: either someone else cleared it, or
 *     a retirement blocked the page while this slot was still named.
 */
bool
__wti_dirty_index_unlink_page(WT_PAGE *page, uint32_t slot)
{
    return (
      __wt_atomic_cas_uint32(&page->dirty_index_slot, WTI_DIRTY_BP_MAKE(slot), WTI_DIRTY_BP_NONE));
}

/*
 * __wti_dirty_index_release_page --
 *     Release a block that was taken while the draining slot still owned the page's back-pointer,
 *     which is the drain's half of the retirement handshake: a retirement that finds the named slot
 *     holding a replacement ref leaves the page blocked, and consuming that ref is what makes the
 *     page available for insertion again.
 *
 * Pass the result of clearing the back-pointer for the drained slot. Clearing it ends this slot's
 *     claim on the page, so a block seen afterwards belongs to a retirement that raced the pop;
 *     that block has to outlive the drain until the retiring ref is discarded or the split unblocks
 *     it, and releasing it here would let a producer publish a ref on its way out.
 */
void
__wti_dirty_index_release_page(WT_PAGE *page, bool cleared)
{
    if (!cleared)
        (void)__wt_atomic_cas_uint32(
          &page->dirty_index_slot, WTI_DIRTY_BP_BLOCKED, WTI_DIRTY_BP_NONE);
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
 *     safe to call more than once for the same page: a second call after the first has already
 *     cleared the entry finds nothing to do. Split retirement uses __wt_dirty_index_block_page
 *     instead so the page cannot acquire a new entry between cleanup and the ref state transition.
 *
 * The page is about to be freed, so no slot may be left naming this ref. As with retirement, the
 *     back-pointer leads to at most one slot and need not be the one holding this ref, so the paths
 *     that cannot confirm the removal fall back to searching the ring.
 */
void
__wt_dirty_index_clear_page(WT_SESSION_IMPL *session, WT_BTREE *btree, WT_REF *ref, WT_PAGE *page)
{
    WTI_DIRTY_INDEX *idx;
    WTI_DIRTY_INDEX_SLOT *slots, *slotp;
    uint32_t bp;

    /* Check the page's own back-pointer first: zero means it never entered the ring. */
    if (page == NULL ||
      (bp = __wt_atomic_load_uint32_acquire(&page->dirty_index_slot)) == WTI_DIRTY_BP_NONE)
        return;
    idx = __wt_atomic_load_ptr_acquire(&btree->dirty_index);
    if (idx == NULL || (slots = __wt_atomic_load_ptr_acquire(&idx->slots)) == NULL)
        return;

    /* A retirement holds the block; the back-pointer no longer names a slot to follow. */
    if (bp == WTI_DIRTY_BP_BLOCKED) {
        __evict_dirty_index_scan_clear(idx, slots, ref);
        return;
    }
    WT_ASSERT(session, WTI_DIRTY_BP_SLOT(bp) < idx->capacity);
    if (WTI_DIRTY_BP_SLOT(bp) >= idx->capacity) {
        __evict_dirty_index_scan_clear(idx, slots, ref);
        return;
    }

    slotp = &slots[WTI_DIRTY_BP_SLOT(bp)];
    /* Only clear the page back-pointer if this ref still owns the slot. */
    if (__wt_atomic_cas_ptr(&slotp->ref, ref, NULL))
        (void)__wt_atomic_cas_uint32(&page->dirty_index_slot, bp, WTI_DIRTY_BP_NONE);
    else
        __evict_dirty_index_scan_clear(idx, slots, ref);
}
