/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * Tuning constants: I hesitate to call this tuning, but we want to review some number of pages from
 * each file's in-memory tree for each page we evict.
 */
#define WTI_EVICT_MAX_TREES WT_THOUSAND /* Maximum walk points */
#define WTI_EVICT_WALK_BASE 300         /* Pages tracked across file visits */
#define WTI_EVICT_WALK_INCR 100         /* Pages added each walk */

/* True if there are eviction worker threads beyond the server thread itself. */
#define WT_EVICT_HAS_WORKERS(s) \
    (__wt_atomic_load_uint32_relaxed(&S2C(s)->evict_config.threads.current_threads) > 1)

/*
 * WTI_EVICT_ENTRY --
 *	Encapsulation of an eviction candidate.
 */
struct __wti_evict_entry {
    WT_BTREE *btree; /* Enclosing btree object */
    WT_REF *ref;     /* Page to flush/evict */
    uint64_t score;  /* Relative eviction priority */
};

#define WTI_EVICT_QUEUE_MAX 3    /* Two ordinary queues plus urgent */
#define WTI_EVICT_URGENT_QUEUE 2 /* Urgent queue index */

/*
 * WTI_DIRTY_INDEX --
 *     Per-btree multi-producer / single-consumer ring of WT_REF pointers fed by cursor modifies.
 *     The eviction walker drains entries in FIFO order to supply candidates without re-walking the
 *     tree. Capacity is a power of two so the consumer can mask-index into the slot array.
 *
 *     Layout (head and tail are free-running 64-bit counters; slot index = counter & mask):
 *
 *               tail (consumer)                 head (producers)
 *                 v                               v
 *       slots[]:  [ . ][ R ][ R ][ R ][ R ][ R ][ . ][ . ] ...    (R = live ref, . = empty)
 *                 `------ drained FIFO ------'`--- live ---'
 *
 *     Producer: fetch-add head to reserve a unique slot, release-store the ref into the slot, then
 *     CAS the page's one-indexed back-pointer (dirty_index_slot) so a page is not inserted twice.
 *     Consumer (single drain thread per btree visit): acquire-load head, walk tail..head, take each
 *     ref, then release-store the advanced tail.
 *
 *     A reserved slot can be abandoned (the producer bails after the fetch-add on saturation, or
 *     loses the back-pointer CAS); the consumer sees a NULL slot and skips it. A lost slot only
 *     costs a missed fast-path candidate -- the eviction walker stays the source of truth, so the
 *     ring is best-effort, never authoritative.
 */
#define WTI_DIRTY_INDEX_MIN_CAPACITY 16384u
#define WTI_DIRTY_INDEX_MAX_CAPACITY 262144u

/*
 * The page back-pointer (WT_PAGE.dirty_index_slot, uint32) packs the owning ring's generation above
 * the one-indexed slot. MAX_CAPACITY is 2^18, so a one-indexed slot needs 19 bits, leaving 13 bits
 * of generation. With auto-grow disabled the generation is always 0, so the encoding degrades to a
 * bare slot+1 -- identical to the single-ring layout. Generation is only used to pick the right
 * ring (active vs the one draining ring) and to guard a back-pointer clear against a stale ring.
 */
#define WTI_DIRTY_BP_SLOT_BITS 19u
#define WTI_DIRTY_BP_SLOT_MASK ((1u << WTI_DIRTY_BP_SLOT_BITS) - 1u)
#define WTI_DIRTY_BP_MAKE(gen, slot) \
    (((uint32_t)(gen) << WTI_DIRTY_BP_SLOT_BITS) | ((uint32_t)(slot) + 1u))
#define WTI_DIRTY_BP_GEN(bp) ((bp) >> WTI_DIRTY_BP_SLOT_BITS)
#define WTI_DIRTY_BP_SLOT(bp) (((bp) & WTI_DIRTY_BP_SLOT_MASK) - 1u)

/*
 * Adaptive drain scheduling. After EMPTY_THRESHOLD consecutive empty drains the per-btree drain
 * parks (walker-only) and re-probes once every PROBE_INTERVAL passes. Separately, a precise
 * checkpoint cannot evict a dirty page whose commit timestamp is ahead of the pinned stable
 * timestamp; when a ring fills with such pages the drain captures the median blocked commit
 * timestamp and the walker skips the drain until the stable timestamp crosses it (see
 * WT_BTREE.drain_stable_block_ts). The drain then stops re-examining and re-inserting pages it
 * cannot queue while their working set sits ahead of stable; the walker still evicts any page that
 * does fall below stable in the meantime.
 */
#define WTI_DRAIN_EMPTY_THRESHOLD 8u
#define WTI_DRAIN_PROBE_INTERVAL 32u

/*
 * Clear the saturation hint once the ring drains to this fraction of capacity (capacity >> SHIFT).
 * Set at full, clear at half: the hysteresis bounds how often the hint flips.
 */
#define WTI_DIRTY_INDEX_SATURATE_CLEAR_SHIFT 1u

/*
 * Auto-grow trigger: grow only when the ring is found saturated on this many consecutive drain
 * passes, so a transient burst that fills the ring once does not provoke a grow -- only a ring that
 * the drain cannot keep empty.
 */
#define WTI_DIRTY_INDEX_GROW_FULL_THRESHOLD 4u

struct __wti_dirty_index {
    WT_REF **slots;      /* Circular buffer of ref pointers */
    uint32_t capacity;   /* Slot count (power of two) */
    uint32_t mask;       /* capacity - 1 */
    uint32_t generation; /* Identifies this ring in page back-pointers; 0 unless grown */
    wt_shared WTI_DIRTY_INDEX
      *next_old; /* Next ring on the btree's retired list (freed at close) */

    wt_shared uint64_t head; /* Next slot to be filled (monotonic, fetch-add by producers) */
    wt_shared uint64_t tail; /* Next slot to drain (monotonic, advanced by the consumer) */

    /*
     * Saturation hint: set by a producer that finds the ring full; cleared by the consumer once the
     * ring drains below half. Advisory only -- the head/tail overflow check is the correctness
     * boundary, so relaxed ordering suffices.
     */
    wt_shared uint8_t saturated;
};

/*
 * WTI_EVICT_QUEUE --
 *	Encapsulation of an eviction candidate queue.
 */
struct __wti_evict_queue {
    WT_SPINLOCK evict_lock;                /* Eviction LRU queue */
    WTI_EVICT_ENTRY *evict_queue;          /* LRU pages being tracked */
    WTI_EVICT_ENTRY *evict_current;        /* LRU current page to be evicted */
    uint32_t evict_candidates;             /* LRU list pages to evict */
    uint32_t evict_entries;                /* LRU entries in the queue */
    wt_shared volatile uint32_t evict_max; /* LRU maximum eviction slot used */
};

#define WTI_WITH_PASS_LOCK(session, op)                                                  \
    do {                                                                                 \
        WT_WITH_LOCK_WAIT(session, &evict->evict_pass_lock, WT_SESSION_LOCKED_PASS, op); \
    } while (0)

/* DO NOT EDIT: automatically built by prototypes.py: BEGIN */

extern bool __wti_evict_push_candidate(WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue,
  WTI_EVICT_ENTRY *evict_entry, WT_REF *ref) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_evict_app_assist_worker(WT_SESSION_IMPL *session, bool busy, bool readonly,
  bool interruptible) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_evict_clear_all_walks_and_saved_tree(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_evict_clear_walk_and_saved_tree_if_current_locked(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_evict_lock_handle_list(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_evict_lru_pages(WT_SESSION_IMPL *session, bool is_server)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_evict_lru_walk(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_evict_page(WT_SESSION_IMPL *session, bool is_server)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_evict_walk(WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_evict_queue_clear_page(WT_SESSION_IMPL *session, WT_REF *ref);
extern void __wti_evict_queue_clear_page_locked(
  WT_SESSION_IMPL *session, WT_REF *ref, bool exclude_urgent);
extern void __wti_evict_set_saved_walk_tree(WT_SESSION_IMPL *session, WT_DATA_HANDLE *new_dhandle);
static WT_INLINE bool __wti_evict_hs_dirty(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_readgen_is_soon_or_wont_need(uint64_t *readgen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wti_evict_updates_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE double __wti_evict_dirty_target(WT_EVICT *evict)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE void __wti_evict_read_gen_bump(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wti_evict_read_gen_new(WT_SESSION_IMPL *session, WT_PAGE *page);

#ifdef HAVE_UNITTEST

#endif

/* DO NOT EDIT: automatically built by prototypes.py: END */
