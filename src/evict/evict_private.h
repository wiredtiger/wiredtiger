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

/*
 * The walk period doubles on every unproductive walk of a tree, so saturation means the tree has
 * been unproductive for many consecutive walks.
 */
#define WTI_EVICT_WALK_PERIOD_MAX 100 /* Ceiling on walks skipped for one tree */

/*
 * Cap the wait for callers that pin no transaction state: the stuck-cache escape cannot roll them
 * back, so an unbounded wait ends only when eviction succeeds, and blocking them can stop the
 * application from advancing the timestamps that would make the cache reclaimable.
 *
 * Used only when the caller set no operation timeout of its own. Keep it generous: a shorter cap
 * cuts these callers out of the assist while the cache is still draining, and they are the only
 * application threads that help with nothing pinned, so the work is not made up elsewhere.
 */
#define WTI_EVICT_BOUNDED_WAIT_US (60 * WT_MILLION)

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
 *     Per-btree ring of WT_REF pointers fed by cursor modifies. Producers reserve positions by
 *     atomically advancing head, then publish through per-slot sequence counters. Capacity is a
 *     power of two so the consumer can mask-index into the slot array.
 *
 *     Layout (head and tail are monotonically increasing counters; slot index = counter & mask):
 *
 *               tail (consumer)                 head (producers)
 *                 v                               v
 *       slots[]:  [ . ][ R ][ R ][ R ][ R ][ R ][ . ][ . ] ...    (R = live ref, . = empty)
 *                 `------ drained FIFO ------'`--- live ---'
 *
 *     A producer that finds the bounded ring full abandons the fast path. The eviction walker
 *     remains the source of truth, so the ring is best-effort, never authoritative.
 */
#define WTI_DIRTY_INDEX_MIN_CAPACITY (16 * 1024u)
#define WTI_DIRTY_INDEX_MAX_CAPACITY (256 * 1024u)
#define WTI_DIRTY_INDEX_MAX_RESERVATION_RETRIES 8u

/*
 * The page back-pointer (WT_PAGE.dirty_index_slot, uint32) stores the one-indexed slot;
 * WTI_DIRTY_BP_MAKE encodes a zero-based slot index into that back-pointer value, and
 * WTI_DIRTY_BP_SLOT recovers the slot index from it. Producers and page teardown coordinate
 * ownership of the back-pointer with atomic compare-and-swap operations.
 *
 * Two sentinels mean the page holds no slot, and the difference between them is what lets a
 * retiring ref skip the ring search. NONE is the calloc-zeroed initial value and promises the page
 * was never inserted, so no slot can name any of its refs. CLEARED says a claim was released, which
 * promises nothing: slots are per ref and a page may have refs in slots the back-pointer never
 * named, so only a search can rule that out. Everything that gives up a claim stores CLEARED rather
 * than NONE; a page reverts to NONE only by being freed and its memory reused.
 */
#define WTI_DIRTY_BP_NONE 0u
#define WTI_DIRTY_BP_CLEARED (UINT32_MAX - 1u)
#define WTI_DIRTY_BP_BLOCKED UINT32_MAX
#define WTI_DIRTY_BP_MAKE(slot) ((uint32_t)(slot) + 1u)
#define WTI_DIRTY_BP_SLOT(bp) ((bp) - 1u)
/* True when no producer or retirement holds the back-pointer, so a producer may claim it. */
#define WTI_DIRTY_BP_IS_FREE(bp) ((bp) == WTI_DIRTY_BP_NONE || (bp) == WTI_DIRTY_BP_CLEARED)
#define WTI_DIRTY_INDEX_IS_DISAGG(btree) \
    F_ISSET((btree), WT_BTREE_DISAGGREGATED | WT_BTREE_GARBAGE_COLLECT)

/*
 * Adaptive drain scheduling. After EMPTY_THRESHOLD consecutive empty drains the per-btree drain
 * parks (walker-only) and re-probes once every PROBE_INTERVAL passes. Separately, a precise
 * checkpoint cannot evict a dirty page whose commit timestamp is ahead of the pinned stable
 * timestamp; when a ring fills with such pages the drain captures the midpoint of the blocked
 * commit timestamp range and the walker skips the drain until the stable timestamp crosses it (see
 * WT_BTREE.drain_stable_block_ts). The drain then stops re-examining and re-inserting pages it
 * cannot queue while their working set sits ahead of stable; the walker still evicts any page that
 * does fall below stable in the meantime.
 */
#define WTI_DRAIN_EMPTY_THRESHOLD 8u
#define WTI_DRAIN_PROBE_INTERVAL 32u

/*
 * The ring is leaf-only, so a drain that fills a tree's whole budget leaves the walker no slots and
 * the internal tier goes stale. After this many consecutive budget-filling passes the walker gets
 * one to itself.
 */
#define WTI_DRAIN_FILLED_SKIP_MAX 32u

typedef struct {
    wt_shared WT_REF *ref;
    wt_shared uint64_t sequence;
} WTI_DIRTY_INDEX_SLOT;

/*
 * head is CAS-hot for every producer, tail is written only by the single-consumer drain, and
 * slots/capacity/mask are read by every producer on every insert. The padding keeps those three
 * groups on separate cache lines so the drain's tail update can't bounce the lines producers are
 * spinning on or reading. Two padding arrays, not one: merging them would put head and tail back on
 * the same line, the exact false sharing this is avoiding.
 */
struct __wti_dirty_index {
    wt_shared uint64_t head; /* Next slot to reserve */
    uint8_t head_padding[WT_CACHE_LINE_ALIGNMENT - sizeof(uint64_t)];
    wt_shared uint64_t tail; /* Next slot to drain */
    uint8_t tail_padding[WT_CACHE_LINE_ALIGNMENT - sizeof(uint64_t)];
    WTI_DIRTY_INDEX_SLOT *slots; /* Circular buffer of published ref pointers */
    uint32_t capacity;           /* Slot count (power of two) */
    uint32_t mask;               /* capacity - 1 */
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

extern bool __wti_dirty_index_unlink_page(WT_PAGE *page, uint32_t slot)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wti_evict_push_candidate(WT_SESSION_IMPL *session, WTI_EVICT_QUEUE *queue,
  WTI_EVICT_ENTRY *evict_entry, WT_REF *ref) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_evict_app_assist_worker(WT_SESSION_IMPL *session, bool busy, bool readonly,
  bool interruptible, bool bounded) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
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
extern void __wti_dirty_index_release_page(WT_PAGE *page, bool cleared);
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
