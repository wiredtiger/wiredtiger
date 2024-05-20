/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * Helper: in order to read without any calls to eviction, we have to ignore the cache size and
 * disable splits.
 */
#include "hardware.h"
#define WT_READ_NO_EVICT (WT_READ_IGNORE_CACHE_SIZE | WT_READ_NO_SPLIT)

/*
 * Tuning constants: I hesitate to call this tuning, but we want to review some number of pages from
 * each file's in-memory tree for each page we evict.
 */
#define WT_EVICT_MAX_TREES WT_THOUSAND /* Maximum walk points */
#define WT_EVICT_WALK_BASE 300         /* Pages tracked across file visits */
#define WT_EVICT_WALK_INCR 100         /* Pages added each walk */

/*
 * WT_EVICT_ENTRY --
 *	Encapsulation of an eviction candidate.
 */
struct __wt_evict_entry {
    WT_BTREE *btree; /* Enclosing btree object */
    WT_REF *ref;     /* Page to flush/evict */
    uint64_t score;  /* Relative eviction priority */
};

#define WT_EVICT_QUEUE_MAX 3    /* Two ordinary queues plus urgent */
#define WT_EVICT_URGENT_QUEUE 2 /* Urgent queue index */

/*
 * WT_EVICT_QUEUE --
 *	Encapsulation of an eviction candidate queue.
 */
struct __wt_evict_queue {
    WT_SPINLOCK evict_lock;                /* Eviction LRU queue */
    WT_EVICT_ENTRY *evict_queue;           /* LRU pages being tracked */
    WT_EVICT_ENTRY *evict_current;         /* LRU current page to be evicted */
    uint32_t evict_candidates;             /* LRU list pages to evict */
    uint32_t evict_entries;                /* LRU entries in the queue */
    wt_shared volatile uint32_t evict_max; /* LRU maximum eviction slot used */
};

/* Cache operations. */
typedef enum __wt_cache_op {
    WT_SYNC_CHECKPOINT,
    WT_SYNC_CLOSE,
    WT_SYNC_DISCARD,
    WT_SYNC_WRITE_LEAVES
} WT_CACHE_OP;

#define WT_HS_FILE_MIN (100 * WT_MEGABYTE)

/*
 * WiredTiger cache structure.
 */
struct __wt_cache {
    /*
     * Different threads read/write pages to/from the cache and create pages in the cache, so we
     * cannot know precisely how much memory is in use at any specific time. However, even though
     * the values don't have to be exact, they can't be garbage, we track what comes in and what
     * goes out and calculate the difference as needed.
     */

    wt_shared uint64_t bytes_dirty_intl; /* Bytes/pages currently dirty */
    wt_shared uint64_t bytes_dirty_leaf;
    wt_shared uint64_t bytes_dirty_total;
    wt_shared uint64_t bytes_evict;      /* Bytes/pages discarded by eviction */
    wt_shared uint64_t bytes_image_intl; /* Bytes of disk images (internal) */
    wt_shared uint64_t bytes_image_leaf; /* Bytes of disk images (leaf) */
    wt_shared uint64_t bytes_inmem;      /* Bytes/pages in memory */
    wt_shared uint64_t bytes_internal;   /* Bytes of internal pages */
    wt_shared uint64_t bytes_read;       /* Bytes read into memory */
    wt_shared uint64_t bytes_updates;    /* Bytes of updates to pages */
    wt_shared uint64_t bytes_written;

    /*
     * History store cache usage. TODO: The values for these variables are cached and potentially
     * outdated.
     */
    wt_shared uint64_t bytes_hs; /* History store bytes inmem */
    uint64_t bytes_hs_dirty;     /* History store bytes inmem dirty */

    wt_shared uint64_t pages_dirty_intl;
    wt_shared uint64_t pages_dirty_leaf;
    wt_shared uint64_t pages_evicted;
    wt_shared uint64_t pages_inmem;

    wt_shared volatile uint64_t eviction_progress; /* Eviction progress count */
    uint64_t last_eviction_progress;               /* Tracked eviction progress */

    uint64_t app_waits;  /* User threads waited for cache */
    uint64_t app_evicts; /* Pages evicted by user threads */

    uint64_t evict_max_page_size;    /* Largest page seen at eviction */
    uint64_t evict_max_ms;           /* Longest milliseconds spent at a single eviction */
    uint64_t reentry_hs_eviction_ms; /* Total milliseconds spent inside a nested eviction */
    struct timespec stuck_time;      /* Stuck time */

    /*
     * Read information.
     */
    uint64_t read_gen;        /* Current page read generation */
    uint64_t read_gen_oldest; /* Oldest read generation the eviction
                               * server saw in its last queue load */
    uint64_t evict_pass_gen;  /* Number of eviction passes */

    /*
     * Eviction thread information.
     */
    WT_CONDVAR *evict_cond;      /* Eviction server condition */
    WT_SPINLOCK evict_walk_lock; /* Eviction walk location */

    /*
     * Eviction threshold percentages use double type to allow for specifying percentages less than
     * one.
     */
    wt_shared double eviction_dirty_target;  /* Percent to allow dirty */
    wt_shared double eviction_dirty_trigger; /* Percent to trigger dirty eviction */
    double eviction_trigger;                 /* Percent to trigger eviction */
    double eviction_target;                  /* Percent to end eviction */
    double eviction_updates_target;          /* Percent to allow for updates */
    double eviction_updates_trigger;         /* Percent of updates to trigger eviction */

    double eviction_checkpoint_target; /* Percent to reduce dirty to during checkpoint scrubs */
    wt_shared double eviction_scrub_target; /* Current scrub target */

    u_int overhead_pct;              /* Cache percent adjustment */
    uint64_t cache_max_wait_us;      /* Maximum time an operation waits for space in cache */
    uint64_t cache_stuck_timeout_ms; /* Maximum time the cache can be stuck for in diagnostic mode
                                        before timing out */

    /*
     * Eviction thread tuning information.
     */
    uint32_t evict_tune_datapts_needed;          /* Data needed to tune */
    struct timespec evict_tune_last_action_time; /* Time of last action */
    struct timespec evict_tune_last_time;        /* Time of last check */
    uint32_t evict_tune_num_points;              /* Number of values tried */
    uint64_t evict_tune_progress_last;           /* Progress counter */
    uint64_t evict_tune_progress_rate_max;       /* Max progress rate */
    bool evict_tune_stable;                      /* Are we stable? */
    uint32_t evict_tune_workers_best;            /* Best performing value */

    /*
     * Pass interrupt counter.
     */
    wt_shared volatile uint32_t pass_intr; /* Interrupt eviction pass. */

    /*
     * LRU eviction list information.
     */
    WT_SPINLOCK evict_pass_lock;   /* Eviction pass lock */
    WT_SESSION_IMPL *walk_session; /* Eviction pass session */
    WT_DATA_HANDLE *walk_tree;     /* LRU walk current tree */

    WT_SPINLOCK evict_queue_lock; /* Eviction current queue lock */
    WT_EVICT_QUEUE evict_queues[WT_EVICT_QUEUE_MAX];
    WT_EVICT_QUEUE *evict_current_queue; /* LRU current queue in use */
    WT_EVICT_QUEUE *evict_fill_queue;    /* LRU next queue to fill.
                                            This is usually the same as the
                                            "other" queue but under heavy
                                            load the eviction server will
                                            start filling the current queue
                                            before it switches. */
    WT_EVICT_QUEUE *evict_other_queue;   /* LRU queue not in use */
    WT_EVICT_QUEUE *evict_urgent_queue;  /* LRU urgent queue */
    uint32_t evict_slots;                /* LRU list eviction slots */

#define WT_LRU_TRACE(STR) \
    do { \
        char ___thread_str[50]; \
        WT_UNUSED(__wt_thread_str(___thread_str, sizeof(___thread_str))); \
        ___thread_str[sizeof(___thread_str)-1] = 0; \
        printf("    %s %s %s %s %s:%d\n", ___thread_str, session->name, STR, __FUNCTION__, __FILE__, __LINE__); fflush(stdout); \
    } while (0)
#define WT_LRU_TRACEF(STR, FMT, ...) \
    do { \
        char _thread_str[50]; \
        WT_UNUSED(__wt_thread_str(_thread_str, sizeof(_thread_str))); \
        _thread_str[sizeof(_thread_str)-1] = 0; \
        printf("    %s %s %s %s %s:%d " FMT "\n", _thread_str, session->name, STR, __FUNCTION__, __FILE__, __LINE__, __VA_ARGS__); fflush(stdout); \
    } while (0)

#define __WT_MACRO_CONCAT(a, b) a##b
// #define __WT_ATOMIC_SUFFIX(a, b) __WT_MACRO_CONCAT(a, b)

#ifdef __cplusplus
#define WT_LRU_TRACEEF(STR, elm, field)
#else

#define WT_LRU_TRACEEF(STR, elm, field) \
    WT_LRU_TRACE(STR " (pre)"); \
    printf(" ...1: %p", (void*)elm); fflush(stdout); \
    printf(" ...2: %p", (void*)S2C(session)->cache->field.tqh_first); fflush(stdout); \
    printf(" ...3: %p", (void*)S2C(session)->cache->field.tqh_last); fflush(stdout); \
    printf(" ...4: %p", !S2C(session)->cache->field.tqh_last ? NULL : (void*)*S2C(session)->cache->field.tqh_last); fflush(stdout); \
    printf(" ...5: %p", !S2C(session)->cache->field.tqh_last || !*S2C(session)->cache->field.tqh_last ? NULL : (void*)TAILQ_LAST(&S2C(session)->cache->field, __lru_##field)); fflush(stdout); \
    printf(" ...6: %p", (void*)(elm)->field.tqe_next); fflush(stdout); \
    printf(" ...7: %p", (void*)(elm)->field.tqe_prev); fflush(stdout); \
    printf(" ...8: %p", !(elm)->field.tqe_prev ? NULL : (void*)*(elm)->field.tqe_prev); fflush(stdout); \
    printf(" ...9: %p", !(elm)->field.tqe_prev || !*(elm)->field.tqe_prev ? NULL : (void*)TAILQ_PREV(elm, __lru_##field, field)); fflush(stdout); \
    printf(" ...10: %s\n", __wt_ref_is_root(elm) ? "is_root" : "is_not_root"); fflush(stdout); \
    WT_LRU_TRACEF(STR, "ref=%p CACHE %s %p %p %p(%p) ELEMENT %p %p %p(%p)", (void*)elm, #field, \
        (void*)S2C(session)->cache->field.tqh_first, (void*)S2C(session)->cache->field.tqh_last, \
            !S2C(session)->cache->field.tqh_last ? NULL : (void*)*S2C(session)->cache->field.tqh_last, !S2C(session)->cache->field.tqh_last || !*S2C(session)->cache->field.tqh_last ? NULL : (void*)TAILQ_LAST(&S2C(session)->cache->field, __lru_##field), \
        (void*)(elm)->field.tqe_next, (void*)(elm)->field.tqe_prev, \
            !(elm)->field.tqe_prev ? NULL : (void*)*(elm)->field.tqe_prev, !(elm)->field.tqe_prev || !*(elm)->field.tqe_prev ? NULL : (void*)TAILQ_PREV(elm, __lru_##field, field))
#endif

// #define WT_CACHE_LOCK_TYPE uint32_t
// #define WT_CACHE_LOCK_ATOMIC_SUFFIX 32
#define WT_CACHE_LOCK_TYPE WT_SPINLOCK

#define __GUARD1 0xA1A2A3A4A5A6A7A8
#define __GUARD2 0xB1B2B3B4B5B6B7B8
#define __GUARD3 0xC1C2C3C4C5C6C7C8
#define __GUARD1_CLEAR 0x5152535455565758
#define __GUARD2_CLEAR 0x6162636465666768
#define __GUARD3_CLEAR 0x7172737475767778
#define INIT_GUARD(elm, field) \
    do { \
        WT_LRU_TRACEEF("INIT_GUARD", elm, field); \
        elm->field##__guard1 = __GUARD1; \
        elm->field##__guard2 = __GUARD2; \
        elm->field##__guard3 = __GUARD3; \
    } while (0)
#define CLEAR_GUARD(elm, field) \
    do { \
        WT_LRU_TRACEEF("CLEAR_GUARD", elm, field); \
        elm->field##__guard1 = __GUARD1_CLEAR; \
        elm->field##__guard2 = __GUARD2_CLEAR; \
        elm->field##__guard3 = __GUARD3_CLEAR; \
    } while (0)
#define CHECK_GUARD(str, elm, field) \
    do { \
        WT_ASSERT_ALWAYS(session, (bool)#str && elm != NULL, #str " : " #elm " is NULL"); \
        WT_ASSERT_ALWAYS(session, (bool)#str && (S2C(session)->cache->field.tqh_last != NULL || (S2C(session)->cache->field.tqh_last == NULL && S2C(session)->cache->field.tqh_first == NULL)), #str " : " #elm " cache.tqh_last is NULL"); \
        WT_ASSERT_ALWAYS(session, (bool)#str && ((elm)->field.tqe_prev != NULL || ((elm)->field.tqe_prev == NULL && (elm)->field.tqe_next == NULL)), #str " : " #elm ".tqe_prev is NULL"); \
        /* Don't check root page (__wt_root_ref_init) */ \
        /*if (!__wt_ref_is_root(elm)) {*/ \
            if (elm->field##__guard1 == 0 && elm->field##__guard2 == 0 && elm->field##__guard3 == 0) { \
                INIT_GUARD(elm, field); \
            } else { \
                WT_ASSERT_ALWAYS(session, (bool)#str && elm->field##__guard1 == __GUARD1, #str " : guard1 is bad"); \
                WT_ASSERT_ALWAYS(session, (bool)#str && elm->field##__guard2 == __GUARD2, #str " : guard2 is bad"); \
                WT_ASSERT_ALWAYS(session, (bool)#str && elm->field##__guard3 == __GUARD3, #str " : guard3 is bad"); \
            } \
            WT_LRU_TRACEEF(#str " - CHECK_GUARD", elm, field); \
            /* MEM CHK */ \
        /*}*/ \
    } while (0)

            // do { \
            //     struct xxx { \
            //         uint64_t guard1,guard11; \
            //         uint64_t size; \
            //         uint64_t guard2; \
            //     } *pre = (struct xxx *)((char *)elm - sizeof(struct xxx)); \
            //     WT_ASSERT_ALWAYS(session, pre->guard1 == 0xa0a1a2a3a4a5a6a7, "memory guard1=0x%" PRIu64 "x", pre->guard1); \
            //     WT_ASSERT_ALWAYS(session, pre->guard11 == 0xa0a1a2a3a4a5a6a7, "memory guard11=0x%" PRIu64 "x", pre->guard11); \
            //     WT_ASSERT_ALWAYS(session, pre->guard2 == 0xa8a9aaabacadaeaf, "memory guard1=0x%" PRIu64 "x", pre->guard2); \
            //     WT_ASSERT_ALWAYS(session, pre->size == sizeof(WT_REF), "memory size=%" PRIu64 "d", pre->size); \
            // } while (0); \
            //

#define WT_LRU_HEAD(field) \
    WT_CACHE_LOCK_TYPE field##_lock; \
    TAILQ_HEAD(__lru_##field, __wt_ref) field

    WT_LRU_HEAD(lru_all);
    // WT_LRU_HEAD(lru_dirty);
    // WT_LRU_HEAD(lru_updated);
    // WT_LRU_HEAD(lru_forced_eviction);
    // WT_LRU_HEAD(lru_evict_soon);

#define __WT_LRU_UPDATE_MAX_FREQUENCY_NS   10000000000

#define WT_CACHE_LRU_INIT(field) \
    do { \
        TAILQ_INIT(&S2C(session)->cache->field); \
        /* S2C(session)->cache->field##_lock = 0; */ \
        WT_RET(__wt_spin_init(session, &S2C(session)->cache->field##_lock, "LRU queue lock: " # field)); \
    } while (0)
#define WT_CACHE_LRU_DESTROY(field) \
    do { \
        __wt_spin_destroy(session, &S2C(session)->cache->field##_lock); \
    } while (0)
#define WT_CACHE_LRU_INIT_ALL() \
    do { \
        WT_CACHE_LRU_INIT(lru_all); \
        /* WT_CACHE_LRU_INIT(lru_dirty); */ \
        /* WT_CACHE_LRU_INIT(lru_updated); */ \
        /* WT_CACHE_LRU_INIT(lru_forced_eviction); */ \
        /* WT_CACHE_LRU_INIT(lru_evict_soon); */ \
    } while (0)
#define WT_CACHE_LRU_DESTROY_ALL() \
    do { \
        WT_CACHE_LRU_DESTROY(lru_all); \
        /* WT_CACHE_LRU_DESTROY(lru_dirty); */ \
        /* WT_CACHE_LRU_DESTROY(lru_updated); */ \
        /* WT_CACHE_LRU_DESTROY(lru_forced_eviction); */ \
        /* WT_CACHE_LRU_DESTROY(lru_evict_soon); */ \
    } while (0)
#define WT_CACHE_LRU_REMOVE_FROM_ALL(ref) \
    do { \
        WT_LRU_REMOVE_AND_CLEAR(ref, lru_all); \
        /* WT_LRU_REMOVE_AND_CLEAR(ref, lru_dirty); */ \
        /* WT_LRU_REMOVE_AND_CLEAR(ref, lru_updated); */ \
        /* WT_LRU_REMOVE_AND_CLEAR(ref, lru_forced_eviction); */ \
        /* WT_LRU_REMOVE_AND_CLEAR(ref, lru_evict_soon); */ \
    } while (0)

// #define __WT_LRU_LOCK(field) while (__WT_ATOMIC_SUFFIX(__wt_atomic_cas, WT_CACHE_LOCK_ATOMIC_SUFFIX)(&S2C(session)->cache->field##_lock, 0, 1) != 0) /*__wt_yield()*/
// #define __WT_LRU_UNLOCK(field) __WT_ATOMIC_SUFFIX(__wt_atomic_store, WT_CACHE_LOCK_ATOMIC_SUFFIX)(&S2C(session)->cache->field##_lock, 0)
#define __WT_LRU_LOCK(field) __wt_spin_lock(session, &S2C(session)->cache->field##_lock); printf("LOCK %s %s %s %s:%d\n", session->name, #field, __FUNCTION__, __FILE__, __LINE__); fflush(stdout)
#define __WT_LRU_UNLOCK(field) __wt_spin_unlock(session, &S2C(session)->cache->field##_lock); printf("UNLOCK %s %s %s %s:%d\n", session->name, #field, __FUNCTION__, __FILE__, __LINE__); fflush(stdout)

// #define WT_LRU_IS_IN_LIST(elm, field) ((elm)->field##_t == 0)
#define WT_LRU_IS_IN_LIST(elm, field) ((elm)->field.tqe_next != NULL || (elm)->field.tqe_prev != NULL)
#define WT_LRU_REMOVE_AND_CLEAR_NOLOCK(elm, field) \
    do { \
        TAILQ_REMOVE(&S2C(session)->cache->field, elm, field); \
        (elm)->field.tqe_next = NULL; \
        (elm)->field.tqe_prev = NULL; /* barrier */ \
        __wt_atomic_store64(&(elm)->field##_t, 0); \
    } while (0)
#define WT_LRU_REMOVE_AND_CLEAR(elm, field) \
    if (!__wt_ref_is_root(elm)) { \
        __WT_LRU_LOCK(field); \
        CHECK_GUARD(WT_LRU_REMOVE_AND_CLEAR2, elm, field); \
        WT_LRU_TRACEEF("WT_LRU_REMOVE_AND_CLEAR +", elm, field); \
        WT_ASSERT(session, (elm->field.tqe_prev == NULL && elm->field.tqe_next ==  NULL) || (elm->field.tqe_prev != NULL)); \
        WT_ASSERT(session, S2C(session)->cache->field.tqh_last != NULL); \
        CHECK_GUARD(WT_LRU_REMOVE_AND_CLEAR3, elm, field); \
        /* WT_ACQUIRE_BARRIER(); */ \
        if (WT_LRU_IS_IN_LIST(elm, field)) \
            WT_LRU_REMOVE_AND_CLEAR_NOLOCK(elm, field); \
        CHECK_GUARD(WT_LRU_REMOVE_AND_CLEAR4, elm, field); \
        /* WT_RELEASE_BARRIER(); */ \
        WT_LRU_TRACEEF("WT_LRU_REMOVE_AND_CLEAR -", elm, field); \
        __WT_LRU_UNLOCK(field); \
    } else {}
// #define WT_LRU_REMOVE(elm, field) \
//     do { \
//         __WT_LRU_LOCK(field); \
//         /* WT_ACQUIRE_BARRIER(); */ \
//         if (WT_LRU_IS_IN_LIST(elm, field)) \
//             TAILQ_REMOVE(&S2C(session)->cache->field, elm, field); \
//         __wt_atomic_store64(&elm->field##_t, 0); \
//         /* WT_RELEASE_BARRIER(); */ \
//         __WT_LRU_UNLOCK(field); \
//     } while (0)
/* (re-)insert the page into the queue */
// #define WT_LRU_UPDATE(elm, field)
#define WT_LRU_UPDATE(elm, field) \
    if (!__wt_ref_is_root(elm)) { \
        uint64_t t, page_t; \
        char thread_str[50]; \
        WT_READ_ONCE(page_t, elm->field##_t); \
        t = __wt_clock(session); \
        if ((int64_t)(t - page_t) >= __WT_LRU_UPDATE_MAX_FREQUENCY_NS) { \
            /* __wt_atomic_store64(&elm->field##_t, t); */ \
            WT_UNUSED(__wt_thread_str(thread_str, sizeof(thread_str))); \
            thread_str[sizeof(thread_str)-1] = 0; \
            printf("+   %s %s %p %s %s:%d\n", thread_str, session->name, (void*)ref, __FUNCTION__, __FILE__, __LINE__); fflush(stdout); \
            printf("+ E %s %p %p\n", thread_str, (void*)(elm)->field.tqe_next, (void*)(elm)->field.tqe_prev); fflush(stdout); \
            __WT_LRU_LOCK(field); \
            WT_LRU_TRACEEF("WT_LRU_UPDATE +", elm, field); \
            CHECK_GUARD(WT_LRU_UPDATE3, elm, field); \
            printf("1   %s %d\n", thread_str, (int)(t - page_t)); fflush(stdout); \
            printf("+ E %s %p %p %p\n", thread_str, (void*)(elm)->field.tqe_next, (void*)(elm)->field.tqe_prev, !(elm)->field.tqe_prev ? NULL : (void*)*(elm)->field.tqe_prev); fflush(stdout); \
            WT_ASSERT(session, (elm->field.tqe_prev == NULL && elm->field.tqe_next ==  NULL) || (elm->field.tqe_prev != NULL)); \
            WT_ASSERT(session, S2C(session)->cache->field.tqh_last != NULL); \
            /* WT_ACQUIRE_BARRIER(); */ \
            elm->field##_t = t; \
            printf("2 C %s %p %p %p\n", thread_str, \
                (void*)S2C(session)->cache->field.tqh_first, (void*)S2C(session)->cache->field.tqh_last, !S2C(session)->cache->field.tqh_last ? NULL : (void*)*S2C(session)->cache->field.tqh_last); fflush(stdout); \
            printf("2 E %s %p %p %p\n", thread_str, (void*)(elm)->field.tqe_next, (void*)(elm)->field.tqe_prev, !(elm)->field.tqe_prev ? NULL : (void*)*(elm)->field.tqe_prev); fflush(stdout); \
            if ((elm)->field.tqe_next != NULL || (elm)->field.tqe_prev == NULL) { \
                printf("3   %s\n", thread_str); fflush(stdout); \
                CHECK_GUARD(WT_LRU_UPDATE3, elm, field); \
                if (WT_LRU_IS_IN_LIST(elm, field)) { \
                    TAILQ_REMOVE(&S2C(session)->cache->field, elm, field); \
                    printf("4   %s\n", thread_str); fflush(stdout); \
                } \
                (elm)->field.tqe_next = NULL; \
                (elm)->field.tqe_prev = NULL; \
                CHECK_GUARD(WT_LRU_UPDATE4, elm, field); \
                (elm)->field.tqe_next = (WT_REF*)0x12345678; \
                (elm)->field.tqe_prev = (WT_REF**)0x87654321; \
                printf("5   %s\n", thread_str); fflush(stdout); \
                TAILQ_INSERT_TAIL(&S2C(session)->cache->field, elm, field); \
                /*TAILQ_INSERT_HEAD(&S2C(session)->cache->field, elm, field);*/ \
            } \
            /* WT_RELEASE_BARRIER(); */ \
            CHECK_GUARD(WT_LRU_UPDATE5, elm, field); \
            printf("6 E %s %p %p %p\n", thread_str, (void*)(elm)->field.tqe_next, (void*)(elm)->field.tqe_prev, !(elm)->field.tqe_prev ? NULL : (void*)*(elm)->field.tqe_prev); fflush(stdout); \
            printf("6 C %s %p %p %p\n", thread_str, \
                (void*)S2C(session)->cache->field.tqh_first, (void*)S2C(session)->cache->field.tqh_last, !S2C(session)->cache->field.tqh_last ? NULL : (void*)*S2C(session)->cache->field.tqh_last); fflush(stdout); \
            WT_LRU_TRACEEF("WT_LRU_UPDATE -", elm, field); \
            __WT_LRU_UNLOCK(field); \
            printf("-   %s\n", thread_str); fflush(stdout); \
        } \
        CHECK_GUARD(WT_LRU_UPDATE6, elm, field); \
    } else {}
#define WT_LRU_POP(p_elm, field) \
    do { \
        if (__wt_atomic_load_pointer(S2C(session)->cache->field->tqh_first)) { \
            __WT_LRU_LOCK(field); \
            /* WT_ACQUIRE_BARRIER(); */ \
            /* TODO use loop to find a suitable page: in_mem, existing ->page, etc */ \
            WT_LRU_TRACEEF("WT_LRU_POP +", elm, field); \
            WT_ASSERT(session, S2C(session)->cache->field.tqh_last != NULL); \
            p_elm = TAILQ_FIRST(&S2C(session)->cache->field); \
            WT_LRU_REMOVE_AND_CLEAR_NOLOCK(p_elm, field); \
            __wt_atomic_store64(&page->field##_t, 0); \
            /* WT_RELEASE_BARRIER(); */ \
            WT_LRU_TRACEEF("WT_LRU_POP -", elm, field); \
            __WT_LRU_UNLOCK(field); \
        } else { \
            p_elm = NULL; \
        } \
    } while (0)

#define WT_LRU_REF_PAGE_SET(ref, pg, field) \
    do { \
        CHECK_GUARD(WT_LRU_REF_PAGE_SET, ref, field); \
        (ref)->page = pg; \
        WT_LRU_UPDATE(ref, lru_all); \
    } while (0);
#define WT_LRU_REF_PAGE_CLEAR(ref, field) \
    do { \
        CHECK_GUARD(WT_LRU_REF_PAGE_CLEAR, ref, field); \
        (ref)->page = NULL; \
        WT_CACHE_LRU_REMOVE_FROM_ALL(ref); \
    } while (0);

#define WT_EVICT_PRESSURE_THRESHOLD 0.95
#define WT_EVICT_SCORE_BUMP 10
#define WT_EVICT_SCORE_CUTOFF 10
#define WT_EVICT_SCORE_MAX 100
    /*
     * Score of how aggressive eviction should be about selecting eviction candidates. If eviction
     * is struggling to make progress, this score rises (up to a maximum of WT_EVICT_SCORE_MAX), at
     * which point the cache is "stuck" and transactions will be rolled back.
     */
    wt_shared uint32_t evict_aggressive_score;

    /*
     * Score of how often LRU queues are empty on refill. This score varies between 0 (if the queue
     * hasn't been empty for a long time) and 100 (if the queue has been empty the last 10 times we
     * filled up.
     */
    uint32_t evict_empty_score;

    uint32_t hs_fileid; /* History store table file ID */

    /*
     * The "history_activity" verbose messages are throttled to once per checkpoint. To accomplish
     * this we track the checkpoint generation for the most recent read and write verbose messages.
     */
    uint64_t hs_verb_gen_read;
    wt_shared uint64_t hs_verb_gen_write;

    /*
     * Cache pool information.
     */
    uint64_t cp_pass_pressure;   /* Calculated pressure from this pass */
    uint64_t cp_quota;           /* Maximum size for this cache */
    uint64_t cp_reserved;        /* Base size for this cache */
    WT_SESSION_IMPL *cp_session; /* May be used for cache management */
    uint32_t cp_skip_count;      /* Post change stabilization */
    wt_thread_t cp_tid;          /* Thread ID for cache pool manager */
    /* State seen at the last pass of the shared cache manager */
    uint64_t cp_saved_app_evicts; /* User eviction count at last review */
    uint64_t cp_saved_app_waits;  /* User wait count at last review */
    uint64_t cp_saved_read;       /* Read count at last review */

/*
 * Flags.
 */
/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CACHE_POOL_MANAGER 0x1u        /* The active cache pool manager */
#define WT_CACHE_POOL_RUN 0x2u            /* Cache pool thread running */
                                          /* AUTOMATIC FLAG VALUE GENERATION STOP 32 */
    wt_shared uint16_t pool_flags_atomic; /* Cache pool flags */

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CACHE_EVICT_CLEAN 0x001u        /* Evict clean pages */
#define WT_CACHE_EVICT_CLEAN_HARD 0x002u   /* Clean % blocking app threads */
#define WT_CACHE_EVICT_DIRTY 0x004u        /* Evict dirty pages */
#define WT_CACHE_EVICT_DIRTY_HARD 0x008u   /* Dirty % blocking app threads */
#define WT_CACHE_EVICT_NOKEEP 0x010u       /* Don't add read pages to cache */
#define WT_CACHE_EVICT_SCRUB 0x020u        /* Scrub dirty pages */
#define WT_CACHE_EVICT_UPDATES 0x040u      /* Evict pages with updates */
#define WT_CACHE_EVICT_UPDATES_HARD 0x080u /* Update % blocking app threads */
#define WT_CACHE_EVICT_URGENT 0x100u       /* Pages are in the urgent queue */
/* AUTOMATIC FLAG VALUE GENERATION STOP 32 */
#define WT_CACHE_EVICT_ALL (WT_CACHE_EVICT_CLEAN | WT_CACHE_EVICT_DIRTY | WT_CACHE_EVICT_UPDATES)
#define WT_CACHE_EVICT_HARD \
    (WT_CACHE_EVICT_CLEAN_HARD | WT_CACHE_EVICT_DIRTY_HARD | WT_CACHE_EVICT_UPDATES_HARD)
    uint32_t flags;
};

#define WT_WITH_PASS_LOCK(session, op)                                                   \
    do {                                                                                 \
        WT_WITH_LOCK_WAIT(session, &cache->evict_pass_lock, WT_SESSION_LOCKED_PASS, op); \
    } while (0)

/*
 * WT_CACHE_POOL --
 *	A structure that represents a shared cache.
 */
struct __wt_cache_pool {
    WT_SPINLOCK cache_pool_lock;
    WT_CONDVAR *cache_pool_cond;
    const char *name;
    uint64_t size;
    uint64_t chunk;
    uint64_t quota;
    uint64_t currently_used;
    uint32_t refs; /* Reference count for structure. */
    /* Locked: List of connections participating in the cache pool. */
    TAILQ_HEAD(__wt_cache_pool_qh, __wt_connection_impl) cache_pool_qh;

    wt_shared uint8_t pool_managed; /* Cache pool has a manager thread */

/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_CACHE_POOL_ACTIVE 0x1u /* Cache pool is active */
                                  /* AUTOMATIC FLAG VALUE GENERATION STOP 8 */
    uint8_t flags;
};

/*
 * Optimize comparisons against the history store URI, flag handles that reference the history store
 * file.
 */
#define WT_IS_HS(dh) F_ISSET(dh, WT_DHANDLE_HS)

/* Flags used with __wt_evict */
/* AUTOMATIC FLAG VALUE GENERATION START 0 */
#define WT_EVICT_CALL_CLOSING 0x1u  /* Closing connection or tree */
#define WT_EVICT_CALL_NO_SPLIT 0x2u /* Splits not allowed */
#define WT_EVICT_CALL_URGENT 0x4u   /* Urgent eviction */
/* AUTOMATIC FLAG VALUE GENERATION STOP 32 */
