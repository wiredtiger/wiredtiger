/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
static bool __evict_internal_page_has_cached_children(WT_SESSION_IMPL *sesison, WT_REF *ref);
static int __evict_lru_pages(WT_SESSION_IMPL *session, bool is_server);
static int __evict_page(WT_SESSION_IMPL *session);
static void __evict_read_gen_new(WT_SESSION_IMPL *session, WT_PAGE *page);
static int __evict_server(WT_SESSION_IMPL *session, bool *did_work);
static bool __evict_skip_page(WT_SESSION_IMPL *session, WT_REF *ref, int i);
static bool __evict_update_work(WT_SESSION_IMPL *session, bool *eviction_needed);

#define WT_EVICT_HAS_WORKERS(s) \
    (__wt_atomic_load_uint32_relaxed(&S2C(s)->evict_threads.current_threads) > 1)

/* !!!
 * __wt_evict_server_wake --
 *     Wake up the eviction server thread. The eviction server typically sleeps for some time when
 *     cache usage is below the target thresholds. When the cache is expected to exceed these
 *     thresholds, callers can nudge the eviction server to wake up and resume its work.
 *
 *     This function is called in situations where pages are queued for urgent eviction or when
 *     application threads request eviction assistance.
 */
void
__wt_evict_server_wake(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    cache = conn->cache;

    if (WT_VERBOSE_LEVEL_ISSET(session, WT_VERB_EVICTION, WT_VERBOSE_DEBUG_2)) {
        uint64_t bytes_dirty, bytes_inuse, bytes_max, bytes_updates;

        bytes_inuse = __wt_cache_bytes_inuse(cache);
        bytes_max = conn->cache_size;
        bytes_dirty = __wt_cache_dirty_inuse(cache);
        bytes_updates = __wt_cache_bytes_updates(cache);
        __wt_verbose_debug2(session, WT_VERB_EVICTION,
          "waking, bytes inuse %s max (%" PRIu64 "MB %s %" PRIu64 "MB), bytes dirty %" PRIu64
          "(bytes), bytes updates %" PRIu64 "(bytes)",
          bytes_inuse <= bytes_max ? "<=" : ">", bytes_inuse / WT_MEGABYTE,
          bytes_inuse <= bytes_max ? "<=" : ">", bytes_max / WT_MEGABYTE, bytes_dirty,
          bytes_updates);
    }
    __wt_cond_signal(session, conn->evict->evict_server_cond);
}

/*
 * __evict_log_cache_stuck --
 *     Output log messages if the cache is stuck.
 */
static int
__evict_log_cache_stuck(WT_SESSION_IMPL *session, bool *did_work)
{
    struct timespec now;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    uint64_t time_diff_ms;

    /* Assume there has been no progress. */
    *did_work = false;

    conn = S2C(session);
    evict = conn->evict;

    if (!__wt_evict_cache_stuck(session)) {
        evict->last_eviction_progress = 0; /* Make sure we'll notice next time we're stuck. */
        return (0);
    }

    /* Track if work was done. */
    *did_work =
      __wt_atomic_load_uint64_v_relaxed(&evict->eviction_progress) != evict->last_eviction_progress;
    evict->last_eviction_progress = __wt_atomic_load_uint64_v_relaxed(&evict->eviction_progress);

    /* Eviction is stuck, check if we have made progress. */
    if (*did_work) {
#if !defined(HAVE_DIAGNOSTIC)
        /* Need verbose check only if not in diagnostic build */
        if (WT_VERBOSE_ISSET(session, WT_VERB_EVICTION))
#endif
            __wt_epoch(session, &evict->stuck_time);
        return (0);
    }
#if !defined(HAVE_DIAGNOSTIC)
    /* Need verbose check only if not in diagnostic build */
    if (!WT_VERBOSE_ISSET(session, WT_VERB_EVICTION))
        return (0);
#endif
    /*
     * If we're stuck for 5 minutes in diagnostic mode, or the verbose eviction flag is set, log the
     * cache and transaction state.
     *
     * If we're stuck for 5 minutes in diagnostic mode, give up.
     *
     * We don't do this check for in-memory workloads because application threads are not blocked by
     * the cache being full. If the cache becomes full of clean pages, we can be servicing reads
     * while the cache appears stuck to eviction.
     */
    if (F_ISSET(conn, WT_CONN_IN_MEMORY))
        return (0);

    __wt_epoch(session, &now);

    /* The checks below should only be executed when a cache timeout has been set. */
    if (evict->cache_stuck_timeout_ms > 0) {
        time_diff_ms = WT_TIMEDIFF_MS(now, evict->stuck_time);
#ifdef HAVE_DIAGNOSTIC
        /* Enable extra logs 20ms before timing out. */
        if (evict->cache_stuck_timeout_ms < 20 ||
          (time_diff_ms > evict->cache_stuck_timeout_ms - 20))
            WT_SET_VERBOSE_LEVEL(session, WT_VERB_EVICTION, WT_VERBOSE_DEBUG_1);
#endif

        if (time_diff_ms >= evict->cache_stuck_timeout_ms) {
#ifdef HAVE_DIAGNOSTIC
            __wt_err(session, ETIMEDOUT, "Cache stuck for too long, giving up");
            WT_RET(__wt_verbose_dump_txn(session));
            WT_RET(__wt_verbose_dump_cache(session));
            WT_RET(__wt_verbose_dump_metadata(session));
            return (__wt_set_return(session, ETIMEDOUT));
#else
            if (WT_VERBOSE_ISSET(session, WT_VERB_EVICTION)) {
                WT_RET(__wt_verbose_dump_txn(session));
                WT_RET(__wt_verbose_dump_cache(session));

                /* Reset the timer. */
                __wt_epoch(session, &evict->stuck_time);
            }
#endif
        }
    }
    return (0);
}

/*
 * __evict_thread_chk --
 *     Check to decide if the eviction thread should continue running.
 */
static bool
__evict_thread_chk(WT_SESSION_IMPL *session)
{
    return (FLD_ISSET(S2C(session)->server_flags, WT_CONN_SERVER_EVICTION));
}

/*
 * __evict_thread_run --
 *     Entry function for an eviction thread. This is called repeatedly from the thread group code
 *     so it does not need to loop itself.
 */
static int
__evict_thread_run(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    bool did_work;

    conn = S2C(session);
    evict = conn->evict;
    (void)thread;

    /* Mark the session as an eviction thread session. */
    F_SET(session, WT_SESSION_EVICTION);
    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "eviction thread starting");

    /*
     * Cache a history store cursor to avoid deadlock: if an eviction thread marks a file busy and
     * then opens a different file (in this case, the HS file), it can deadlock with a thread
     * waiting for the first file to drain from the eviction queue. See WT-5946 for details.
     */
    WT_ERR(__wt_curhs_cache(session));

    /* Designate one thread to act as a server. */
    if (__wt_atomic_load_bool_relaxed(&conn->evict_server_running) &&
      __wt_spin_trylock(session, &evict->evict_housekeeping_lock) == 0) {
        ret = __evict_server(session, &did_work);
        __wt_spin_unlock(session, &evict->evict_housekeeping_lock);
        WT_ERR(ret);

        /* Pause. The wait period is shorter if the server did work */
        __wt_cond_auto_wait(session, evict->evict_server_cond, did_work, NULL);
        __wt_verbose_debug2(session, WT_VERB_EVICTION, "%s", "waking");
    } else {
        WT_ERR(__evict_lru_pages(session, false));
    }
    if (0) {
err:
        WT_RET_PANIC(session, ret, "eviction thread error");
    }
    return (ret);
}

/*
 * __evict_thread_stop --
 *     Shutdown function for an eviction thread.
 */
static int
__evict_thread_stop(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;

    if (thread->id != 0)
        return (0);

    conn = S2C(session);
    evict = conn->evict;
    (void)evict;

    /*
     * The only cases when an eviction worker is expected to stop are when recovery is finished,
     * when the connection is closing or when an error has occurred and connection panic flag is
     * set.
     */
    WT_ASSERT(session, F_ISSET(conn, WT_CONN_CLOSING | WT_CONN_PANIC | WT_CONN_RECOVERING));

    /* Clear the eviction thread session flag. */
    F_CLR(session, WT_SESSION_EVICTION);

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "eviction thread exiting");

    return (0);
}

/* !!!
 * __wt_evict_threads_create --
 *     Initiate the eviction process by creating and launching the eviction threads.
 *
 *     The `threads_max` and `threads_min` configurations in `api_data.py` control the maximum and
 *     minimum number of eviction worker threads in WiredTiger. One of the threads acts as the
 *     eviction server, responsible for identifying evictable pages and placing them in eviction
 *     queues. The remaining threads are eviction workers, responsible for evicting pages from these
 *     eviction queues.
 *
 *     This function is called once during `wiredtiger_open` or recovery.
 *
 *     Return an error code if the thread group creation fails.
 */
int
__wt_evict_threads_create(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    uint32_t session_flags;

    conn = S2C(session);
    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "starting eviction threads");

    /*
     * In case recovery has allocated some transaction IDs, bump to the current state. This will
     * prevent eviction threads from pinning anything as they start up and read metadata in order to
     * open cursors.
     */
    WT_RET(__wt_txn_update_oldest(session, WT_TXN_OLDEST_STRICT | WT_TXN_OLDEST_WAIT));

    WT_ASSERT(session, conn->evict_threads_min > 0);

    /* Set first, the thread might run before we finish up. */
    FLD_SET(conn->server_flags, WT_CONN_SERVER_EVICTION);

    /*
     * Create the eviction thread group. Set the group size to the maximum allowed sessions.
     */
    session_flags = WT_THREAD_CAN_WAIT | WT_THREAD_PANIC_FAIL;
    WT_RET(__wt_thread_group_create(session, &conn->evict_threads, "eviction-server",
      conn->evict_threads_min, conn->evict_threads_max, session_flags, __evict_thread_chk,
      __evict_thread_run, __evict_thread_stop));

/*
 * Ensure the cache stuck timer is initialized when starting eviction.
 */
#if !defined(HAVE_DIAGNOSTIC)
    /* Need verbose check only if not in diagnostic build */
    if (WT_VERBOSE_ISSET(session, WT_VERB_EVICTION))
#endif
        __wt_epoch(session, &conn->evict->stuck_time);

    __wt_atomic_store_bool_relaxed(&conn->evict_server_running, true);

    return (0);
}

/* !!!
 * __wt_evict_threads_destroy --
 *     Stop and destroy the eviction threads. It must be called exactly once during
 *     `WT_CONNECTION::close` or recovery to ensure all eviction threads are properly terminated.
 *
 *     Return an error code if the thread group destruction fails.
 */
int
__wt_evict_threads_destroy(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    /* We are done if the eviction server didn't start successfully. */
    if (!__wt_atomic_load_bool_relaxed(&conn->evict_server_running))
        return (0);

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "stopping eviction threads");

    /* Wait for any eviction thread group changes to stabilize. */
    __wt_writelock(session, &conn->evict_threads.lock);

    /*
     * Signal the threads to finish and stop populating the queue.
     */
    FLD_CLR(conn->server_flags, WT_CONN_SERVER_EVICTION);
    __wt_atomic_store_bool_relaxed(&conn->evict_server_running, false);

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "waiting for eviction threads to stop");

    /*
     * We call the destroy function still holding the write lock. It assumes it is called locked.
     */
    WT_RET(__wt_thread_group_destroy(session, &conn->evict_threads));

    return (0);
}

/*
 * __evict_lru_pages --
 *     Get pages from the LRU queue to evict.
 */
static int
__evict_lru_pages(WT_SESSION_IMPL *session, bool is_server)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_TRACK_OP_DECL;
    bool eviction_needed;

    WT_TRACK_OP_INIT(session);
    conn = S2C(session);

    while (FLD_ISSET(conn->server_flags, WT_CONN_SERVER_EVICTION) &&
      F_ISSET(conn->evict, WT_EVICT_CACHE_ANY) && ret == 0) {
        WT_RET(__evict_update_work(session, &eviction_needed));
        if (!eviction_needed)
            break;
        if ((ret = __evict_page(session)) == EBUSY)
            ret = 0;
        if (is_server)
            break;
    }

    /* If any resources are pinned, release them now. */
    WT_TRET(__wt_session_release_resources(session));

    /* If a worker thread is here, there is no work to do; pause. */
    if (ret == WT_NOTFOUND && !is_server && FLD_ISSET(conn->server_flags, WT_CONN_SERVER_EVICTION))
        __wt_cond_wait(session, conn->evict_threads.wait_cond, 10 * WT_THOUSAND, NULL);

    WT_TRACK_OP_END(session);
    return (ret == WT_NOTFOUND ? 0 : ret);
}

/*
 * __evict_update_work --
 *     Configure eviction work state.
 */
static bool
__evict_update_work(WT_SESSION_IMPL *session, bool *eviction_needed)
{
    WT_BTREE *hs_tree;
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    double dirty_target, dirty_trigger, target, trigger;
    uint64_t bytes_dirty, bytes_inuse, bytes_max, total_dirty, total_inmem, total_updates;
    uint32_t flags, hs_id;

    conn = S2C(session);
    cache = conn->cache;
    evict = conn->evict;

    dirty_target = __wti_evict_dirty_target(evict);
    dirty_trigger = __wt_atomic_load_double_relaxed(&evict->eviction_dirty_trigger);
    target = evict->eviction_target;
    trigger = evict->eviction_trigger;

    /* Build up the new state. */
    flags = 0;

    if (!FLD_ISSET(conn->server_flags, WT_CONN_SERVER_EVICTION)) {
        __wt_atomic_store_uint32_relaxed(&evict->flags, 0);
        *eviction_needed = false;
        return (0);
    }

    /*
     * TODO: We are caching the cache usage values associated with the history store because the
     * history store dhandle isn't always available to eviction. Keeping potentially out-of-date
     * values could lead to surprising bugs in the future.
     */
    if (F_ISSET_ATOMIC_32(conn, WT_CONN_HS_OPEN)) {
        total_dirty = total_inmem = total_updates = 0;
        hs_id = 0;
        for (;;) {
            WT_RET_NOTFOUND_OK(ret = __wt_curhs_next_hs_id(session, hs_id, &hs_id));
            if (ret == WT_NOTFOUND) {
                ret = 0;
                (void)ret; /* Keep the assignment to 0 just in case, but suppress clang warnings. */
                break;
            }
            /*
             * At this point, we are under the evict pass lock XXX -- FIX and should only attempt to
             * read from the cursors dhandle cache to obtain the HS. If it is not present in the
             * cursors dhandle cache, we bail out. We must not proceed to acquire a connection
             * dhandle read lock or a schema lock to acquire the HS dhandle while holding the pass
             * lock, as this could lead to a deadlock. There are several places in the code where a
             * pass lock is taken after a schema lock, which makes this sequence unsafe.
             */
            WT_RET_NOTFOUND_OK(ret = __wt_curhs_get_cached(session, hs_id, &hs_tree));
            if (ret == 0) {
                total_inmem += __wt_atomic_load_uint64_relaxed(&hs_tree->bytes_inmem);
                total_dirty += __wt_atomic_load_uint64_relaxed(&hs_tree->bytes_dirty_intl) +
                  __wt_atomic_load_uint64_relaxed(&hs_tree->bytes_dirty_leaf);
                total_updates += __wt_atomic_load_uint64_relaxed(&hs_tree->bytes_updates);
            } else {
                if (hs_id == WT_HS_ID)
                    WT_STAT_CONN_INCR(session, cache_eviction_hs_cursor_not_cached);
                else if (hs_id == WT_HS_ID_SHARED)
                    WT_STAT_CONN_INCR(session, cache_eviction_hs_shared_cursor_not_cached);
            }
        }
        __wt_atomic_store_uint64_relaxed(&cache->bytes_hs, total_inmem);
        __wt_atomic_store_uint64_relaxed(&cache->bytes_hs_dirty, total_dirty);
        __wt_atomic_store_uint64_relaxed(&cache->bytes_hs_updates, total_updates);
    }

    /*
     * If we need space in the cache, try to find clean pages to evict.
     *
     * Avoid division by zero if the cache size has not yet been set in a shared cache.
     */
    bytes_max = __wt_tsan_suppress_load_uint64_v(&conn->cache_size) + 1;
    bytes_inuse = __wt_cache_bytes_inuse(cache);
    if (__wti_evict_exceeded_clean_trigger(session, NULL)) {
        LF_SET(WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_CLEAN_HARD);
        WT_STAT_CONN_INCR(session, cache_eviction_trigger_reached);
    } else if (__wti_evict_exceeded_clean_target(session)) {
        LF_SET(WT_EVICT_CACHE_CLEAN);
    }

    bytes_dirty = __wt_cache_dirty_leaf_inuse(cache);
    if (__wti_evict_exceeded_dirty_trigger(session, NULL)) {
        LF_SET(WT_EVICT_CACHE_DIRTY | WT_EVICT_CACHE_DIRTY_HARD);
        WT_STAT_CONN_INCR(session, cache_eviction_trigger_dirty_reached);
    } else if (__wti_evict_exceeded_dirty_target(session)) {
        LF_SET(WT_EVICT_CACHE_DIRTY);
    }

    if (__wti_evict_exceeded_updates_trigger(session, NULL)) {
        LF_SET(WT_EVICT_CACHE_UPDATES | WT_EVICT_CACHE_UPDATES_HARD);
        WT_STAT_CONN_INCR(session, cache_eviction_trigger_updates_reached);
    } else if (__wti_evict_exceeded_updates_target(session)) {
        LF_SET(WT_EVICT_CACHE_UPDATES);
    }

    /*
     * If application threads are blocked by data in cache, track the fill ratio.
     */
    uint64_t cache_fill_ratio = bytes_inuse / bytes_max;
    bool evict_is_hard = LF_ISSET(WT_EVICT_CACHE_HARD);
    if (evict_is_hard) {
        if (cache_fill_ratio < 0.25)
            WT_STAT_CONN_INCR(session, cache_eviction_app_threads_fill_ratio_lt_25);
        else if (cache_fill_ratio < 0.50)
            WT_STAT_CONN_INCR(session, cache_eviction_app_threads_fill_ratio_25_50);
        else if (cache_fill_ratio < 0.75)
            WT_STAT_CONN_INCR(session, cache_eviction_app_threads_fill_ratio_50_75);
        else
            WT_STAT_CONN_INCR(session, cache_eviction_app_threads_fill_ratio_gt_75);
    }

    /*
     * If application threads are blocked by the total volume of data in cache, try dirty pages as
     * well.
     */
    if (LF_ISSET(WT_EVICT_CACHE_CLEAN_HARD) && __wt_evict_aggressive(session))
        LF_SET(WT_EVICT_CACHE_DIRTY);

    if (!F_ISSET(evict, WT_EVICT_CACHE_DIRTY | WT_EVICT_CACHE_UPDATES))
        WT_STAT_CONN_INCR(session, eviction_target_strategy_clean);
    else if (!F_ISSET(evict, WT_EVICT_CACHE_CLEAN)) {
        WT_STAT_CONN_INCR(session, eviction_target_strategy_dirty);
    } else
        WT_STAT_CONN_INCR(session, eviction_target_strategy_both_clean_and_dirty);

    /*
     * Configure scrub - which reinstates clean equivalents of reconciled dirty pages. This is
     * useful because an evicted dirty page isn't necessarily a good proxy for knowing if the page
     * will be accessed again soon. Be more aggressive about scrubbing in disaggregated storage
     * because the cost of retrieving a recently reconciled page is higher in that configuration. In
     * the local storage case scrub dirty pages and keep them in cache if we are less than half way
     * to the clean, dirty and updates triggers.
     *
     * There's an experimental flag WT_CACHE_PREFER_SCRUB_EVICTION that can be turned on to enable
     * scrub eviction as long as cache usage overall is under half way to the trigger limit.
     */
    if (__wt_conn_is_disagg(session) && bytes_inuse < (uint64_t)(trigger * bytes_max) / 100)
        LF_SET(WT_EVICT_CACHE_SCRUB);
    else if (bytes_inuse < (uint64_t)((target + trigger) * bytes_max) / 200) {
        if (F_ISSET_ATOMIC_32(
              &(conn->cache->cache_eviction_controls), WT_CACHE_PREFER_SCRUB_EVICTION)) {
            LF_SET(WT_EVICT_CACHE_SCRUB);
        } else if (bytes_dirty < (uint64_t)((dirty_target + dirty_trigger) * bytes_max) / 200) {
            LF_SET(WT_EVICT_CACHE_SCRUB);
        }

    } else
        LF_SET(WT_EVICT_CACHE_NOKEEP);

    if (FLD_ISSET(conn->debug_flags, WT_CONN_DEBUG_UPDATE_RESTORE_EVICT)) {
        LF_SET(WT_EVICT_CACHE_SCRUB);
        LF_CLR(WT_EVICT_CACHE_NOKEEP);
    }

    /*
     * With an in-memory cache, we only do dirty eviction in order to scrub pages.
     */
    if (F_ISSET(conn, WT_CONN_IN_MEMORY)) {
        if (LF_ISSET(WT_EVICT_CACHE_CLEAN))
            LF_SET(WT_EVICT_CACHE_DIRTY);
        if (LF_ISSET(WT_EVICT_CACHE_CLEAN_HARD))
            LF_SET(WT_EVICT_CACHE_DIRTY_HARD);
        LF_CLR(WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_CLEAN_HARD);
    }

    /* Update the global eviction state. */
    __wt_atomic_store_uint32_relaxed(&evict->flags, flags);

    *eviction_needed = F_ISSET(evict, WT_EVICT_CACHE_ANY | WT_EVICT_CACHE_URGENT);
    return (0);
}

/* !!!
 * __wt_evict_file_exclusive_on --
 *     Acquire exclusive access to a file/tree making it possible to evict the entire file using
 *     `__wt_evict_file`. It does this by incrementing the `evict_disabled` counter for a
 *     tree, which disables all other means of eviction (except file eviction).
 *
 *     For the incremented `evict_disabled` value, the eviction workers skip this tree for
 *     eviction.
 *
 *     It is called from multiple places in the code base, such as when initiating file eviction
 *     `__wt_evict_file` or when opening or closing trees.
 */
#if 1
void
__wt_evict_file_exclusive_on(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    btree = S2BT(session);

    (void)__wt_atomic_add_int32(&btree->evict_data.evict_disabled, 1);

    __wt_verbose_debug1(session, WT_VERB_EVICTION, "obtained exclusive eviction lock on btree %s",
      btree->dhandle->name);

    /*
     * Special operations don't enable eviction, however the underlying command (e.g. verify) may
     * choose to turn on eviction. This falls outside of the typical eviction flow, and here
     * eviction may forcibly remove pages from the cache. Consequently, we may end up evicting
     * internal pages which still have child pages present on the pre-fetch queue. Remove any refs
     * still present on the pre-fetch queue so that they are not accidentally accessed in an invalid
     * way later on.
     */
    WT_ERR(__wt_conn_prefetch_clear_tree(session, false));

    /*
     * We have disabled further eviction: wait for concurrent LRU eviction activity to drain.
     */
    while (__wt_tsan_suppress_load_uint32_v(&btree->evict_data.evict_busy))
        __wt_yield();

    if (0) {
err:
        (void)__wt_atomic_sub_int32(&btree->evict_data.evict_disabled, 1);
    }

    printf("evict_disabled  = %d\n", btree->evict_data.evict_disabled);
    fflush (stdout);
}
#endif

#if 0
void
__wt_evict_file_exclusive_on(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_EVICT *evict;

    btree = S2BT(session);
    evict = S2C(session)->evict;

    /*
     * Hold the exclusive lock to turn off eviction. If this lock becomes a bottleneck, we could
     * create per-handle exclusive locks.
     */
    __wt_spin_lock(session, &evict->evict_exclusive_lock);
    if (++btree->evict_data.evict_disabled > 1) {
        __wt_spin_unlock(session, &evict->evict_exclusive_lock);
        return;
    }

    __wt_verbose_debug1(session, WT_VERB_EVICTION, "obtained exclusive eviction lock on btree %s",
      btree->dhandle->name);

    /*
     * Special operations don't enable eviction, however the underlying command (e.g. verify) may
     * choose to turn on eviction. This falls outside of the typical eviction flow, and here
     * eviction may forcibly remove pages from the cache. Consequently, we may end up evicting
     * internal pages which still have child pages present on the pre-fetch queue. Remove any refs
     * still present on the pre-fetch queue so that they are not accidentally accessed in an invalid
     * way later on.
     */
    WT_ERR(__wt_conn_prefetch_clear_tree(session, false));

    /*
     * We have disabled further eviction: wait for concurrent LRU eviction activity to drain.
     */
    while (btree->evict_data.evict_busy > 0)
        __wt_yield();

    if (0) {
err:
        --btree->evict_data.evict_disabled;
    }
    __wt_spin_unlock(session, &evict->evict_exclusive_lock);
}
#endif

#if 1
/* !!!
 * __wt_evict_file_exclusive_off --
 *     Release exclusive access to a file/tree by decrementing the `evict_disabled` count
 *     back to zero, allowing eviction to proceed for the tree.
 *
 *     It is called from multiple places in the code where exclusive eviction access is no longer
 *     needed.
 */
void
__wt_evict_file_exclusive_off(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;

    btree = S2BT(session);

    /*
     * We have seen subtle bugs with multiple threads racing to turn eviction on/off. Make races
     * more likely in diagnostic builds.
     */
    WT_DIAGNOSTIC_YIELD;

/*
 * Atomically decrement the evict-disabled count, without acquiring the eviction walk-lock. We can't
 * acquire that lock here because there's a potential deadlock. When acquiring exclusive eviction
 * access, we acquire the eviction walk-lock and then the eviction's pass-intr lock. The eviction
 * server can hold the pass-intr lock and call into this function, which might deadlock with another
 * thread trying to get exclusive eviction access.
 */
#if defined(HAVE_DIAGNOSTIC)
    {
        int32_t v;

        v = __wt_atomic_sub_int32(&btree->evict_data.evict_disabled, 1);
        if(v < 0){
            printf("evict_disabled  = %d\n", btree->evict_data.evict_disabled);
            fflush (stdout);
            WT_ASSERT(session, v >= 0);
        }
    }
#else
    (void)__wt_atomic_sub_int32(&btree->evict_data.evict_disabled, 1);
#endif
    printf("evict_disabled  = %d\n", btree->evict_data.evict_disabled);
    fflush (stdout);
}
#endif

#if 0
/* !!!
 * __wt_evict_file_exclusive_off --
 *     Release exclusive access to a file/tree by decrementing the `evict_disabled` count
 *     back to zero, allowing eviction to proceed for the tree.
 *
 *     It is called from multiple places in the code where exclusive eviction access is no longer
 *     needed.
 */
void
__wt_evict_file_exclusive_off(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;
    WT_EVICT *evict;

    btree = S2BT(session);
    evict = S2C(session)->evict;

    /*
     * We have seen subtle bugs with multiple threads racing to turn eviction on/off. Make races
     * more likely in diagnostic builds.
     */
    WT_DIAGNOSTIC_YIELD;

    __wt_spin_lock(session, &evict->evict_exclusive_lock);
    --btree->evict_data.evict_disabled;
#if defined(HAVE_DIAGNOSTIC)
    WT_ASSERT(session, btree->evict_data.evict_disabled >= 0);
#endif
    __wt_spin_unlock(session, &evict->evict_exclusive_lock);
    __wt_verbose_debug1(session, WT_VERB_EVICTION, "released exclusive eviction lock on btree %s",
      btree->dhandle->name);
}
#endif

#define EVICT_TUNE_BATCH 1 /* Max workers to add each period */
                           /*
                            * Data points needed before deciding if we should keep adding workers or
                            * settle on an earlier value.
                            */
#define EVICT_TUNE_DATAPT_MIN 8
#define EVICT_TUNE_PERIOD 60 /* Tune period in milliseconds */

/*
 * We will do a fresh re-tune every that many milliseconds to adjust to significant phase changes.
 */
#define EVICT_FORCE_RETUNE (25 * WT_THOUSAND)

/*
 * __evict_tune_workers --
 *     Find the right number of eviction workers. Gradually ramp up the number of workers increasing
 *     the number in batches indicated by the setting above. Store the number of workers that gave
 *     us the best throughput so far and the number of data points we have tried. Every once in a
 *     while when we have the minimum number of data points we check whether the eviction throughput
 *     achieved with the current number of workers is the best we have seen so far. If so, we will
 *     keep increasing the number of workers. If not, we are past the infliction point on the
 *     eviction throughput curve. In that case, we will set the number of workers to the best
 *     observed so far and settle into a stable state.
 */
static void
__evict_tune_workers(WT_SESSION_IMPL *session)
{
    struct timespec current_time;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    uint64_t delta_msec, delta_pages;
    uint64_t eviction_progress, eviction_progress_rate, time_diff;
    uint32_t current_threads;
    int32_t cur_threads, i, target_threads, thread_surplus;

    conn = S2C(session);
    evict = conn->evict;

    /*
     * If we have a fixed number of eviction threads, there is no value in calculating if we should
     * do any tuning.
     */
    if (conn->evict_threads_max == conn->evict_threads_min)
        return;

    __wt_epoch(session, &current_time);
    time_diff = WT_TIMEDIFF_MS(current_time, evict->evict_tune_last_time);

    /*
     * If we have reached the stable state and have not run long enough to surpass the forced
     * re-tuning threshold, return.
     */
    if (evict->evict_tune_stable) {
        if (time_diff < EVICT_FORCE_RETUNE)
            return;

        /*
         * Stable state was reached a long time ago. Let's re-tune. Reset all the state.
         */
        evict->evict_tune_stable = false;
        evict->evict_tune_last_action_time.tv_sec = 0;
        evict->evict_tune_progress_last = 0;
        evict->evict_tune_num_points = 0;
        evict->evict_tune_progress_rate_max = 0;

        /* Reduce the number of eviction workers by one */
        thread_surplus =
          (int32_t)__wt_atomic_load_uint32_relaxed(&conn->evict_threads.current_threads) -
          (int32_t)conn->evict_threads_min;

        if (thread_surplus > 0)
            __wt_thread_group_stop_one(session, &conn->evict_threads);

    } else if (time_diff < EVICT_TUNE_PERIOD)
        /*
         * If we have not reached stable state, don't do anything unless enough time has passed
         * since the last time we have taken any action in this function.
         */
        return;

    /*
     * Measure the evicted progress so far. Eviction rate correlates to performance, so this is our
     * metric of success.
     */
    eviction_progress = __wt_atomic_load_uint64_v_relaxed(&evict->eviction_progress);

    /*
     * If we have recorded the number of pages evicted at the end of the previous measurement
     * interval, we can compute the eviction rate in evicted pages per second achieved during the
     * current measurement interval. Otherwise, we just record the number of evicted pages and
     * return.
     */
    if (evict->evict_tune_progress_last == 0)
        goto done;

    delta_msec = WT_TIMEDIFF_MS(current_time, evict->evict_tune_last_time);
    delta_pages = eviction_progress - evict->evict_tune_progress_last;
    eviction_progress_rate = (delta_pages * WT_THOUSAND) / delta_msec;
    evict->evict_tune_num_points++;

    /*
     * Keep track of the maximum eviction throughput seen and the number of workers corresponding to
     * that throughput.
     */
    if (eviction_progress_rate > evict->evict_tune_progress_rate_max) {
        evict->evict_tune_progress_rate_max = eviction_progress_rate;
        current_threads = __wt_atomic_load_uint32_relaxed(&conn->evict_threads.current_threads);
        __wt_atomic_store_uint32_relaxed(&evict->evict_tune_workers_best, current_threads);
    }

    /*
     * Compare the current number of data points with the number needed variable. If they are equal,
     * we will check whether we are still going up on the performance curve, in which case we will
     * increase the number of needed data points, to provide opportunity for further increasing the
     * number of workers. Or we are past the inflection point on the curve, in which case we will go
     * back to the best observed number of workers and settle into a stable state.
     */
    if (evict->evict_tune_num_points >= evict->evict_tune_datapts_needed) {
        current_threads = __wt_atomic_load_uint32_relaxed(&conn->evict_threads.current_threads);
        if (evict->evict_tune_workers_best == current_threads &&
          current_threads < conn->evict_threads_max) {
            /*
             * Keep adding workers. We will check again at the next check point.
             */
            evict->evict_tune_datapts_needed += WT_MIN(EVICT_TUNE_DATAPT_MIN,
              (conn->evict_threads_max - current_threads) / EVICT_TUNE_BATCH);
        } else {
            /*
             * We are past the inflection point. Choose the best number of eviction workers observed
             * and settle into a stable state.
             */
            thread_surplus =
              (int32_t)__wt_atomic_load_uint32_relaxed(&conn->evict_threads.current_threads) -
              (int32_t)evict->evict_tune_workers_best;

            for (i = 0; i < thread_surplus; i++)
                __wt_thread_group_stop_one(session, &conn->evict_threads);

            evict->evict_tune_stable = true;
            goto done;
        }
    }

    /*
     * If we have not added any worker threads in the past, we set the number of data points needed
     * equal to the number of data points that we must accumulate before deciding if we should keep
     * adding workers or settle on a previously tried stable number of workers.
     */
    if (evict->evict_tune_last_action_time.tv_sec == 0)
        evict->evict_tune_datapts_needed = EVICT_TUNE_DATAPT_MIN;

    if (F_ISSET(evict, WT_EVICT_CACHE_ANY)) {
        cur_threads =
          (int32_t)__wt_atomic_load_uint32_relaxed(&conn->evict_threads.current_threads);
        target_threads = WT_MIN(cur_threads + EVICT_TUNE_BATCH, (int32_t)conn->evict_threads_max);
        /*
         * Start the new threads.
         */
        for (i = cur_threads; i < target_threads; ++i) {
            __wt_thread_group_start_one(session, &conn->evict_threads, false);
            __wt_verbose_debug1(session, WT_VERB_EVICTION, "%s", "added worker thread");
        }
        evict->evict_tune_last_action_time = current_time;
    }

done:
    evict->evict_tune_last_time = current_time;
    evict->evict_tune_progress_last = eviction_progress;
}

/*
 * __evict_server --
 *     Work to do for a thread elected to act as a server. In addition to evicting pages this thread
 *     is responsible for tuning the number of workers and incrementing the global read generation.
 */
#define EVICT_WORK_THRESHOLD 20
static int
__evict_server(WT_SESSION_IMPL *session, bool *did_work)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_TRACK_OP_DECL;
    WT_TXN_GLOBAL *txn_global;
    uint64_t eviction_progress, oldest_id, prev_oldest_id, evicted_pages_new, evicted_pages_prev;
    uint64_t time_now, time_prev;
    u_int loop;
    bool eviction_needed;

    WT_TRACK_OP_INIT(session);
    conn = S2C(session);
    cache = conn->cache;
    evict = conn->evict;
    txn_global = &conn->txn_global;
    time_prev = 0; /* [-Wconditional-uninitialized] */

    /* Track whether pages are being evicted and progress is made. */
    evicted_pages_prev = __wt_atomic_load_uint64_v_relaxed(&evict->evicted_pages);
    eviction_progress = __wt_atomic_load_uint64_v_relaxed(&evict->eviction_progress);
    prev_oldest_id = __wt_atomic_load_uint64_v_relaxed(&txn_global->oldest_id);

    for (loop = 0;; loop++) {
        time_now = __wt_clock(session);
        if (loop == 0)
            time_prev = time_now;

        __evict_tune_workers(session);

        /* Increment the shared read generation only if we are actually evicting pages */
        if ((evicted_pages_new = __wt_atomic_load_uint64_v_relaxed(&evict->evicted_pages)) -
            evicted_pages_prev >
          EVICT_WORK_THRESHOLD) {
            __wt_atomic_add_uint64(&evict->read_gen, 1);
            evicted_pages_prev = evicted_pages_new;
            WT_STAT_CONN_SET(session, eviction_server_readgen,
                             __wt_atomic_load_uint64_v_relaxed(&evict->read_gen));
        }

        /*
         * Update the oldest ID: we use it to decide whether pages are candidates for eviction.
         * Without this, if all threads are blocked after a long-running transaction (such as a
         * checkpoint) completes, we may never start evicting again.
         *
         * Do this every time the eviction server wakes up, regardless of whether the cache is full,
         * to prevent the oldest ID falling too far behind. Don't wait to lock the table: with
         * highly threaded workloads, that creates a bottleneck.
         */
        WT_RET(__wt_txn_update_oldest(session, WT_TXN_OLDEST_STRICT));

        WT_RET(__evict_update_work(session, &eviction_needed));
        if (!eviction_needed)
            break;

        __wt_verbose_debug2(session, WT_VERB_EVICTION,
          "Eviction pass with: Max: %" PRIu64 " In use: %" PRIu64 " Dirty: %" PRIu64
          " Updates: %" PRIu64,
          conn->cache_size, __wt_atomic_load_uint64_relaxed(&cache->bytes_inmem),
          __wt_atomic_load_uint64_relaxed(&cache->bytes_dirty_intl) +
            __wt_atomic_load_uint64_relaxed(&cache->bytes_dirty_leaf),
          __wt_atomic_load_uint64_relaxed(&cache->bytes_updates));

        /* Evict pages if there are no workers */
        if (!WT_EVICT_HAS_WORKERS(session)) {
            WT_RET(__evict_lru_pages(session, true));
        }

        /*
         * If we're making progress, keep going; if we're not making any progress at all, mark the
         * cache "stuck" and go back to sleep, it's not something we can fix.
         *
         * We check for progress every 20ms, the idea being that the aggressive score will reach 10
         * after 200ms if we aren't making progress and eviction will start considering more pages.
         * If there is still no progress after 2s, we will treat the cache as stuck and start
         * rolling back transactions and writing updates to the history store table.
         */
        if (eviction_progress == __wt_atomic_load_uint64_v_relaxed(&evict->eviction_progress)) {
            if (WT_CLOCKDIFF_MS(time_now, time_prev) >= 20 && F_ISSET(evict, WT_EVICT_CACHE_HARD)) {
                if (__wt_atomic_load_uint32_relaxed(&evict->evict_aggressive_score) <
                  WT_EVICT_SCORE_MAX)
                    (void)__wt_atomic_add_uint32(&evict->evict_aggressive_score, 1);
                oldest_id = __wt_atomic_load_uint64_v(&txn_global->oldest_id);
                if (prev_oldest_id == oldest_id &&
                  __wt_atomic_load_uint64_v_relaxed(&txn_global->current) != oldest_id &&
                  __wt_atomic_load_uint32_relaxed(&evict->evict_aggressive_score) <
                    WT_EVICT_SCORE_MAX)
                    (void)__wt_atomic_add_uint32(&evict->evict_aggressive_score, 1);
                time_prev = time_now;
                prev_oldest_id = oldest_id;
            }

            /*
             * Keep trying for long enough that we should be able to evict a page.
             */
            if (loop < 100 ||
              __wt_atomic_load_uint32_relaxed(&evict->evict_aggressive_score) <
                WT_EVICT_SCORE_MAX) {
                /*
                 * Back off if we aren't making progress.
                 */
                WT_STAT_CONN_INCR(session, eviction_server_slept);
                __wt_cond_wait(session, evict->evict_server_cond, WT_THOUSAND, NULL);
                continue;
            }
            WT_STAT_CONN_INCR(session, eviction_slow);
            __wt_verbose_debug1(session, WT_VERB_EVICTION, "%s", "unable making slow progress");
            break;
        }
        if (__wt_atomic_load_uint32_relaxed(&evict->evict_aggressive_score) > 0)
            (void)__wt_atomic_sub_uint32(&evict->evict_aggressive_score, 1);
        loop = 0;
        eviction_progress = __wt_atomic_load_uint64_v_relaxed(&evict->eviction_progress);
    }

    /* Check if the cache is stuck and write messages to the log */
    __evict_log_cache_stuck(session, did_work);

    /* If any resources are pinned, release them now. */
    WT_TRET(__wt_session_release_resources(session));

    WT_TRACK_OP_END(session);
    return (ret == WT_NOTFOUND ? 0 : ret);
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
      (uint64_t)(0.5 * evict->eviction_target * bytes_max) / 100)
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
        WT_STAT_CONN_INCR(session, eviction_skip_pages_retry);
        return (true);
    }

    /*
     * If we are under cache pressure, allow evicting pages with newly committed updates to free
     * space. Otherwise, avoid doing that as it may thrash the cache.
     */
    if (F_ISSET(conn->evict, WT_EVICT_CACHE_DIRTY_HARD | WT_EVICT_CACHE_UPDATES_HARD) &&
      F_ISSET(txn, WT_TXN_HAS_SNAPSHOT)) {
        if (!__txn_visible_id(session, __wt_atomic_load_uint64_relaxed(&page->modify->update_txn)))
            return (true);
    } else if (__wt_atomic_load_uint64_relaxed(&page->modify->update_txn) >=
      __wt_atomic_load_uint64_v_relaxed(&conn->txn_global.last_running)) {
        WT_STAT_CONN_INCR(session, eviction_skip_page_last_running);
        return (true);
    } else if (F_ISSET(conn, WT_CONN_PRECISE_CHECKPOINT)) {
        WT_BTREE *btree = S2BT(session);
        wt_timestamp_t newest_commit_timestamp =
          __wt_atomic_load_uint64_relaxed(&page->modify->newest_commit_timestamp);
        if (F_ISSET(btree, WT_BTREE_GARBAGE_COLLECT)) {
            wt_timestamp_t prune_timestamp =
              __wt_atomic_load_uint64_relaxed(&btree->prune_timestamp);
            if (newest_commit_timestamp > prune_timestamp) {
                WT_STAT_CONN_INCR(session, eviction_skip_page_prune_timestamp);
                return (true);
            }
        } else {
            if (newest_commit_timestamp > __wt_txn_pinned_stable_timestamp(session)) {
                WT_STAT_CONN_INCR(session, eviction_skip_page_checkpoint_timestamp);
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
            WT_IGNORE_RET(__wti_evict_exceeded_dirty_trigger(session, &pct_dirty));
            high_pressure = (pct_dirty >
              (conn->evict->eviction_dirty_trigger * WT_DIRTY_PAGE_LOW_PRESSURE_THRESHOLD));
        }

        if (!high_pressure && F_ISSET(conn->evict, WT_EVICT_CACHE_UPDATES)) {
            WT_IGNORE_RET(__wti_evict_exceeded_updates_trigger(session, &pct_updates));
            high_pressure = (pct_updates >
              (__wt_atomic_load_double_relaxed(&conn->evict->eviction_updates_trigger) *
                WT_DIRTY_PAGE_LOW_PRESSURE_THRESHOLD));
        }

        if (!high_pressure) {
            WT_STAT_CONN_INCR(session, eviction_skip_few_updates_no_pressure);
            return (true);
        }
    }
    return (false);
}

#define PRINT_CACHE_STATE 0
#if PRINT_CACHE_STATE
static const char *WT_EVICT_LEVEL_NAMES[] = {"WT_EVICT_LEVEL_WONT_NEED_LEAF",
  "WT_EVICT_LEVEL_CLEAN_LEAF", "WT_EVICT_LEVEL_DIRTY_LEAF", "WT_EVICT_LEVEL_WONT_NEED_INTERNAL",
  "WT_EVICT_LEVEL_DIRTY_INTERNAL", "WT_EVICT_LEVEL_UPDATES_LEAF", "WT_EVICT_LEVEL_UPDATES_INTERNAL",
  "WT_EVICT_LEVEL_CLEAN_INTERNAL"};

static const char *
__evict_level_to_string(uint32_t level)
{

    if (level < (int)(sizeof(WT_EVICT_LEVEL_NAMES) / sizeof(WT_EVICT_LEVEL_NAMES[0])))
        return WT_EVICT_LEVEL_NAMES[level];
    else
        return "UNKNOWN_WT_EVICT_LEVEL";
}

#endif

/*
 * __evict_get_ref --
 *     Get a page for eviction. The returned page is locked. It will be unlocked by the function
 *     that tries to evict it from memory if eviction fails. The ref remains in its evict bucket. It
 *     will be removed during eviction, just before reconciliation, and will be put back in the
 *     event eviction fails.
 */
static int
__evict_get_ref(
  WT_SESSION_IMPL *session, WT_BTREE **btreep, WT_REF **refp, WT_REF_STATE *previous_statep)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_EVICT_BUCKET *bucket;
    WT_EVICT_BUCKETSET *bucketset;
    WT_PAGE *page;
    WT_REF *ref;
    WT_REF_STATE previous_state;
    uint32_t i, iter, j, min_level, max_level, num_buckets, total_iter;
    int early_skipped_tree, skipped, skip_locked;
    bool skip_page;

#if PRINT_CACHE_STATE
    int empty_buckets;
    uint64_t total_items;
    WT_CACHE *cache;
#endif

    *btreep = NULL;
    bucketset = NULL;
    conn = S2C(session);
    evict = conn->evict;
    i = 0;
    iter = total_iter = 0;
    min_level = max_level = 0;
    previous_state = 0;
    early_skipped_tree = skipped = skip_locked = 0;
#if PRINT_CACHE_STATE
    cache = conn->cache;
    total_items = 0;
#endif
    /*
     * It is polite to initialize output variables, but it isn't safe for callers to use the
     * previous state if we don't return a locked ref.
     */
    *previous_statep = WT_REF_MEM;
    *refp = ref = NULL;

    if (!F_ISSET(evict, WT_EVICT_CACHE_ANY))
        goto done;

    if (!F_ISSET(evict, WT_EVICT_CACHE_CLEAN) && !F_ISSET(evict, WT_EVICT_CACHE_DIRTY)) {
        if (F_ISSET(evict, WT_EVICT_CACHE_UPDATES))
            WT_STAT_CONN_INCR(session, eviction_target_strategy_updates_only);
    }

    /*
     * We iterate over bucket sets in eviction priority order from highest to lowest is:
     * 1. Clean leaf pages.
     * 2. Clean internal pages.
     * 3. Dirty leaf pages.
     * 4. Dirty internal pages.
     *
     * The iteration order of the bucket sets can be changed if a different priority is desired.
     *
     * In each bucketset we iterate over the buckets starting with the smallest, because smaller
     * buckets will have pages with smaller read generations.
     */
    if (F_ISSET(evict, WT_EVICT_CACHE_CLEAN))
        max_level = WT_EVICT_LEVEL_CLEAN_LEAF;
    if (F_ISSET(evict, WT_EVICT_CACHE_DIRTY))
        max_level = WT_EVICT_LEVEL_DIRTY_INTERNAL;
    if (F_ISSET(evict, WT_EVICT_CACHE_UPDATES))
        max_level = WT_EVICT_LEVEL_UPDATES_LEAF; // WT_EVICT_LEVEL_UPDATES_INTERNAL;

    if (!F_ISSET(evict, WT_EVICT_CACHE_CLEAN))
        min_level = WT_EVICT_LEVEL_DIRTY_LEAF;
    if (!F_ISSET(evict, WT_EVICT_CACHE_DIRTY) && !F_ISSET(evict, WT_EVICT_CACHE_CLEAN))
        min_level = WT_EVICT_LEVEL_UPDATES_LEAF;

    /* Only evict from all levels, including clean internal pages, if this is urgent */
    if (F_ISSET(evict, WT_EVICT_CACHE_URGENT)) {
        min_level = 0;
        max_level = WT_EVICT_LEVELS - 1;
        printf("URGENT EVICTION!!!!!!!!!!!!\n");
    }

    for (i = min_level; i <= max_level; i++) {
        if (!F_ISSET(conn->evict, WT_EVICT_CACHE_ANY))
            break;

        bucketset = &evict->evict_bucketset[i];
        if (bucketset->bucketset_num_items == 0)
            continue;

        num_buckets = bucketset->num_buckets;
        for (j = __wt_atomic_load_uint32_relaxed(&bucketset->bucket_last_considered) % num_buckets,
            iter = 0;
             iter++ < num_buckets; j = (j + 1) % num_buckets, total_iter++) {

            if (!F_ISSET(conn->evict, WT_EVICT_CACHE_ANY))
                break;
            bucket = &bucketset->buckets[j];

            if (__wt_spin_trylock(session, &bucket->evict_queue_lock) == EBUSY) {
                skip_locked++;
                WT_STAT_CONN_INCR(session, eviction_skip_page_locked_bucket);
                continue;
            }
            if (iter > 0)
                __wt_atomic_store_uint32_relaxed(&bucketset->bucket_last_considered, j);

            if (TAILQ_EMPTY(&bucket->evict_queue))
                WT_STAT_CONN_INCR(session, eviction_skip_empty_bucket);

            /* Iterate over the pages in the bucket until we find one that's available. */
            TAILQ_FOREACH (page, &bucket->evict_queue, evict_data.evict_q) {
                if (!F_ISSET(conn->evict, WT_EVICT_CACHE_ANY))
                    break;
                ref = page->ref;
                WT_ASSERT(session, ref != NULL);
                /*
                 * Pages created during splits may end up in the eviction data structures before
                 * their home gets set. This is the same check as we make for the root page. Skip
                 * them until their home gets set or if this is a true root. Keep this check here,
                 * because split pages spend a small window of time without a home, which makes them
                 * look like a root page. This condition is temporary as the home gets set quickly
                 * after.
                 */
                if (__wt_ref_is_root(ref)) {
                    ref = NULL;
                    continue;
                }

                /*
                 * We have the bucket locked. Even if someone is looking at the page they cannot be
                 * removing it from the bucket and hence from the tree. So we can access its dhandle
                 * attribute.
                 */
                if (WT_BTREE_SYNCING((WT_BTREE *)page->evict_data.dhandle->handle)) {
                    ref = NULL;
                    early_skipped_tree++;
                    continue;
                }

                /* Try to lock the reference. If it's already locked, skip it. */
                previous_state = WT_REF_GET_STATE(ref);
                WT_ASSERT(session, previous_state == WT_REF_LOCKED || previous_state == WT_REF_MEM);
                if (previous_state == WT_REF_LOCKED) {
                    WT_STAT_CONN_INCR(session, eviction_skip_page_locked);
                    ref = NULL;
                    continue;
                } else if (previous_state == WT_REF_MEM) {
                    if (!WT_REF_CAS_STATE(session, ref, previous_state, WT_REF_LOCKED)) {
                        WT_STAT_CONN_INCR(session, eviction_skip_page_locked);
                        ref = NULL;
                        continue;
                    }
                }

                /*
                 * If we are here, we have a ref and it is locked. Make sure we unlock it if we
                 * decide to skip.
                 */
                (void)__wt_atomic_add_int32(&page->evict_data.dhandle->session_inuse, 1);
                WT_WITH_DHANDLE(session, page->evict_data.dhandle,
                  skip_page = __evict_skip_page(session, ref, (int)i));
                if (skip_page) {
                    skipped++;
                    WT_REF_UNLOCK(ref, previous_state);
                    ref = NULL;
                    (void)__wt_atomic_sub_int32(&page->evict_data.dhandle->session_inuse, 1);
                    continue;
                } else {
                    /*
                     * We are almost ready to take this reference for eviciton. Check that eviction
                     * hasn't been disabled while we were checking it.
                     */
                    *btreep = ref->page->evict_data.dhandle->handle;
                    (void)__wt_atomic_add_uint32_v(&((*btreep)->evict_data.evict_busy), 1);
                    if (__wt_atomic_load_int32_relaxed(&(*btreep)->evict_data.evict_disabled) > 0) {
                        printf("Late evict_disabled check\n");
                        WT_REF_UNLOCK(ref, previous_state);
                        ref = NULL;
                        (void)__wt_atomic_sub_uint32_v(&((*btreep)->evict_data.evict_busy), 1);
                        (void)__wt_atomic_sub_int32(&page->evict_data.dhandle->session_inuse, 1);
                        continue;
                    } else
                        goto unlock_bucket_and_done;
                }
            }
unlock_bucket_and_done:
            if (ref != NULL) {
                TAILQ_REMOVE(&bucket->evict_queue, page, evict_data.evict_q);
                page->evict_data.bucket = NULL;
            }
            __wt_spin_unlock(session, &bucket->evict_queue_lock);
            if (ref != NULL)
                goto done;
        }
    }
done:
    if (ref != NULL) {
        *previous_statep = previous_state;
        *refp = ref;

        /* Decrement items in the bucketset where the page came from */
        __wt_atomic_sub_uint64(&bucketset->bucketset_num_items, 1);
        (void)__wt_atomic_sub_int32(&page->evict_data.dhandle->session_inuse, 1);

#if PRINT_CACHE_STATE
        if (total_iter > 1000) {
            printf("Server read_gen is %" PRIu64
                   ". Evict flags: %d. Found ref in %d iterations at level %s. Min_level %d, "
                   "max_level = %d\n",
              evict->read_gen, (int)evict->flags, (int)total_iter, __evict_level_to_string(i),
              (int)min_level, (int)max_level);

            empty_buckets = 0;
            for (i = 0; i < bucketset->num_buckets; i++) {
                if (TAILQ_EMPTY(&bucketset->buckets[i].evict_queue))
                    empty_buckets++;
            }

            printf("At level %d, %d buckets empty\n", (int)bucketset->level, empty_buckets);

            for (i = 0; i < WT_EVICT_LEVELS; i++) {
                total_items += evict->evict_bucketset[i].bucketset_num_items;
                printf("level [%d][%s]: %" PRIu64 " items.\n", (int)i, __evict_level_to_string(i),
                  evict->evict_bucketset[i].bucketset_num_items);
            }
            printf("Total pages:  %" PRIu64 ", %" PRIu64 " dirty bytes, %" PRIu64
                   " update bytes, %" PRIu64 " total pages,  %" PRIu64 " total bytes images\n",
              total_items, __wt_cache_dirty_inuse(cache), __wt_cache_bytes_updates(cache),
              __wt_cache_pages_inuse(cache), __wt_cache_bytes_image(cache));
        }
#endif
    } else {
        WT_STAT_CONN_INCR(session, eviction_get_ref_empty);
    }

    ret = (*refp == NULL ? WT_NOTFOUND : 0);
    return (ret);
}

/*
 * __evict_page --
 *     Called by both eviction and application threads to evict a page.
 */
static int
__evict_page(WT_SESSION_IMPL *session)
{
    WT_BTREE *btree;
    WT_DECL_RET;
    WT_REF *ref;
    WT_REF_STATE previous_state;
    WT_TRACK_OP_DECL;
    uint32_t flags;

    WT_TRACK_OP_INIT(session);

    flags = 0;

    WT_RET_TRACK(__evict_get_ref(session, &btree, &ref, &previous_state));
    WT_ASSERT(session, (WT_REF_GET_STATE(ref) == WT_REF_LOCKED && WT_REF_OWNER(ref) == session));

    WT_WITH_BTREE(session, btree, ret = __wt_evict(session, ref, previous_state, flags));

    (void)__wt_atomic_sub_uint32_v(&btree->evict_data.evict_busy, 1);

    if (WT_UNLIKELY(ret != 0)) {
        WT_STAT_CONN_INCR(session, eviction_worker_evict_fail);
    } else
        __wt_atomic_add_uint64(&S2C(session)->evict->evicted_pages, 1);

    WT_TRACK_OP_END(session);
    return (ret);
}

/* !!!
 * __wt_evict_page_urgent --
 *     Push a page into the urgent eviction queue.
 *
 *     It is called by the btree code releasing the reference to the page.
 */
void
__wt_evict_page_urgent(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_ASSERT(session, session->dhandle != NULL);
    __wt_evict_touch_page(session, ref, false, true /* won't need */);
    if (WT_EVICT_HAS_WORKERS(session))
        __wt_cond_signal(session, S2C(session)->evict_threads.wait_cond);
    else
        __wt_evict_server_wake(session);
}

/* !!!
 * __wt_evict_priority_set --
 *     Set a tree's eviction priority. A higher priority indicates less likelihood for the tree to
 *     be considered for eviction. The eviction server skips the eviction of trees with a non-zero
 *     priority unless eviction is in an aggressive state and the Btree is significantly utilizing
 *     the cache.
 *
 *     At present, it is exclusively called for metadata and bloom filter files, as these are meant
 *     to be retained in the cache.
 *
 *     Input parameter:
 *       `v`: An integer that denotes the priority level.
 */
void
__wt_evict_priority_set(WT_SESSION_IMPL *session, uint64_t v)
{
    S2BT(session)->evict_data.evict_priority = v;
}

/*
 * __wt_evict_priority_clear --
 *     Clear a tree's eviction priority to zero. It is called during the closure of the
 *     dhandle/btree.
 */
void
__wt_evict_priority_clear(WT_SESSION_IMPL *session)
{
    S2BT(session)->evict_data.evict_priority = 0;
}

/*
 * __verbose_dump_cache_single --
 *     Output diagnostic information about a single file in the cache.
 */
static int
__verbose_dump_cache_single(WT_SESSION_IMPL *session, uint64_t *total_bytesp,
  uint64_t *total_dirty_bytesp, uint64_t *total_updates_bytesp)
{
    WT_DATA_HANDLE *dhandle;
    WT_PAGE *page;
    WT_REF *next_walk;
    size_t size;
    uint64_t intl_bytes, intl_bytes_max, intl_dirty_bytes;
    uint64_t intl_dirty_bytes_max, intl_dirty_pages, intl_pages;
    uint64_t leaf_bytes, leaf_bytes_max, leaf_dirty_bytes;
    uint64_t leaf_dirty_bytes_max, leaf_dirty_pages, leaf_pages, updates_bytes;

    intl_bytes = intl_bytes_max = intl_dirty_bytes = 0;
    intl_dirty_bytes_max = intl_dirty_pages = intl_pages = 0;
    leaf_bytes = leaf_bytes_max = leaf_dirty_bytes = 0;
    leaf_dirty_bytes_max = leaf_dirty_pages = leaf_pages = 0;
    updates_bytes = 0;

    dhandle = session->dhandle;

    /*
     * We cannot walk the tree of a dhandle held exclusively because the owning thread could be
     * manipulating it in a way that causes us to dump core. So print out that we visited and
     * skipped it.
     */
    if (F_ISSET(dhandle, WT_DHANDLE_EXCLUSIVE))
        return (__wt_msg(session, " handle opened exclusively, cannot walk tree, skipping"));

    next_walk = NULL;
    while (__wt_tree_walk(session, &next_walk,
             WT_READ_CACHE | WT_READ_NO_EVICT | WT_READ_NO_WAIT | WT_READ_VISIBLE_ALL) == 0 &&
      next_walk != NULL) {
        page = next_walk->page;
        size = __wt_atomic_load_size(&page->memory_footprint);

        if (F_ISSET(next_walk, WT_REF_FLAG_INTERNAL)) {
            ++intl_pages;
            intl_bytes += size;
            intl_bytes_max = WT_MAX(intl_bytes_max, size);
            if (__wt_page_is_modified(page)) {
                ++intl_dirty_pages;
                intl_dirty_bytes += size;
                intl_dirty_bytes_max = WT_MAX(intl_dirty_bytes_max, size);
            }
        } else {
            ++leaf_pages;
            leaf_bytes += size;
            leaf_bytes_max = WT_MAX(leaf_bytes_max, size);
            if (__wt_page_is_modified(page)) {
                ++leaf_dirty_pages;
                leaf_dirty_bytes += size;
                leaf_dirty_bytes_max = WT_MAX(leaf_dirty_bytes_max, size);
            }
            if (page->modify != NULL)
                updates_bytes += page->modify->bytes_updates;
        }
    }

    if (intl_pages == 0)
        WT_RET(__wt_msg(session, "internal: 0 pages"));
    else
        WT_RET(
          __wt_msg(session,
            "internal: "
            "%" PRIu64 " pages, %.2f KB, "
            "%" PRIu64 "/%" PRIu64 " clean/dirty pages, "
            "%.2f/%.2f clean / dirty KB, "
            "%.2f KB max page, "
            "%.2f KB max dirty page ",
            intl_pages, (double)intl_bytes / WT_KILOBYTE, intl_pages - intl_dirty_pages,
            intl_dirty_pages, (double)(intl_bytes - intl_dirty_bytes) / WT_KILOBYTE,
            (double)intl_dirty_bytes / WT_KILOBYTE, (double)intl_bytes_max / WT_KILOBYTE,
            (double)intl_dirty_bytes_max / WT_KILOBYTE));
    if (leaf_pages == 0)
        WT_RET(__wt_msg(session, "leaf: 0 pages"));
    else
        WT_RET(
          __wt_msg(session,
            "leaf: "
            "%" PRIu64 " pages, %.2f KB, "
            "%" PRIu64 "/%" PRIu64 " clean/dirty pages, "
            "%.2f /%.2f /%.2f clean/dirty/updates KB, "
            "%.2f KB max page, "
            "%.2f KB max dirty page",
            leaf_pages, (double)leaf_bytes / WT_KILOBYTE, leaf_pages - leaf_dirty_pages,
            leaf_dirty_pages, (double)(leaf_bytes - leaf_dirty_bytes) / WT_KILOBYTE,
            (double)leaf_dirty_bytes / WT_KILOBYTE, (double)updates_bytes / WT_KILOBYTE,
            (double)leaf_bytes_max / WT_KILOBYTE, (double)leaf_dirty_bytes_max / WT_KILOBYTE));

    *total_bytesp += intl_bytes + leaf_bytes;
    *total_dirty_bytesp += intl_dirty_bytes + leaf_dirty_bytes;
    *total_updates_bytesp += updates_bytes;

    return (0);
}

/*
 * __verbose_dump_cache_apply --
 *     Apply dumping cache for all the dhandles.
 */
static int
__verbose_dump_cache_apply(WT_SESSION_IMPL *session, uint64_t *total_bytesp,
  uint64_t *total_dirty_bytesp, uint64_t *total_updates_bytesp)
{
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle;
    WT_DECL_RET;

    conn = S2C(session);
    for (dhandle = NULL;;) {
        WT_DHANDLE_NEXT(session, dhandle, &conn->dhqh, q);
        if (dhandle == NULL)
            break;

        /* Skip if the tree is marked discarded by another thread. */
        if (!WT_DHANDLE_BTREE(dhandle) || !F_ISSET(dhandle, WT_DHANDLE_OPEN) ||
          F_ISSET(dhandle, WT_DHANDLE_DISCARD))
            continue;

        WT_WITH_DHANDLE(session, dhandle,
          ret = __verbose_dump_cache_single(
            session, total_bytesp, total_dirty_bytesp, total_updates_bytesp));
        if (ret != 0)
            WT_RET(ret);
    }
    return (0);
}

/*
 * __wt_verbose_dump_cache --
 *     Output diagnostic information about the cache.
 */
int
__wt_verbose_dump_cache(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    double pct;
    uint64_t bytes_dirty_intl, bytes_dirty_leaf, bytes_inmem;
    uint64_t cache_bytes_updates, total_bytes, total_dirty_bytes, total_updates_bytes;
    bool needed;

    conn = S2C(session);
    cache = conn->cache;
    total_bytes = total_dirty_bytes = total_updates_bytes = 0;
    pct = 0.0; /* [-Werror=uninitialized] */
    WT_NOT_READ(cache_bytes_updates, 0);

    WT_RET(__wt_msg(session, "%s", WT_DIVIDER));
    WT_RET(__wt_msg(session, "cache dump"));

    WT_RET(__wt_msg(session, "cache full: %s", __wt_cache_full(session) ? "yes" : "no"));
    needed = __wti_evict_exceeded_clean_trigger(session, &pct);
    WT_RET(__wt_msg(session, "cache clean check: %s (%2.3f%%)", needed ? "yes" : "no", pct));
    needed = __wti_evict_exceeded_dirty_trigger(session, &pct);
    WT_RET(__wt_msg(session, "cache dirty check: %s (%2.3f%%)", needed ? "yes" : "no", pct));
    needed = __wti_evict_exceeded_updates_trigger(session, &pct);
    WT_RET(__wt_msg(session, "cache updates check: %s (%2.3f%%)", needed ? "yes" : "no", pct));

    WT_WITH_HANDLE_LIST_READ_LOCK(session,
      ret = __verbose_dump_cache_apply(
        session, &total_bytes, &total_dirty_bytes, &total_updates_bytes));
    WT_RET(ret);

    /*
     * Apply the overhead percentage so our total bytes are comparable with the tracked value.
     */
    total_bytes = __wt_cache_bytes_plus_overhead(conn->cache, total_bytes);
    cache_bytes_updates = __wt_cache_bytes_updates(cache);

    bytes_inmem = __wt_atomic_load_uint64_relaxed(&cache->bytes_inmem);
    bytes_dirty_intl = __wt_atomic_load_uint64_relaxed(&cache->bytes_dirty_intl);
    bytes_dirty_leaf = __wt_atomic_load_uint64_relaxed(&cache->bytes_dirty_leaf);

    WT_RET(__wt_msg(session, "cache dump: total found: %.2f MB vs tracked inuse %.2f MB",
      (double)total_bytes / WT_MEGABYTE, (double)bytes_inmem / WT_MEGABYTE));
    WT_RET(__wt_msg(session, "total dirty bytes: %.2f MB vs tracked dirty %.2f MB",
      (double)total_dirty_bytes / WT_MEGABYTE,
      (double)(bytes_dirty_intl + bytes_dirty_leaf) / WT_MEGABYTE));
    WT_RET(__wt_msg(session, "total updates bytes: %.2f MB vs tracked updates %.2f MB",
      (double)total_updates_bytes / WT_MEGABYTE, (double)cache_bytes_updates / WT_MEGABYTE));

    return (0);
}

/*
 * __wt_evict_remove --
 *     Remove the page from its evict bucket.
 */
void
__wt_evict_remove(WT_SESSION_IMPL *session, WT_REF *ref, bool destroying)
{
    WT_PAGE *page;
    WT_REF_STATE previous_state;
    bool must_unlock_ref;

    must_unlock_ref = false;
    previous_state = 0;

    WT_ASSERT(session, ref->page != NULL);
    page = ref->page;
    if (WT_EVICT_PAGE_CLEARED(page))
        return;

    if (WT_REF_GET_STATE(ref) == WT_REF_LOCKED && WT_REF_OWNER(ref) == session) {
        /* The ref is already locked by us */
#if EVICT_DEBUG_PRINT
        printf("ref for page %p %s (type %d) already locked in __wt_evict_remove by session %d\n",
          (void *)ref->page, __wt_page_type_string(ref->page->type), ref->page->type, session->id);
        fflush(stdout);
#endif
        must_unlock_ref = false;
    } else {
#if EVICT_DEBUG_PRINT
        printf("Session %d about to LOCK ref for page %p %s (type %d) in __wt_evict_remove!\n",
          session->id, (void *)page, __wt_page_type_string(page->type), page->type);
        fflush(stdout);
#endif
        WT_REF_LOCK(session, ref, &previous_state);
        must_unlock_ref = true;
        fflush(stdout);
    }

    if (!WT_EVICT_PAGE_CLEARED(page)) {
        __wt_spin_lock(session, &page->evict_data.bucket->evict_queue_lock);
        TAILQ_REMOVE(&page->evict_data.bucket->evict_queue, page, evict_data.evict_q);
        __wt_spin_unlock(session, &page->evict_data.bucket->evict_queue_lock);

        __wt_atomic_sub_uint64(&page->evict_data.bucket->bucketset->bucketset_num_items, 1);
        page->evict_data.bucket = NULL;

        if (destroying)
            page->evict_data.destroying = true; /* sticky flag, once set can't unset */
    }

    if (must_unlock_ref)
        WT_REF_UNLOCK(ref, previous_state);
}

/*
 * __wt_evict_enqueue_page --
 *     Put the page into the evict bucket corresponding to its read generation.
 */
void
__wt_evict_enqueue_page(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_EVICT_BUCKET *bucket;
    WT_EVICT_BUCKETSET *bucketset;
    WT_PAGE *page;
    WT_REF_STATE previous_state;
    bool must_unlock_ref;

    WT_ASSERT(session, ref != NULL);
    bucket = NULL;
    page = ref->page;
    previous_state = WT_REF_GET_STATE(ref);

    /*
     * If the page isn't valid there is no need to put it into eviction data structures. We can get
     * here if the page is about to be discarded, but it is set clean before being deallocated.
     */
    if (previous_state != WT_REF_LOCKED && previous_state != WT_REF_MEM)
        return;
    /*
     * Lock the page so it doesn't disappear. We aren't evicting the page, so we don't need to check
     * for hazard pointers.
     */
    if (previous_state == WT_REF_LOCKED && WT_REF_OWNER(ref) == session)
        must_unlock_ref = false;
    else if (previous_state == WT_REF_LOCKED) {
        /*
         * Page is locked, but not by us. Chances are, someone is already enqueueing, evicting or
         * deleting it. Bail. Could there be an esoteric scenario where the page ends up absent from
         * eviction queues at all? Possibly, though I can't think of one. If this does occur, this
         * means that this page won't be eviction until it is deleted, reconciled or we close the
         * tree. These scenarios will be rare, so we won't worry about them.
         */
        return;
    } else /* We must lock */ {
        WT_REF_LOCK(session, ref, &previous_state);
        must_unlock_ref = true;
    }

    if (page->evict_data.dhandle == NULL)
        page->evict_data.dhandle = session->dhandle;

    if (__evict_get_target_destination(session, page, NULL, &bucket) == true) {
        goto done;
    } else
        __wt_evict_remove(session, ref, false);

    /* Get the right bucketset for this page */
    bucketset = bucket->bucketset;

    __wt_spin_lock(session, &bucket->evict_queue_lock);
    TAILQ_INSERT_TAIL(&bucket->evict_queue, page, evict_data.evict_q);
    __wt_spin_unlock(session, &bucket->evict_queue_lock);

    page->evict_data.bucket = bucket;
    __wt_atomic_add_uint64(&bucketset->bucketset_num_items, 1);

    WT_STAT_CONN_INCR(session, eviction_enqueued_page);
done:
    if (must_unlock_ref)
        WT_REF_UNLOCK(ref, previous_state);
}

/*
 * __wt_evict_touch_page --
 *     Update a page's eviction state (read generation) when it is accessed. This function is called
 *     every time a page is touched in the cache.
 *
 * A page that is recently read will have a higher read generation and will be less likely to be
 *     evicted. This mechanism helps eviction to prioritize the order in which pages are evicted.
 *
 * Input parameters: (1) `ref`: The reference to a page whose eviction state is being updated. (2)
 *     `internal_only`: A flag indicating whether the operation is internal. If true, the read
 *     generation is not updated, as internal operations (such as compaction or eviction) should not
 *     affect the page's eviction priority. (3) `wont_need`: A flag indicating that the page will
 *     not be needed in the future. If true, the page is marked for forced eviction.
 */
void
__wt_evict_touch_page(WT_SESSION_IMPL *session, WT_REF *ref, bool internal_only, bool wont_need)
{
    WT_PAGE *page;
    bool bumped;

    page = ref->page;

    WT_ASSERT(session, page != NULL);

    /* Is this the first use of the page? */
    if (__wt_atomic_load_uint64_relaxed(&page->evict_data.read_gen) == WT_READGEN_NOTSET) {
        if (wont_need)
            __wt_atomic_store_uint64_relaxed(&page->evict_data.read_gen, WT_READGEN_WONT_NEED);
        else
            __evict_read_gen_new(session, page);
        __wt_evict_enqueue_page(session, ref);
    } else if (!internal_only) {
        bumped = __wti_evict_read_gen_bump(session, page);
        if (bumped || page->evict_data.bucket == NULL)
            __wt_evict_enqueue_page(session, ref);
    }
}

/* !!!
 * __wt_evict_page_soon --
 *     Mark the page to be evicted as soon as possible by setting the `WT_READGEN_EVICT_SOON`
 *     flag.
 *
 *     Once this flag is set, the page will be moved in the highest priorit bucket.
 *
 *     This function allows its callers to evict empty internal pages, pages exceeding a
 *     certain size, obsolete pages, pages with long skip list/update chains, among
 *     other similar cases.
 *
 *     Input parameter:
 *       `ref`: The reference to the page to be marked for soon eviction.
 */
void
__wt_evict_page_soon(WT_SESSION_IMPL *session, WT_REF *ref)
{
    __wt_atomic_store_uint64_relaxed(&ref->page->evict_data.read_gen, WT_READGEN_EVICT_SOON);
    __wt_evict_enqueue_page(session, ref);
}

/*
 * __evict_read_gen_new --
 *     Get the read generation for a new page in memory.
 */
static void
__evict_read_gen_new(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    WT_IGNORE_RET(__wti_evict_read_gen_bump(session, page));
}

void
__wt_evict_page_first_dirty(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    /*
     * In the event we dirty a page which is flagged as wont need, we update its read generation to
     * avoid evicting a dirty page prematurely.
     */
    if (__wt_atomic_load_uint64_relaxed(&page->evict_data.read_gen) == WT_READGEN_WONT_NEED)
        __evict_read_gen_new(session, page);

    /* Move the page to the right bucketset */
    if (page->ref != NULL)
        __wt_evict_enqueue_page(session, page->ref);
}

/* !!!
 * __wt_evict_page_set_clean --
 *     Update a page's eviction state when a page transitions from dirty to clean.
 */
void
__wt_evict_page_set_clean(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    /* Move the page to the right bucketset */
    if (!page->evict_data.destroying && page->ref != NULL && page->evict_data.dhandle != NULL) {
        __wt_evict_enqueue_page(session, page->ref);
        WT_STAT_CONN_INCR(session, eviction_pages_set_clean);
    }
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
            WT_STAT_CONN_INCR(session, eviction_skip_ingest_trees);
        else if (F_ISSET(btree, WT_BTREE_DISAGGREGATED))
            WT_STAT_CONN_INCR(session, eviction_skip_stable_trees);
    }
}

/*
 * __evict_skip_tree --
 *     Decide if we should skip this tree
 */
static bool
__evict_skip_tree(WT_SESSION_IMPL *session, WT_BTREE *btree)
{
    WT_EVICT *evict;

    evict = S2C(session)->evict;

    /* Skip files that don't allow eviction. */
    if (__wt_atomic_load_int32_relaxed(&btree->evict_data.evict_disabled) > 0) {
        WT_STAT_CONN_INCR(session, eviction_skip_trees_eviction_disabled);
        __evict_disagg_btree_skip_count(session, btree);
        return true;
    }

    /* Skip read-only btrees if we are not looking for clean pages. */
    if (F_ISSET(btree, WT_BTREE_READONLY) && !F_ISSET(evict, WT_EVICT_CACHE_CLEAN)) {
        WT_STAT_CONN_INCR(session, eviction_skip_trees_read_only);
        __evict_disagg_btree_skip_count(session, btree);
        return true;
    }

    /*
     * Skip files that are checkpointing if we are only looking for dirty pages.
     */
    if (WT_BTREE_SYNCING(btree) && !F_ISSET(evict, WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_UPDATES)) {
        WT_STAT_CONN_INCR(session, eviction_skip_checkpointing_trees);
        __evict_disagg_btree_skip_count(session, btree);
        return true;
    }

    /*
     * Skip files that are configured to stick in cache until we become aggressive.
     *
     * If the file is contributing heavily to our cache usage then ignore the "stickiness" of its
     * pages.
     */
    if (btree->evict_data.evict_priority != 0 && !__wt_evict_aggressive(session) &&
      !__evict_btree_dominating_cache(session, btree)) {
        WT_STAT_CONN_INCR(session, eviction_skip_trees_stick_in_cache);
        __evict_disagg_btree_skip_count(session, btree);
        return true;
    }

    return (false);
}

/*
 * __evict_skip_page --
 *     Decide if we should skip this page for eviction.
 */
static bool
__evict_skip_page(WT_SESSION_IMPL *session, WT_REF *ref, int level)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    WT_PAGE *page;
    bool modified;

    btree = S2BT(session);
    conn = S2C(session);
    evict = conn->evict;
    page = ref->page;
    modified = __wt_page_is_modified(page);

    if (page->evict_data.evict_skip) {
        /*
         * We are skipping the page, because we recently skipped it and the skip flag was set.
         * Reset, the flag, so we don't skip it all the time.
         */
        page->evict_data.evict_skip = false;
        WT_STAT_CONN_INCR(session, eviction_skip_page_again);
        return true;
    }

    /*
     * Don't attempt eviction of internal pages with children in cache.
     */
    if (F_ISSET(ref, WT_REF_FLAG_INTERNAL) && level != WT_EVICT_LEVEL_UPDATES_INTERNAL &&
      __evict_internal_page_has_cached_children(session, ref)) {
        WT_STAT_CONN_INCR(session, eviction_skip_intl_page_with_active_child);
        return (true);
    }

    /* Don't queue dirty pages in trees during checkpoints. */
    if (WT_BTREE_SYNCING(btree) && __wt_page_is_modified(ref->page) && ref->page->modify == NULL) {
        WT_STAT_CONN_INCR(session, eviction_skip_dirty_pages_during_checkpoint);
        return (true);
    }

    if (__evict_skip_tree(session, btree))
        return (true);

    /*
     * Do not evict a clean metadata page that contains historical data needed to satisfy a reader.
     * Since there is no history store for metadata, we won't be able to serve an older reader if we
     * evict this page.
     */
    if (WT_IS_METADATA(session->dhandle) && F_ISSET(evict, WT_EVICT_CACHE_CLEAN_HARD) &&
      F_ISSET(ref, WT_REF_FLAG_LEAF) && !modified && page->modify != NULL &&
      !__wt_txn_visible_all(session, page->modify->rec_max_txn, page->modify->rec_max_timestamp)) {
        WT_STAT_CONN_INCR(session, eviction_skip_metatdata_with_history);
        return (true);
    }

    /* Evaluate dirty page candidacy, when eviction is not aggressive. */
    if (!__wt_evict_aggressive(session) && modified &&
      __evict_skip_dirty_candidate(session, page)) {
        WT_STAT_CONN_INCR(session, eviction_skip_page_dirty_not_aggressive);
        return (true);
    }

    /* If the page can't be evicted, give up. */
    if (!__wt_page_can_evict(session, ref, NULL)) {
        WT_STAT_CONN_INCR(session, eviction_skip_page_cannot_evict);
        return (true);
    }

    if (__wt_hazard_check(session, ref, NULL) != NULL) {
        WT_STAT_CONN_INCR(session, eviction_skip_page_hazard);
        return true;
    }

    return (false);
}

/*
 * __evict_internal_page_has_cached_children --
 *     Check if the internal page has children in cache.
 */
static bool
__evict_internal_page_has_cached_children(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_PAGE_INDEX *pindex;
    uint32_t slot;
    bool has_cached_children;

    has_cached_children = false;

    WT_ENTER_PAGE_INDEX(session);
    WT_INTL_INDEX_GET(session, ref->page, pindex);

    for (slot = 0; slot < pindex->entries; slot++) {
        if (WT_REF_GET_STATE(pindex->index[slot]) == WT_REF_MEM) {
            has_cached_children = true;
            break;
        }
    }
    WT_LEAVE_PAGE_INDEX(session);
    return (has_cached_children);
}
