/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
static void __evict_choose_dhandle(WT_SESSION_IMPL *session, WT_DATA_HANDLE **dhandle_p);
static bool __evict_internal_page_has_cached_children(WT_SESSION_IMPL *sesison, WT_REF *ref);
static int __evict_lru_pages(WT_SESSION_IMPL *session);
static int __evict_page(WT_SESSION_IMPL *session);
static bool __evict_page_consistency_check(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle,
										   WT_PAGE *page, bool verbose);
static void __evict_read_gen_new(WT_SESSION_IMPL *session, WT_PAGE *page);
static bool __evict_skip_page(WT_SESSION_IMPL *session, WT_REF *ref);

/*
 * __evict_log_cache_stuck --
 *     Output log messages if the cache is stuck.
 */
static int
__evict_log_cache_stuck(WT_SESSION_IMPL *session)
{
    struct timespec now;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    uint64_t time_diff_ms;

    conn = S2C(session);
    evict = conn->evict;

    if (!__wt_evict_cache_stuck(session)) {
        evict->last_eviction_progress = 0; /* Make sure we'll notice next time we're stuck. */
        return (0);
    }

    evict->last_eviction_progress = __wt_atomic_loadv64(&evict->eviction_progress);

    /* Eviction is stuck, check if we have made progress. */
    if (__wt_atomic_loadv64(&evict->eviction_progress) != evict->last_eviction_progress) {
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
 * __evict_lock_handle_list --
 *     Try to get the handle list lock, with yield and sleep back off. Keep timing statistics
 *     overall.
 */
static int
__evict_lock_handle_list(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_RWLOCK *dh_lock;
    u_int spins;

    conn = S2C(session);
    dh_lock = &conn->dhandle_lock;

    /*
     * Use a custom lock acquisition back off loop so the eviction server notices any interrupt
     * quickly.
     */
    for (spins = 0; (ret = __wt_try_readlock(session, dh_lock)) == EBUSY; spins++) {
        if (spins < WT_THOUSAND)
            __wt_yield();
        else
            __wt_sleep(0, WT_THOUSAND);
    }
    return (ret);
}

/*
 * __evict_thread_chk --
 *     Check to decide if the eviction thread should continue running.
 */
static bool
__evict_thread_chk(WT_SESSION_IMPL *session)
{
    return (F_ISSET(S2C(session), WT_CONN_EVICTION_RUN));
}

/*
 * __evict_thread_run --
 *     Entry function for an eviction thread. This is called repeatedly from the thread group code
 *     so it does not need to loop itself.
 */
static int
__evict_thread_run(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_DECL_RET;

    (void)thread;

    /* Mark the session as an eviction thread session. */
    F_SET(session, WT_SESSION_EVICTION);

    /*
     * Cache a history store cursor to avoid deadlock: if an eviction thread marks a file busy and
     * then opens a different file (in this case, the HS file), it can deadlock with a thread
     * waiting for the first file to drain from the eviction queue. See WT-5946 for details.
     */
    WT_ERR(__wt_curhs_cache(session));

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "eviction thread starting");
    WT_ERR(__evict_lru_pages(session));

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
    F_SET(conn, WT_CONN_EVICTION_RUN);

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

    /*
     * Allow queues to be populated now that the eviction threads are running.
     */
    __wt_atomic_storebool(&conn->evict_server_running, true);

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
    if (!__wt_atomic_loadbool(&conn->evict_server_running))
        return (0);

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "stopping eviction threads");

    /* Wait for any eviction thread group changes to stabilize. */
    __wt_writelock(session, &conn->evict_threads.lock);

    /*
     * Signal the threads to finish and stop populating the queue.
     */
    F_CLR(conn, WT_CONN_EVICTION_RUN);
    __wt_atomic_storebool(&conn->evict_server_running, false);

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "waiting for eviction threads to stop");

    /*
     * We call the destroy function still holding the write lock. It assumes it is called locked.
     */
    WT_RET(__wt_thread_group_destroy(session, &conn->evict_threads));

    return (0);
}

/*
 * __evict_update_work --
 *     Configure eviction work state.
 */
static bool
__evict_update_work(WT_SESSION_IMPL *session)
{
    WT_BTREE *hs_tree;
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    double dirty_target, dirty_trigger, target, trigger, updates_target, updates_trigger;
    uint64_t bytes_dirty, bytes_inuse, bytes_max, bytes_updates;
    uint32_t flags;

    conn = S2C(session);
    cache = conn->cache;
    evict = conn->evict;

    bytes_dirty = __wt_cache_dirty_leaf_inuse(cache);
    bytes_inuse = __wt_cache_bytes_inuse(cache);
    bytes_max = conn->cache_size + 1;
    bytes_updates = __wt_cache_bytes_updates(cache);
    dirty_target = __wti_evict_dirty_target(evict);
    dirty_trigger = evict->eviction_dirty_trigger;
    target = evict->eviction_target;
    trigger = evict->eviction_trigger;
    updates_target = evict->eviction_updates_target;
    updates_trigger = evict->eviction_updates_trigger;

    /* Build up the new state. */
    flags = 0;

    if (!F_ISSET(conn, WT_CONN_EVICTION_RUN)) {
        __wt_atomic_store32(&evict->flags, 0);
        return (false);
    }

    /*
     * TODO: We are caching the cache usage values associated with the history store because the
     * history store dhandle isn't always available to eviction. Keeping potentially out-of-date
     * values could lead to surprising bugs in the future.
     */
    if (F_ISSET(conn, WT_CONN_HS_OPEN) && __wt_hs_get_btree(session, &hs_tree) == 0) {
        __wt_atomic_store64(&cache->bytes_hs, __wt_atomic_load64(&hs_tree->bytes_inmem));
        cache->bytes_hs_dirty = hs_tree->bytes_dirty_intl + hs_tree->bytes_dirty_leaf;
    }

    /*
     * If we need space in the cache, try to find clean pages to evict.
     */
    if (__wti_evict_exceeded_clean_trigger(session, NULL))
        LF_SET(WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_CLEAN_HARD);
    else if (__wti_evict_exceeded_clean_target(session))
        LF_SET(WT_EVICT_CACHE_CLEAN);

    if (__wti_evict_exceeded_dirty_trigger(session, NULL))
        LF_SET(WT_EVICT_CACHE_DIRTY | WT_EVICT_CACHE_DIRTY_HARD);
    else if (__wti_evict_exceeded_dirty_target(session))
        LF_SET(WT_EVICT_CACHE_DIRTY);

    if (__wti_evict_exceeded_updates_trigger(session, NULL))
        LF_SET(WT_EVICT_CACHE_UPDATES | WT_EVICT_CACHE_UPDATES_HARD);
    else if (__wti_evict_exceeded_updates_target(session))
        LF_SET(WT_EVICT_CACHE_UPDATES);

    /*
     * If application threads are blocked by the total volume of data in cache, try dirty pages as
     * well.
     */
    if (__wt_evict_aggressive(session) && LF_ISSET(WT_EVICT_CACHE_CLEAN_HARD))
        LF_SET(WT_EVICT_CACHE_DIRTY);

    if (!F_ISSET(evict, WT_EVICT_CACHE_DIRTY | WT_EVICT_CACHE_UPDATES))
        WT_STAT_CONN_INCR(session, eviction_target_strategy_clean);
    else if (!F_ISSET(evict, WT_EVICT_CACHE_CLEAN)) {
        WT_STAT_CONN_INCR(session, eviction_target_strategy_dirty);
    } else
        WT_STAT_CONN_INCR(session, eviction_target_strategy_both_clean_and_dirty);

    /*
     * Scrub dirty pages and keep them in cache if we are less than half way to the clean, dirty or
     * updates triggers. TODO: why these thresholds? Shall we always keep scrubbed pages?
     */
    if (bytes_inuse < (uint64_t)((target + trigger) * bytes_max) / 200) {
        if (bytes_dirty < (uint64_t)((dirty_target + dirty_trigger) * bytes_max) / 200 &&
          bytes_updates < (uint64_t)((updates_target + updates_trigger) * bytes_max) / 200)
            LF_SET(WT_EVICT_CACHE_SCRUB);
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
    __wt_atomic_store32(&evict->flags, flags);

    return (F_ISSET(evict, WT_EVICT_CACHE_ANY | WT_EVICT_CACHE_URGENT));
}

/* !!!
 * __wt_evict_file_exclusive_on --
 *     Acquire exclusive access to a file/tree making it possible to evict the entire file using
 *     `__wt_evict_file`. It does this by incrementing the `evict_disabled` counter for a
 *     tree, which disables all other means of eviction (except file eviction).
 *
 *     For the incremented `evict_disabled` value, the eviction workers skip this tree for
 *     eviction candidates, and force-evicting or queuing pages from this tree is not allowed.
 *
 *     It is called from multiple places in the code base, such as when initiating file eviction
 *     `__wt_evict_file` or when opening or closing trees.
 *
 *     Return an error code if unable to acquire necessary locks.
 */
int
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
		printf("In exclusive_on: evict_disabled for %s is %d\n",
			   btree->dhandle->name, btree->evict_data.evict_disabled);
        return (0);
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
    return (ret);
}

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
	printf("In exclusive_off: evict_disabled for %s is %d\n",
		   btree->dhandle->name, btree->evict_data.evict_disabled);
#if defined(HAVE_DIAGNOSTIC)
    WT_ASSERT(session, btree->evict_data.evict_disabled >= 0);
#endif
    __wt_spin_unlock(session, &evict->evict_exclusive_lock);
    __wt_verbose_debug1(session, WT_VERB_EVICTION, "released exclusive eviction lock on btree %s",
      btree->dhandle->name);
}

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
        thread_surplus = (int32_t)__wt_atomic_load32(&conn->evict_threads.current_threads) -
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
    eviction_progress = __wt_atomic_loadv64(&evict->eviction_progress);

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
        evict->evict_tune_workers_best = __wt_atomic_load32(&conn->evict_threads.current_threads);
    }

    /*
     * Compare the current number of data points with the number needed variable. If they are equal,
     * we will check whether we are still going up on the performance curve, in which case we will
     * increase the number of needed data points, to provide opportunity for further increasing the
     * number of workers. Or we are past the inflection point on the curve, in which case we will go
     * back to the best observed number of workers and settle into a stable state.
     */
    if (evict->evict_tune_num_points >= evict->evict_tune_datapts_needed) {
        current_threads = __wt_atomic_load32(&conn->evict_threads.current_threads);
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
            thread_surplus = (int32_t)__wt_atomic_load32(&conn->evict_threads.current_threads) -
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
        cur_threads = (int32_t)__wt_atomic_load32(&conn->evict_threads.current_threads);
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
 * __evict_lru_pages --
 *     Get pages from the LRU queue to evict. This function will be called repeatedly via the thread
 *     group until the threads are told to exit.
 */
static int
__evict_lru_pages(WT_SESSION_IMPL *session)
{
    WT_CACHE *cache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_TRACK_OP_DECL;
    WT_TXN_GLOBAL *txn_global;
    uint64_t eviction_progress, oldest_id, prev_oldest_id;
    uint64_t time_now, time_prev;
    u_int loop;

    WT_TRACK_OP_INIT(session);
    conn = S2C(session);
    cache = conn->cache;
    evict = conn->evict;
    txn_global = &conn->txn_global;
    time_prev = 0; /* [-Wconditional-uninitialized] */

    /* Track whether pages are being evicted and progress is made. */
    eviction_progress = __wt_atomic_loadv64(&evict->eviction_progress);
    prev_oldest_id = __wt_atomic_loadv64(&txn_global->oldest_id);

    /* Evict pages from the cache. */
    for (loop = 0;; loop++) {
        time_now = __wt_clock(session);
        if (loop == 0)
            time_prev = time_now;

        __evict_tune_workers(session);
        /*
         * Increment the shared read generation. Do this occasionally even if eviction is not
         * currently required, so that pages have some relative read generation when the eviction
         * server does need to do some work.
         */
        __wt_atomic_add64(&evict->read_gen, 1);
        __wt_atomic_add64(&evict->evict_pass_gen, 1);

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

        if (!__evict_update_work(session))
            break;

        __wt_verbose_debug2(session, WT_VERB_EVICTION,
          "Eviction pass with: Max: %" PRIu64 " In use: %" PRIu64 " Dirty: %" PRIu64
          " Updates: %" PRIu64,
          conn->cache_size, __wt_atomic_load64(&cache->bytes_inmem),
          __wt_atomic_load64(&cache->bytes_dirty_intl) +
            __wt_atomic_load64(&cache->bytes_dirty_leaf),
          __wt_atomic_load64(&cache->bytes_updates));

        /* Try to evict a page */
        if ((ret = __evict_page(session)) == EBUSY)
            ret = 0;

        /*
         * If we're making progress, keep going; if we're not making any progress at all, mark the
         * cache "stuck" and go back to sleep, it's not something we can fix.
         *
         * We check for progress every 20ms, the idea being that the aggressive score will reach 10
         * after 200ms if we aren't making progress and eviction will start considering more pages.
         * If there is still no progress after 2s, we will treat the cache as stuck and start
         * rolling back transactions and writing updates to the history store table.
         */
        if (eviction_progress == __wt_atomic_loadv64(&evict->eviction_progress)) {
            if (WT_CLOCKDIFF_MS(time_now, time_prev) >= 20 && F_ISSET(evict, WT_EVICT_CACHE_HARD)) {
                if (__wt_atomic_load32(&evict->evict_aggressive_score) < WT_EVICT_SCORE_MAX)
                    (void)__wt_atomic_addv32(&evict->evict_aggressive_score, 1);
                oldest_id = __wt_atomic_loadv64(&txn_global->oldest_id);
                if (prev_oldest_id == oldest_id &&
                  __wt_atomic_loadv64(&txn_global->current) != oldest_id &&
                  __wt_atomic_load32(&evict->evict_aggressive_score) < WT_EVICT_SCORE_MAX)
                    (void)__wt_atomic_addv32(&evict->evict_aggressive_score, 1);
                time_prev = time_now;
                prev_oldest_id = oldest_id;
            }

            /*
             * Keep trying for long enough that we should be able to evict a page.
             */
            if (loop < 100 ||
              __wt_atomic_load32(&evict->evict_aggressive_score) < WT_EVICT_SCORE_MAX) {
                /*
                 * Back off if we aren't making progress.
                 */
                WT_STAT_CONN_INCR(session, eviction_slept);
                __wt_cond_wait(session, conn->evict_threads.wait_cond, WT_THOUSAND, NULL);
                continue;
            }

            WT_STAT_CONN_INCR(session, eviction_slow);
            __wt_verbose_debug1(session, WT_VERB_EVICTION, "%s", "unable making slow progress");
            break;
        }
        if (__wt_atomic_load32(&evict->evict_aggressive_score) > 0)
            (void)__wt_atomic_subv32(&evict->evict_aggressive_score, 1);
        loop = 0;
        eviction_progress = __wt_atomic_loadv64(&evict->eviction_progress);
    }

    /* Check if the cache is stuck and write messages to the log */
    __evict_log_cache_stuck(session);

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

    if (__wt_cache_bytes_plus_overhead(cache, __wt_atomic_load64(&btree->bytes_inmem)) >
      (uint64_t)(0.5 * evict->eviction_target * bytes_max) / 100)
        return (true);

    bytes_dirty =
      __wt_atomic_load64(&btree->bytes_dirty_intl) + __wt_atomic_load64(&btree->bytes_dirty_leaf);
    if (__wt_cache_bytes_plus_overhead(cache, bytes_dirty) >
      (uint64_t)(0.5 * evict->eviction_dirty_target * bytes_max) / 100)
        return (true);
    if (__wt_cache_bytes_plus_overhead(cache, __wt_atomic_load64(&btree->bytes_updates)) >
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
        if (!__txn_visible_id(session, __wt_atomic_load64(&page->modify->update_txn)))
            return (true);
    } else if (__wt_atomic_load64(&page->modify->update_txn) >=
      __wt_atomic_loadv64(&conn->txn_global.last_running)) {
        WT_STAT_CONN_INCR(session, eviction_skip_pages_last_running);
        return (true);
    }

    return (false);
}

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
    WT_DATA_HANDLE *dhandle;
	WT_DECL_RET;
	WT_EVICT *evict;
    WT_EVICT_BUCKET *bucket;
    WT_EVICT_BUCKETSET *bucketset;
	WT_PAGE *page;
    WT_REF *ref;
    WT_REF_STATE previous_state;
    int i, j, max_level;
    bool dhandle_list_locked;
	uint64_t loop_count;

    *btreep = NULL;
	bucketset = NULL;
    conn = S2C(session);
	dhandle = NULL;
	dhandle_list_locked = false;
	evict = conn->evict;
	j = 0;
	loop_count = 0;
	max_level = 0;
	previous_state = 0;
    /*
     * It is polite to initialize output variables, but it isn't safe for callers to use the
     * previous state if we don't return a locked ref.
     */
    *previous_statep = WT_REF_MEM;
    *refp = ref = NULL;

    /*
     * Pick a dhandle from which to evict. The function picking the handle dictates the priority
     * order for eviction.
     */
    while (loop_count++ < conn->dhandle_count) {
        /* We're done if shutting down or reconfiguring. */
        if (F_ISSET(conn, WT_CONN_CLOSING) || F_ISSET(conn, WT_CONN_RECONFIGURING))
            goto done;

        /*
         * Lock the dhandle list to find the next handle and bump its reference count to keep it
         * alive while we use it.
         */
        if (!dhandle_list_locked) {
            WT_ERR(__evict_lock_handle_list(session));
            dhandle_list_locked = true;
        }
        __evict_choose_dhandle(session, &dhandle);
        if (dhandle == NULL)
            continue;
    }
    __wt_readunlock(session, &conn->dhandle_lock);
    dhandle_list_locked = false;

    if (dhandle == NULL) {
        ret = WT_NOTFOUND;
        WT_STAT_CONN_INCR(session, eviction_get_ref_no_dhandle);
        goto err;
    } else
        (void)__wt_atomic_addi32(&dhandle->session_inuse, 1);

    WT_ASSERT(session, WT_DHANDLE_BTREE(dhandle));
    WT_ASSERT(session, ((WT_BTREE*)(dhandle->handle))->evict_data.initialized);

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
        max_level = WT_EVICT_LEVEL_CLEAN_INTERNAL;
    if (F_ISSET(evict, WT_EVICT_CACHE_DIRTY))
        max_level = WT_EVICT_LEVEL_DIRTY_INTERNAL;

    for (i = WT_EVICT_LEVEL_CLEAN_LEAF; i <= max_level; i++) {
        bucketset = WT_DHANDLE_TO_BUCKETSET(dhandle, i);
        for (j = 0; j < WT_EVICT_NUM_BUCKETS; j++) {
            bucket = &bucketset->buckets[j];
            if (__wt_atomic_load64(&bucket->num_items) == 0)
                continue;
            __wt_spin_lock(session, &bucket->evict_queue_lock);
            /* Iterate over the pages in the bucket until we find one that's available. */
            TAILQ_FOREACH (page, &bucket->evict_queue, evict_data.evict_q) {
				ref = page->ref;
				WT_ASSERT(session, ref != NULL);
                /* Try to lock the reference. If it's already locked, skip it. */
                if ((previous_state = WT_REF_GET_STATE(ref)) != WT_REF_MEM ||
                  !WT_REF_CAS_STATE(session, ref, previous_state, WT_REF_LOCKED)) {
                    WT_STAT_CONN_INCR(session, eviction_skip_pages_locked_or_evicted);
                    __wt_verbose_notice(session, WT_VERB_EVICTION, "%s",
                      "eviction skipped a page because it was locked");
                    ref = NULL;
                    continue;
                }
                /*
                 * If we are here, we have a ref and it is locked. Make sure we unlock it if we
                 * decide to skip.
                 */
                if (__wt_atomic_loadbool(&ref->page->evict_data.evict_skip)) {
                    /*
                     * We are skipping the page, because we recently skipped it and the skip flag
                     * was set. Reset, the flag, so we don't skip it all the time.
                     */
                    __wt_atomic_storebool(&ref->page->evict_data.evict_skip, false);
                    WT_REF_UNLOCK(ref, previous_state);
                    ref = NULL;

                    WT_STAT_CONN_INCR(session, eviction_skip_pages_flag);
                    __wt_verbose_debug1(session, WT_VERB_EVICTION, "%s",
                      "eviction skipped a page because skip flag was set");

                    continue;
                } else {
                    bool skip_page;
                    WT_WITH_DHANDLE(session, dhandle, skip_page = __evict_skip_page(session, ref));
                    if (skip_page) {
                        WT_REF_UNLOCK(ref, previous_state);
                        ref = NULL;
                        continue;
                    } else /* found a reference */
                        goto unlock_bucket_and_done;
                }
            }
unlock_bucket_and_done:
            __wt_spin_unlock(session, &bucket->evict_queue_lock);
            if (ref != NULL)
                goto done;
        }
    }
done:
    if (ref != NULL) {
        WT_ASSERT(session, ref->page != NULL);
        *btreep = ref->page->evict_data.dhandle->handle;
        *previous_statep = previous_state;
        *refp = ref;

        /*
         * Increment the busy count in the btree handle to prevent it from being closed under us.
         */
        (void)__wt_atomic_addv32(&((*btreep)->evict_data.evict_busy), 1);

        /*
         * If the reference is in the first bucket, but has a high read generation, we missed a case
         * when the page had to be upgraded to a higher bucket. This is not a critical error,
         * because we are okay to occasionally evict a page that's not the oldest, but we must flag
         * this in stats, because this must be debugged if happens frequently.
         */
        if (ref != NULL && j == 0 && bucketset != NULL &&
          __wt_atomic_loadv64(&bucketset->lowest_bucket_upper_range) <
            __wt_atomic_load64(&ref->page->evict_data.read_gen)) {
            WT_STAT_CONN_INCR(session, eviction_new_page_in_old_bucket);
            __wt_verbose_notice(session, WT_VERB_EVICTION,
              "eviction found a young page with read generation %" PRIu64 " in oldest bucket",
              ref->page->evict_data.read_gen);
        }
    } else
        WT_STAT_CONN_INCR(session, eviction_get_ref_empty);

    ret = (*refp == NULL ? WT_NOTFOUND : 0);

    /* Release the dhandle */
    WT_ASSERT(session, __wt_atomic_loadi32(&dhandle->session_inuse) > 0);
    (void)__wt_atomic_subi32(&dhandle->session_inuse, 1);

#if defined(HAVE_DIAGNOSTIC)
	__evict_page_consistency_check(session,  ref->page->evict_data.dhandle, ref->page, true);
#endif

err:
    if (dhandle_list_locked)
        __wt_readunlock(session, &conn->dhandle_lock);
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
    uint64_t time_start, time_stop;
    uint32_t flags;
    bool page_is_modified;

    WT_TRACK_OP_INIT(session);

    WT_RET_TRACK(__evict_get_ref(session, &btree, &ref, &previous_state));
    WT_ASSERT(session, WT_REF_GET_STATE(ref) == WT_REF_LOCKED);

    time_start = 0;

    flags = 0;
    page_is_modified = false;

    /*
     * Was the page evicted by an eviction worker on an application thread?
     */
    if (F_ISSET(session, WT_SESSION_INTERNAL))
        WT_STAT_CONN_INCR(session, eviction_worker_evict_attempt);
    else {
        if (__wt_page_is_modified(ref->page)) {
            page_is_modified = true;
            WT_STAT_CONN_INCR(session, eviction_app_dirty_attempt);
        }
        WT_STAT_CONN_INCR(session, eviction_app_attempt);
        S2C(session)->evict->app_evicts++;
        time_start = WT_STAT_ENABLED(session) ? __wt_clock(session) : 0;
    }

    WT_WITH_BTREE(session, btree, ret = __wt_evict(session, ref, previous_state, flags));

    (void)__wt_atomic_subv32(&btree->evict_data.evict_busy, 1);

    if (time_start != 0) {
        time_stop = __wt_clock(session);
        WT_STAT_CONN_INCRV(session, eviction_app_time, WT_CLOCKDIFF_US(time_stop, time_start));
    }

    if (WT_UNLIKELY(ret != 0)) {
        if (F_ISSET(session, WT_SESSION_INTERNAL))
            WT_STAT_CONN_INCR(session, eviction_worker_evict_fail);
        else {
            if (page_is_modified)
                WT_STAT_CONN_INCR(session, eviction_app_dirty_fail);
            WT_STAT_CONN_INCR(session, eviction_app_fail);
        }
    }

    WT_TRACK_OP_END(session);
    return (ret);
}

/*
 * __wti_evict_app_assist_worker --
 *     Worker function for __wt_evict_app_assist_worker_check: evict pages if the cache crosses
 *     eviction trigger thresholds.
 */
int
__wti_evict_app_assist_worker(WT_SESSION_IMPL *session, bool busy, bool readonly, double pct_full)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_EVICT *evict;
    WT_TRACK_OP_DECL;
    WT_TXN_GLOBAL *txn_global;
    WT_TXN_SHARED *txn_shared;
    uint64_t cache_max_wait_us, initial_progress, max_progress;
    uint64_t elapsed, time_start, time_stop;
    bool app_thread;

    WT_TRACK_OP_INIT(session);

    conn = S2C(session);
    evict = conn->evict;
    time_start = 0;
    txn_global = &conn->txn_global;
    txn_shared = WT_SESSION_TXN_SHARED(session);

    if (session->cache_max_wait_us != 0)
        cache_max_wait_us = session->cache_max_wait_us;
    else
        cache_max_wait_us = evict->cache_max_wait_us;

    /* FIXME-WT-12905: Pre-fetch threads are not allowed to be pulled into eviction. */
    if (F_ISSET(session, WT_SESSION_PREFETCH_THREAD))
        goto done;

    /*
     * Before we enter the eviction generation, make sure this session has a cached history store
     * cursor, otherwise we can deadlock with a session wanting exclusive access to a handle: that
     * session will have a handle list write lock and will be waiting on eviction to drain, we'll be
     * inside eviction waiting on a handle list read lock to open a history store cursor.
     */
    WT_ERR(__wt_curhs_cache(session));

    /*
     * It is not safe to proceed if the eviction server threads aren't setup yet.
     */
    if (!__wt_atomic_loadbool(&conn->evict_server_running) || (busy && pct_full < 100.0))
        goto done;

    /* Wake the eviction threads if we need to do work. */
    __wt_cond_signal(session, conn->evict_threads.wait_cond);

    /* Track how long application threads spend doing eviction. */
    app_thread = !F_ISSET(session, WT_SESSION_INTERNAL);
    if (app_thread)
        time_start = __wt_clock(session);

    /*
     * Note that this for loop is designed to reset expected eviction error codes before exiting,
     * namely, the busy return and empty eviction queue. We do not need the calling functions to
     * have to deal with internal eviction return codes.
     */
    for (initial_progress = __wt_atomic_loadv64(&evict->eviction_progress);; ret = 0) {
        /*
         * If eviction is stuck, check if this thread is likely causing problems and should be
         * rolled back. Ignore if in recovery, those transactions can't be rolled back.
         */
        if (!F_ISSET(conn, WT_CONN_RECOVERING) && __wt_evict_cache_stuck(session)) {
            ret = __wt_txn_is_blocking(session);
            if (ret == WT_ROLLBACK) {
                if (__wt_atomic_load32(&evict->evict_aggressive_score) > 0)
                    (void)__wt_atomic_subv32(&evict->evict_aggressive_score, 1);
                WT_STAT_CONN_INCR(session, txn_rollback_oldest_pinned);
                __wt_verbose_debug1(session, WT_VERB_TRANSACTION, "rollback reason: %s",
                  session->txn->rollback_reason);
            }
            WT_ERR(ret);
        }

        /*
         * Check if we've exceeded our operation timeout, this would also get called from the
         * previous txn is blocking call, however it won't pickup transactions that have been
         * committed or rolled back as their mod count is 0, and that txn needs to be the oldest.
         *
         * Additionally we don't return rollback which could confuse the caller.
         */
        if (__wt_op_timer_fired(session))
            break;

        /* Check if we have exceeded the global or the session timeout for waiting on the cache. */
        if (time_start != 0 && cache_max_wait_us != 0) {
            time_stop = __wt_clock(session);
            if (session->cache_wait_us + WT_CLOCKDIFF_US(time_stop, time_start) > cache_max_wait_us)
                break;
        }

        /*
         * Check if we have become busy.
         *
         * If we're busy (because of the transaction check we just did or because our caller is
         * waiting on a longer-than-usual event such as a page read), and the cache level drops
         * below 100%, limit the work to 5 evictions and return. If that's not the case, we can do
         * more.
         */
        if (!busy && __wt_atomic_loadv64(&txn_shared->pinned_id) != WT_TXN_NONE &&
          __wt_atomic_loadv64(&txn_global->current) != __wt_atomic_loadv64(&txn_global->oldest_id))
            busy = true;
        max_progress = busy ? 5 : 20;

        /* See if eviction is still needed. */
        if (!__wt_evict_needed(session, busy, readonly, &pct_full) ||
          (pct_full < 100.0 &&
            (__wt_atomic_loadv64(&evict->eviction_progress) > initial_progress + max_progress)))
            break;

        /* Evict a page. */
        switch (ret = __evict_page(session)) {
        case 0:
            if (busy)
                goto err;
        /* FALLTHROUGH */
        case EBUSY:
            break;
        case WT_NOTFOUND:
            evict->app_waits++;
            break;
        default:
            goto err;
        }
    }

err:
    if (time_start != 0) {
        time_stop = __wt_clock(session);
        elapsed = WT_CLOCKDIFF_US(time_stop, time_start);
        WT_STAT_CONN_INCR(session, application_cache_ops);
        WT_STAT_CONN_INCRV(session, application_cache_time, elapsed);
        WT_STAT_SESSION_INCRV(session, cache_time, elapsed);
        session->cache_wait_us += elapsed;
        /*
         * Check if a rollback is required only if there has not been an error. Returning an error
         * takes precedence over asking for a rollback. We can not do both.
         */
        if (ret == 0 && cache_max_wait_us != 0 && session->cache_wait_us > cache_max_wait_us) {
            ret = __wt_txn_rollback_required(session, WT_TXN_ROLLBACK_REASON_CACHE_OVERFLOW);
            if (__wt_atomic_load32(&evict->evict_aggressive_score) > 0)
                (void)__wt_atomic_subv32(&evict->evict_aggressive_score, 1);
            WT_STAT_CONN_INCR(session, eviction_timed_out_ops);
            __wt_verbose_notice(
              session, WT_VERB_TRANSACTION, "rollback reason: %s", session->txn->rollback_reason);
        }
    }

done:
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
    __wt_evict_touch_page(session, session->dhandle, ref, false, true /* won't need */);
    __wt_cond_signal(session, S2C(session)->evict_threads.wait_cond);
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
 *     XXX update dhandle selection logic to consider
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
    WT_BTREE *btree;
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
    btree = dhandle->handle;
    WT_RET(__wt_msg(session, "%s(%s%s)%s%s:", dhandle->name,
      WT_DHANDLE_IS_CHECKPOINT(dhandle) ? "checkpoint=" : "",
      WT_DHANDLE_IS_CHECKPOINT(dhandle) ? dhandle->checkpoint : "<live>",
	  btree->evict_data.evict_disabled != 0 ? " eviction disabled" : "",
      btree->evict_data.evict_disabled_open ? " at open" : ""));

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
        size = __wt_atomic_loadsize(&page->memory_footprint);

		if (__evict_page_consistency_check(session, dhandle, page, true) == false)
			WT_RET(__wt_msg(session, "page %p inconsistent eviction state", (void*)page));

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

    bytes_inmem = __wt_atomic_load64(&cache->bytes_inmem);
    bytes_dirty_intl = __wt_atomic_load64(&cache->bytes_dirty_intl);
    bytes_dirty_leaf = __wt_atomic_load64(&cache->bytes_dirty_leaf);

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
 * __wt_evict_init_handle_data --
 *     Initialize the per-tree eviction data.
 */
int
__wt_evict_init_handle_data(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle)
{
    WT_BTREE *btree;
    WT_EVICT_BUCKET *bucket;
    WT_EVICT_BUCKETSET *bucketset;
    WT_EVICT_HANDLE_DATA *evict_data;
    int i, j;

    if (!WT_DHANDLE_BTREE(dhandle))
        return (0);

    btree = dhandle->handle;
    evict_data = &btree->evict_data;

    /*
     * We have a few bucket sets organized by eviction priority. Lower numbered bucket set means
     * higher eviction priority. Typically clean leaf pages are the lowest bucket set. Within each
     * bucket set we iterate over buckets.
     */
    for (i = 0; i < WT_EVICT_LEVELS; i++) {
        bucketset = &evict_data->evict_bucketset[i];
		bucketset->lowest_bucket_upper_range =  WT_EVICT_BUCKET_RANGE;
        for (j = 0; j < WT_EVICT_NUM_BUCKETS; j++) {
            bucket = &bucketset->buckets[j];
            bucket->id = (uint64_t)j;
            WT_RET(__wt_spin_init(session, &bucket->evict_queue_lock, "evict bucket queue lock"));
            TAILQ_INIT(&bucket->evict_queue);
        }
    }
    evict_data->initialized = true;
    return (0);
}

/*
 * __evict_bucket_range --
 *     Get the lower and upper range of read generations hosted by this bucket.
 */
static inline void
__evict_bucket_range(WT_SESSION_IMPL *session, WT_EVICT_BUCKET *bucket,
					 uint64_t *min_rangep, uint64_t *max_rangep)
{
	WT_EVICT_BUCKETSET *bucketset;
	uint64_t min_range, max_range;

	WT_ASSERT(session, bucket != NULL);

	bucketset = WT_BUCKET_TO_BUCKETSET(bucket);
    /*
     * Example where the lowest bucket upper range is 400 and the evict bucket range is defined
     * to 300. Bucket 1 lower range is the upper range of the previous bucket plus one. Bucket 1
     * upper range is the upper range of the previous evict bucket plus 300.
     *
     * | bucket0           | bucket 1         | bucket 2          |
     * |                   |                  |                   |
     * | lower range: 0    | lower range: 401 | lower range: 701  |    etc.
     * | upper range: 400  | upper range: 700 | upper range: 1000 |
     *
     */
    if (bucket->id == 0) {
        min_range = 0;
        max_range = __wt_atomic_loadv64(&bucketset->lowest_bucket_upper_range);
    } else {
        min_range = __wt_atomic_loadv64(&bucketset->lowest_bucket_upper_range) +
          WT_EVICT_BUCKET_RANGE * (bucket->id - 1) + 1;
        max_range = __wt_atomic_loadv64(&bucketset->lowest_bucket_upper_range) +
          WT_EVICT_BUCKET_RANGE * bucket->id;
    }
	if (min_rangep != NULL)
		*min_rangep = min_range;
	if (max_rangep != NULL)
		*max_rangep = max_range;
}

/*
 * __wt_evict_remove --
 *     Remove the page from its evict bucket.
 */
void
__wt_evict_remove(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_PAGE *page;
    WT_REF_STATE previous_state;
    bool must_unlock_ref;

    must_unlock_ref = false;
	previous_state = 0;

    WT_ASSERT(session, ref->page != NULL);
    if (WT_REF_GET_STATE(ref) != WT_REF_LOCKED) {
        WT_REF_LOCK(session, ref, &previous_state);
        must_unlock_ref = true;
    }
    page = ref->page;

    if (WT_EVICT_PAGE_CLEARED(page))
        goto done;

    __wt_spin_lock(session, &page->evict_data.bucket->evict_queue_lock);
    TAILQ_REMOVE(&page->evict_data.bucket->evict_queue, page, evict_data.evict_q);
    page->evict_data.bucket->num_items--;
    WT_ASSERT(session, page->evict_data.bucket->num_items >= 0);
    __wt_spin_unlock(session, &page->evict_data.bucket->evict_queue_lock);

    page->evict_data.bucket = NULL;

done:
    if (must_unlock_ref)
        WT_REF_UNLOCK(ref, previous_state);
}

/*
 * __evict_renumber_buckets --
 *     Atomically increase the lowest bucket upper bound. Upper bounds of higher buckets are
 *     implicitly derived from that of the lowest bucket, so as a result the upper bounds of all
 *     buckets are shifted up. We need to renumber the buckets every time there is a read generation
 *     that’s larger than the range accepted by the highest bucket.
 */
static inline void
__evict_renumber_buckets(WT_EVICT_BUCKETSET *bucketset, uint64_t read_gen)
{
    uint64_t prev, new, target_max_readgen;

	/*
	 * We don't want to renumber buckets often, so let's accommodate a read generation 50% larger
	 * than the one that caused the renumbering.
	 */
	target_max_readgen = read_gen + read_gen / 2;
	new = target_max_readgen - (WT_EVICT_NUM_BUCKETS - 1) * WT_EVICT_BUCKET_RANGE;

    prev = __wt_atomic_load64(&bucketset->lowest_bucket_upper_range);
    /*
     * If the compare and swap fails, someone else is trying to update the value at the same time.
     * We let them win the race and return. The bucket's upper range can only grow, so we are okay
     * to lose this race.
     */
    __wt_atomic_casv64(&bucketset->lowest_bucket_upper_range, prev, new);
}

/*
 * __evict_page_correct_bucketset
 *     Is the page in the correct bucketset given its type?
 */
static bool
__evict_page_correct_bucketset(WT_PAGE *page, int home_bucketset_level)
{
	if (WT_PAGE_IS_INTERNAL(page)) {
		if (__wt_page_is_modified(page)) {
			if (home_bucketset_level != WT_EVICT_LEVEL_DIRTY_INTERNAL) {
				return (false);
			}
		}
		else if (home_bucketset_level != WT_EVICT_LEVEL_CLEAN_INTERNAL)
			return (false);
	}
	else { /* leaf page */
		if (__wt_page_is_modified(page)) {
			if (home_bucketset_level != WT_EVICT_LEVEL_DIRTY_LEAF)
				return (false);
		}
		else if (home_bucketset_level != WT_EVICT_LEVEL_CLEAN_LEAF)
			return (false);
	}
	return (true);
}

/*
 * __evict_page_get_bucketset --
 *     Find the home bucketset of the page if the page is already enqueued.
 */
static void
__evict_page_get_bucketset(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, WT_PAGE *page,
						   WT_EVICT_BUCKETSET **bucketset, int *bucketset_level)
{
	WT_EVICT_BUCKET *bucket;
	WT_EVICT_HANDLE_DATA *evict_handle_data;

	*bucketset = NULL;
	*bucketset_level = 0;

	if (!WT_DHANDLE_BTREE(dhandle)) {
#ifdef HAVE_DIAGNOSTIC
		WT_IGNORE_RET(__wt_msg(session,
		  "page (%s) %p: dhandle is not btree, should not be in eviction",
							   __wt_page_type_string(page->type), (void*)page));
#endif
		return;
	}
	evict_handle_data = &((WT_BTREE*)dhandle->handle)->evict_data;
	if (!evict_handle_data->initialized) {
#ifdef HAVE_DIAGNOSTIC
		WT_IGNORE_RET(__wt_msg(session,
							   "page (%s) %p: dhandle evict data is not initialized",
							   __wt_page_type_string(page->type), (void*)page));
#endif
		return;
	}
	if ((bucket = page->evict_data.bucket) == NULL)
		return;

	*bucketset =  WT_BUCKET_TO_BUCKETSET(page->evict_data.bucket);

	if (&evict_handle_data->evict_bucketset[WT_EVICT_LEVEL_CLEAN_LEAF] == *bucketset)
		*bucketset_level = WT_EVICT_LEVEL_CLEAN_LEAF;
	else if (&evict_handle_data->evict_bucketset[WT_EVICT_LEVEL_CLEAN_INTERNAL] == *bucketset)
		*bucketset_level = WT_EVICT_LEVEL_CLEAN_INTERNAL;
	else if (&evict_handle_data->evict_bucketset[WT_EVICT_LEVEL_DIRTY_LEAF] == *bucketset)
		*bucketset_level = WT_EVICT_LEVEL_DIRTY_LEAF;
	else if (&evict_handle_data->evict_bucketset[WT_EVICT_LEVEL_DIRTY_INTERNAL] == *bucketset)
		*bucketset_level = WT_EVICT_LEVEL_DIRTY_INTERNAL;
}

/*
 * __evict_page_consistency_check --
 *     Check that the page is in the right place in the eviction data structures.
 */
static bool
__evict_page_consistency_check(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, WT_PAGE *page,
							   bool verbose)
{
	WT_EVICT_BUCKETSET *bucketset;
	WT_EVICT_HANDLE_DATA *evict_handle_data;
	WT_REF_STATE state;
	uint64_t min_range, max_range;
	int home_bucketset_level = -1;

	evict_handle_data = &((WT_BTREE*)dhandle->handle)->evict_data;

	if (!WT_DHANDLE_BTREE(dhandle)) {
		if (verbose)
			WT_RET(__wt_msg(session, "page %s %p: dhandle is not btree, should not be in eviction",
							__wt_page_type_string(page->type), (void*)page));
		return (false);
	}
	if (page->ref == NULL) {
		if (verbose)
			WT_RET(__wt_msg(session, "page (%s) %p does not have an associated reference",
							__wt_page_type_string(page->type), (void*)page));
		return (false);
	}
	if (page->evict_data.dhandle != dhandle) {
		if (verbose)
			WT_RET(__wt_msg(session, "page %p dhandle mismatch. Expected %s, got %s",
							(void*) page, (dhandle == NULL) ? "null" : dhandle->name,
				   (page->evict_data.dhandle == NULL) ? "null" : page->evict_data.dhandle->name));
		return (false);
	}
	if ((state = WT_REF_GET_STATE(page->ref)) != WT_REF_LOCKED && state != WT_REF_MEM) {
		if (verbose)
		  WT_RET(__wt_msg(session, "page %s %p state is neither locked nor in-memory\n",
						  __wt_page_type_string(page->type), (void*)page));
		return (false);
	}
	WT_ASSERT(session,
		page->evict_data.bucket->id >= 0 && page->evict_data.bucket->id < WT_EVICT_NUM_BUCKETS);

	__evict_page_get_bucketset(session, dhandle, page, &bucketset, &home_bucketset_level);

	if (bucketset == NULL) {
		if (verbose)
			WT_RET(__wt_msg(session,
			"page (%s) %p is not in a bucketset or the bucketset does not belong to its dhandle",
							__wt_page_type_string(page->type),(void*)page));
		return (false);
	}

	if (!evict_handle_data->initialized) {
		if (verbose)
			WT_RET(__wt_msg(session,
			   "page (%s) %p is in a bucket, but dhandle evict data uninitialized",
							__wt_page_type_string(page->type),(void*)page));
		return (false);
	}

	/* Is this the right bucketset given the page type? */
	if (!__evict_page_correct_bucketset(page, home_bucketset_level)) {
		if (verbose)
			WT_RET(__wt_msg(session,
			"page (%s) %p is in bucketset %d, which isn't the correct bucketset for %s %s pages",
							__wt_page_type_string(page->type), (void*)page,  home_bucketset_level,
							WT_PAGE_IS_INTERNAL(page) ? "internal" : "leaf",
							 __wt_page_is_modified(page) ? "dirty" : "clean"));
		return (false);
	}

	__evict_bucket_range(session, page->evict_data.bucket, &min_range, &max_range);
	if (page->evict_data.read_gen > max_range) {
		if (verbose)
			WT_RET(__wt_msg(session,
			"page %p: read generation %" PRIu64 " is larger than the max range of its bucket %"
							PRIu64 "", (void*)page, page->evict_data.read_gen, max_range));
		return (false);
	}
	return (true);
}

/*
 * __wt_evict_enqueue_page --
 *     Put the page into the evict bucket corresponding to its read generation.
 */
void
__wt_evict_enqueue_page(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, WT_REF *ref)
{
    WT_EVICT_BUCKET *bucket;
    WT_EVICT_BUCKETSET *bucketset;
    WT_EVICT_HANDLE_DATA *evict_data;
    WT_PAGE *page;
    WT_REF_STATE previous_state;
    bool must_unlock_ref;
	int bucketset_level;
    uint64_t min_range, max_range, dst_bucket, read_gen, retries;

    must_unlock_ref = false;
    page = ref->page;
    page->evict_data.dhandle = dhandle;
	previous_state = 0;
    retries = 0;

    if (__wt_ref_is_root(ref))
        return;

	if (!WT_DHANDLE_BTREE(dhandle) || !F_ISSET(dhandle, WT_DHANDLE_OPEN))
		return;

	/* Evict handle has the bucket sets for this data handle */
    evict_data = &((WT_BTREE*)dhandle->handle)->evict_data;
    WT_ASSERT(session, evict_data->initialized);

    /*
     * Lock the page so it doesn't disappear. We aren't evicting the page, so we don't need to check
     * for hazard pointers.
     */
    if (WT_REF_GET_STATE(ref) != WT_REF_LOCKED) {
        WT_REF_LOCK(session, ref, &previous_state);
        must_unlock_ref = true;
    }

	__evict_page_get_bucketset(session, dhandle, page, &bucketset, &bucketset_level);

	/* If the page is already in a bucketset, is this the right one? */
	if (bucketset != NULL && __evict_page_correct_bucketset(page, bucketset_level)) {
		bucket = page->evict_data.bucket; /* won't be NULL if bucketset was set to a value */
        /* Is the page already in the right bucket? */
        __evict_bucket_range(session, bucket, &min_range, &max_range);
        if ((read_gen = __wt_atomic_load64(&page->evict_data.read_gen)) >= min_range
			&& read_gen <= max_range)
            goto done;
        else
            __wt_evict_remove(session, ref);
    } else if (bucketset != NULL &&
			   __evict_page_correct_bucketset(page, bucketset_level) == false) {
		/* Page is in the wrong bucketset */
		__wt_evict_remove(session, ref);
	}

    /* Find the bucket set for the page depending on its type */
    if (WT_PAGE_IS_INTERNAL(page)) {
        if (__wt_page_is_modified(page))
			bucketset_level = WT_EVICT_LEVEL_DIRTY_INTERNAL;
        else
            bucketset_level = WT_EVICT_LEVEL_CLEAN_INTERNAL;
    } else { /* we have a leaf page */
        if (__wt_page_is_modified(page))
            bucketset_level = WT_EVICT_LEVEL_DIRTY_LEAF;
        else
            bucketset_level = WT_EVICT_LEVEL_CLEAN_LEAF;
    }

	bucketset = &evict_data->evict_bucketset[bucketset_level];

#define MAX_RETRIES 10
    /*
     * Find the right bucket. The page's read generation may change as we are looking for the right
     * bucket. In that case, the page will end up in a lower bucket than. it should be. That's okay,
     * because we are maintaining approximately sorted order. We expect such events to be rare,
     * because read generations are updated infrequently.
     */
retry:
    if ((read_gen =
		 __wt_atomic_load64(&page->evict_data.read_gen)) <= bucketset->lowest_bucket_upper_range)
        dst_bucket = 0;
    else {
        dst_bucket = 1 + (read_gen - bucketset->lowest_bucket_upper_range) / WT_EVICT_BUCKET_RANGE;
        if (dst_bucket >= WT_EVICT_NUM_BUCKETS && retries++ < MAX_RETRIES) {
            __evict_renumber_buckets(bucketset, read_gen);
			WT_STAT_CONN_INCR(session, eviction_renumbered_buckets);
            goto retry;
        } else if (dst_bucket >= WT_EVICT_NUM_BUCKETS && retries >= MAX_RETRIES) {
            /*
             * We are here if the page's read generation keeps increasing as we are renumbering
             * buckets. This is extremely unlikely to happen, but we use this safety measure to not
             * get stuck in this loop. We simply place the page in the highest bucket and proceed.
             * This is okay, because all we care about is an approximate sorted order.
             */
            dst_bucket = WT_EVICT_NUM_BUCKETS - 1;
        }
    }
    bucket = &bucketset->buckets[dst_bucket];
	page->evict_data.bucket = bucket;

    __wt_spin_lock(session, &bucket->evict_queue_lock);
    TAILQ_INSERT_HEAD(&page->evict_data.bucket->evict_queue, page, evict_data.evict_q);
    bucket->num_items++;
    __wt_spin_unlock(session, &bucket->evict_queue_lock);

	WT_IGNORE_RET(__wt_msg(session,
			   "page (%s) %p: enqueued in bucket %" PRId64 " (%p) of bucketset %d (%p)",
						   __wt_page_type_string(page->type), (void *)page, dst_bucket,
						   (void *)bucket, bucketset_level, (void *)bucketset));

done:
    if (must_unlock_ref)
        WT_REF_UNLOCK(ref, previous_state);
#if defined(HAVE_DIAGNOSTIC)
	__evict_page_consistency_check(session,  page->evict_data.dhandle, page, true);
#endif
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
__wt_evict_touch_page(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, WT_REF *ref,
  bool internal_only, bool wont_need)
{
    WT_PAGE *page;

    page = ref->page;

    WT_ASSERT(session, ref->page != NULL);

    /* Is this the first use of the page? */
    if (__wt_atomic_load64(&page->evict_data.read_gen) == WT_READGEN_NOTSET) {
        if (wont_need)
            __wt_atomic_store64(&page->evict_data.read_gen, WT_READGEN_WONT_NEED);
        else
            __evict_read_gen_new(session, page);
    } else if (!internal_only)
        __wti_evict_read_gen_bump(session, page);

    if (!internal_only)
        __wt_evict_enqueue_page(session, dhandle, ref);
}

/*
 * __evict_init_ref --
 *     Add the ref to eviction data structures. Called by the function that links a page to a ref.
 */
static void
__evict_init_ref(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, WT_REF *ref)
{

    WT_ASSERT(session, ref->page != NULL);
    __wt_evict_enqueue_page(session, dhandle, ref);
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
    __wt_atomic_store64(&ref->page->evict_data.read_gen, WT_READGEN_EVICT_SOON);
    __wt_evict_enqueue_page(session, session->dhandle, ref);
}

/*
 * __evict_want_tree --
 *     Decide if we want to evict from this tree.
 */
static bool
__evict_want_tree(WT_SESSION_IMPL *session)
{
    WT_EVICT *evict;
    uint64_t btree_clean_inuse, btree_dirty_inuse, btree_updates_inuse;
    bool want_tree;

    evict = S2C(session)->evict;
    btree_clean_inuse = btree_dirty_inuse = btree_updates_inuse = 0;

    if (F_ISSET(evict, WT_EVICT_CACHE_CLEAN))
        btree_clean_inuse = __wt_btree_bytes_evictable(session);

    if (F_ISSET(evict, WT_EVICT_CACHE_DIRTY))
        btree_dirty_inuse = __wt_btree_dirty_leaf_inuse(session);

    if (F_ISSET(evict, WT_EVICT_CACHE_UPDATES))
        btree_updates_inuse = __wt_btree_bytes_updates(session);

    want_tree = (F_ISSET(evict, WT_EVICT_CACHE_CLEAN) && (btree_clean_inuse > 0)) ||
      (F_ISSET(evict, WT_EVICT_CACHE_DIRTY) && (btree_dirty_inuse > 0)) ||
      (F_ISSET(evict, WT_EVICT_CACHE_UPDATES) && (btree_updates_inuse > 0));

    return (want_tree);
}

/*
 * __evict_choose_dhandle --
 *     Select a dhandle for eviction
 */
static void
__evict_choose_dhandle(WT_SESSION_IMPL *session, WT_DATA_HANDLE **dhandle_p)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DATA_HANDLE *dhandle, *best_dhandle;
    WT_EVICT *evict;
	WT_EVICT_BUCKET *bucket;
	WT_EVICT_BUCKETSET *bucketset;
    bool want_tree;
    uint64_t bucket_readgen_lower_bound, max_bucketset_to_consider, min_readgen;
#ifdef EVICT_MAX_FOOTPRINT
	uint64_t  max_cache_footprint;
#endif

    best_dhandle = NULL;
    conn = S2C(session);
    evict = conn->evict;
#ifdef EVICT_MAX_FOOTPRINT
    max_cache_footprint = 0;
#endif
    min_readgen = ULONG_MAX;

    WT_ASSERT(session, __wt_rwlock_islocked(session, &conn->dhandle_lock));

    dhandle = TAILQ_FIRST(&conn->dhqh);
    for (uint64_t i = 0; i < conn->dhandle_count; i++) {
        btree = dhandle->handle;

        if (!WT_DHANDLE_BTREE(dhandle) || !F_ISSET(dhandle, WT_DHANDLE_OPEN))
            continue;
        /* Skip files that don't allow eviction. */
        if (btree->evict_data.evict_disabled > 0) {
            WT_STAT_CONN_INCR(session, eviction_skip_trees_eviction_disabled);
            continue;
        }
        /*
         * Skip files that are checkpointing if we are only looking for dirty pages.
         */
        if (WT_BTREE_SYNCING(btree) &&
          !F_ISSET(evict, WT_EVICT_CACHE_CLEAN | WT_EVICT_CACHE_UPDATES)) {
            WT_STAT_CONN_INCR(session, eviction_skip_checkpointing_trees);
            continue;
        }
        /*
         * Skip files that are configured to stick in cache until we become aggressive.
         *
         * If the file is contributing heavily to our cache usage then ignore the "stickiness" of
         * its pages.
         */
        if (btree->evict_data.evict_priority != 0 && !__wt_evict_aggressive(session) &&
          !__evict_btree_dominating_cache(session, btree)) {
            WT_STAT_CONN_INCR(session, eviction_skip_trees_stick_in_cache);
            continue;
        }

        WT_WITH_DHANDLE(session, dhandle, want_tree = __evict_want_tree(session));
        if (!want_tree) {
            WT_STAT_CONN_INCR(session, eviction_skip_unwanted_tree);
            continue;
        }

#ifdef EVICT_MAX_FOOTPRINT
        if (__wt_bytes_inmem(dhandle) > max_cache_footprint) {
            best_dhandle = dhandle;
            max_cache_footprint = __wt_bytes_inmem(dhandle);
        }
#else
        /* Dead trees are fast-tracked. */
        if (F_ISSET(dhandle, WT_DHANDLE_DEAD)) {
            best_dhandle = dhandle;
            break;
        }
        /*
         * If history store dirty content is dominating the cache, we want to prioritize evicting
         * history store pages over other btree pages. This helps in keeping cache contents below
         * the configured cache size during checkpoints where reconciling non-HS pages can generate
         * a significant amount of HS dirty content very quickly.
         */
		if (WT_IS_HS(dhandle) && __wti_evict_hs_dirty(session)) {
			WT_STAT_CONN_INCR(session, eviction_pages_queued_urgent_hs_dirty);
			best_dhandle = dhandle;
			break;
		}
		/*
		 * Find smallest readgen in the bucketset that eviction will consider.
		 * We decide which bucketsets to consider starting from highest to lowest desireability
		 * for eviction, depending on eviction flags.
		 */
		max_bucketset_to_consider = F_ISSET(evict, WT_EVICT_CACHE_UPDATES) ? WT_EVICT_LEVEL_DIRTY_INTERNAL :
			(F_ISSET(evict, WT_EVICT_CACHE_DIRTY) ? WT_EVICT_LEVEL_DIRTY_LEAF : WT_EVICT_LEVEL_CLEAN_LEAF);

		/*
		 * In each considered bucketset find the smallest bucket that has pages and remember its
		 * read generation upper bound.
		 */
		for (uint64_t j = 0; j <= max_bucketset_to_consider; j++) {
			bucketset = WT_DHANDLE_TO_BUCKETSET(dhandle, j);
			for (int k = 0; k < WT_EVICT_NUM_BUCKETS; k++) {
				bucket = &bucketset->buckets[k];
				if (__wt_atomic_load64(&bucket->num_items) == 0)
					continue;
				else {
					__evict_bucket_range(session, bucket, &bucket_readgen_lower_bound, NULL);
					if (bucket_readgen_lower_bound < min_readgen) {
						min_readgen = bucket_readgen_lower_bound;
						best_dhandle = dhandle;
					}
					break;
				}
			}
		}
#endif
        dhandle = TAILQ_NEXT(dhandle, q);
    }
    *dhandle_p = best_dhandle;
}

/*
 * __evict_read_gen_new --
 *     Get the read generation for a new page in memory.
 */
static void
__evict_read_gen_new(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    __wt_atomic_store64(
      &page->evict_data.read_gen,
	  (__evict_read_gen(session) + S2C(session)->evict->read_gen_oldest) / 2);
    __wt_evict_enqueue_page(session, session->dhandle, page->ref);
}

/* !!!
 * __wt_evict_page_first_dirty --
 *     Update a page's eviction state (read generation) when a page transitions from clean to
 *     dirty.
 *
 *     It is called every time a page transitions from clean to dirty for the first time in memory.
 *
 *     Input parameter:
 *       `page`: The page whose eviction state is being updated.
 */
void
__wt_evict_page_first_dirty(WT_SESSION_IMPL *session, WT_PAGE *page)
{
    /*
     * In the event we dirty a page which is flagged as wont need, we update its read generation to
     * avoid evicting a dirty page prematurely.
     */
    if (__wt_atomic_load64(&page->evict_data.read_gen) == WT_READGEN_WONT_NEED)
        __evict_read_gen_new(session, page);

	WT_IGNORE_RET(__wt_msg(session, "page (%s) %p first dirty",
						   __wt_page_type_string(page->type), (void*)page));

	/* Move the page to the right bucketset */
	if (page->ref != NULL && page->evict_data.dhandle != NULL)
		__wt_evict_enqueue_page(session, page->evict_data.dhandle, page->ref);
}

/*
 * __wt_ref_assign_page --
 *     Must be called every time we associate a new page with a ref. Adds the ref to eviction data
 *     structures.
 */
void
__wt_ref_assign_page(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, WT_REF *ref, WT_PAGE *page)
{
    ref->page = page;
	page->ref = ref;
    __evict_init_ref(session, dhandle, ref);
}

/*
 * __evict_skip_page --
 *     Decide if we should skip this page for eviction.
 */
static bool
__evict_skip_page(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_EVICT *evict;
    WT_PAGE *page;
    bool modified, want_page;

    btree = S2BT(session);
    conn = S2C(session);
    evict = conn->evict;
    page = ref->page;
    modified = __wt_page_is_modified(page);

    /* Don't queue dirty pages in trees during checkpoints. */
    if (modified && WT_BTREE_SYNCING(btree)) {
        WT_STAT_CONN_INCR(session, eviction_skip_dirty_pages_during_checkpoint);
        return (true);
    }

    /*
     * It's possible (but unlikely) to visit a page without a read generation, if we race with the
     * read instantiating the page. Set the page's read generation here to ensure a bug doesn't
     * somehow leave a page without a read generation.
     */
    if (__wt_atomic_load64(&page->evict_data.read_gen) == WT_READGEN_NOTSET)
        __evict_read_gen_new(session, page);

    want_page = (F_ISSET(evict, WT_EVICT_CACHE_CLEAN) && !modified) ||
      (F_ISSET(evict, WT_EVICT_CACHE_DIRTY) && modified) ||
      (F_ISSET(evict, WT_EVICT_CACHE_UPDATES) && page->modify != NULL);
    if (!want_page) {
        WT_STAT_CONN_INCR(session, eviction_skip_unwanted_pages);
        return (true);
    }

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

    /*
     * Don't attempt eviction of internal pages with children in cache.
     */
    if (F_ISSET(ref, WT_REF_FLAG_INTERNAL) &&
      __evict_internal_page_has_cached_children(session, ref)) {
        WT_STAT_CONN_INCR(session, eviction_skip_intl_page_with_active_child);
        return (true);
    }

    /* Evaluate dirty page candidacy, when eviction is not aggressive. */
    if (!__wt_evict_aggressive(session) && modified && __evict_skip_dirty_candidate(session, page))
        return (true);

    /* If the page can't be evicted, give up. */
    if (!__wt_page_can_evict(session, ref, NULL))
        return (true);

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
