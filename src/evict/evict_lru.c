/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"


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

    conn = S2C(session);
    evict = conn->evict;

	(void)thread;

    /* Mark the session as an eviction thread session. */
    F_SET(session, WT_SESSION_EVICTION);

	if (__wt_atomic_loadbool(&conn->evict_server_running) &&
      __wt_spin_trylock(session, &evict->evict_pass_lock) == 0) {
		/* We are eviction server */
		printf("Eviction server starting\n");
	} else {
		/* We are evict worker */
		printf("Eviction worker starting\n");
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
    WT_DECL_RET;
    WT_EVICT *evict;

    if (thread->id != 0)
        return (0);

    conn = S2C(session);
    evict = conn->evict;
	(void)evict;

    /*
     * The only cases when the eviction server is expected to stop are when recovery is finished,
     * when the connection is closing or when an error has occurred and connection panic flag is
     * set.
     */
    WT_ASSERT(session, F_ISSET(conn, WT_CONN_CLOSING | WT_CONN_PANIC | WT_CONN_RECOVERING));

    /* Clear the eviction thread session flag. */
    F_CLR(session, WT_SESSION_EVICTION);

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "eviction thread exiting");

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
    __wt_evict_server_wake(session);

    __wt_verbose_info(session, WT_VERB_EVICTION, "%s", "waiting for eviction threads to stop");

    /*
     * We call the destroy function still holding the write lock. It assumes it is called locked.
     */
    WT_RET(__wt_thread_group_destroy(session, &conn->evict_threads));

    return (0);
}


/* !!!
 * __wt_evict_file_exclusive_on --
 *     Acquire exclusive access to a file/tree making it possible to evict the entire file using
 *     `__wt_evict_file`. It does this by incrementing the `evict_disabled` counter for a
 *     tree, which disables all other means of eviction (except file eviction).
 *
 *     For the incremented `evict_disabled` value, the eviction server skips walking this tree for
 *     eviction candidates, and force-evicting or queuing pages from this tree is not allowed.
 *
 *     It is called from multiple places in the code base, such as when initiating file eviction
 *     `__wt_evict_file` or when opening or closing trees.
 *
 *     Return an error code if unable to acquire necessary locks or clear the eviction queues.
 */
int
__wt_evict_file_exclusive_on(WT_SESSION_IMPL *session) {

	(void)session;
	return 0;
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
	(void)session;

}

/* !!!
 * __wt_evict_page_urgent --
 *     Push a page into the urgent eviction queue.
 *
 *     It is called by the eviction server if pages require immediate eviction or by the application
 *     threads as part of forced eviction when directly evicting pages is not feasible.
 *
 *     Input parameters:
 *       `ref`: A reference to the page that is being added to the urgent eviction queue.
 *
 *     Return `true` if the page has been successfully added to the urgent queue, or `false` is
 *     already marked for eviction.
 */
bool
__wt_evict_page_urgent(WT_SESSION_IMPL *session, WT_REF *ref) {

	(void)session;
	(void)ref;

	return false;
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
    S2BT(session)->evict_priority = v;
}

/*
 * __wt_evict_priority_clear --
 *     Clear a tree's eviction priority to zero. It is called during the closure of the
 *     dhandle/btree.
 */
void
__wt_evict_priority_clear(WT_SESSION_IMPL *session)
{
    S2BT(session)->evict_priority = 0;
}

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

    __wt_cond_signal(session, conn->evict->evict_cond);
}

/*
 * __wti_evict_app_assist_worker --
 *     Worker function for __wt_evict_app_assist_worker_check: evict pages if the cache crosses
 *     eviction trigger thresholds.
 */
int
__wti_evict_app_assist_worker(WT_SESSION_IMPL *session, bool busy, bool readonly, double pct_full)
{
	(void)session;
	(void)busy;
	(void)readonly;
	(void)pct_full;

	return 0;
}

/*
 * __wti_evict_list_clear_page --
 *     Check whether a page is present in the LRU eviction list. If the page is found in the list,
 *     remove it. This is called from the page eviction code to make sure there is no attempt to
 *     evict a child page multiple times.
 */
void
__wti_evict_list_clear_page(WT_SESSION_IMPL *session, WT_REF *ref)
{
	(void)session;
	(void)ref;
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
      btree->evict_disabled != 0 ? " eviction disabled" : "",
      btree->evict_disabled_open ? " at open" : ""));

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
    needed = __wt_evict_clean_needed(session, &pct);
    WT_RET(__wt_msg(session, "cache clean check: %s (%2.3f%%)", needed ? "yes" : "no", pct));
    needed = __wt_evict_dirty_needed(session, &pct);
    WT_RET(__wt_msg(session, "cache dirty check: %s (%2.3f%%)", needed ? "yes" : "no", pct));
    needed = __wti_evict_updates_needed(session, &pct);
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
__wt_evict_init_handle_data(WT_SESSION_IMPL *session)
{
	WT_BTREE *btree;
    WT_DATA_HANDLE *dhandle;
	WT_EVICT_HANDLE *evict_handle;
	WT_EVICT_BUCKET *bucket;
	WT_EVICT_BUCKET_SET *bucket_set;
	int i, j;

	dhandle = session->dhandle;

	if (!WT_DHANDLE_BTREE(dhandle))
		return (0);

	btree = dhandle->handle;
	evict_handle = &btree->evict_handle;

	for (i = 0; i < WT_EVICT_LEVELS; i++) { /* Bucket Set. Then iterate over buckets... */
		bucketset = &evict_handle->evict_bucketset[i];
		for (j = 0; j < WT_EVICT_NUM_BUCKETS; j++) {
			bucket = &bucketset->buckets[j];
			bucket->id = j;
			WT_RET(__wt_spin_init(session, &bucket->evict_queue_lock, "evict bucket queue block"))
			TAILQ_INIT(&bucket->evict_queue);
		}
	}
	return (0);
}

/*
 *  __wt_evict_bucket_range --
 *      Get the lower and upper range of read generations hosted by this bucket.
 *
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
static inline void
__wt_evict_bucket_range(WT_EVICT_BUCKET *bucket, uint64_t *min_range, uint64_t *max_range) {

	WT_EVICT_BUCKETSET *bucketset;

	/*
	 * The address of the first bucket in the set is the same as the address
	 * of the bucketset.
	 */
	bucketset = (WT_BUCKET_SET *)(bucket - bucket->id);

	if (bucket->id == 0) {
		*min_range = 0;
		*max_range = __wt_atomic_loadv64(&bucketset->lowest_bucket_upper_range);
	}
	else {
		*min_range = __wt_atomic_loadv64(&bucketset->lowest_bucket_upper_range) +
			WT_EVICT_BUCKET_RANGE * (bucket->id - 1) + 1;
		*max_range = __wt_atomic_loadv64(&bucketset->lowest_bucket_upper_range) +
			WT_EVICT_BUCKET_RANGE * bucket->id;
	}
}

/*
 * __wt_evict_bucket_remove --
 *     Remove the page from the bucket's queue.
 */
static inline bool
__wt_evict_bucket_remove(WT_SESSION_IMPL *session, WT_REF *ref) {

	WT_EVICT_BUCKET *bucket;
	WT_PAGE *page;

	WT_ASSERT(session, WT_REF_GET_STATE(ref) == WT_REF_LOCKED && ref->page != NULL);
	page = ref->page;

	if ((bucket = &page->bucket) == NULL)
		return;

	wt_spin_lock(session, &page->bucket->evict_queue_lock);
	TAILQ_REMOVE(page->bucket->evict_queue, page, evict_q);
	wt_spin_unlock(session, &page->bucket->evict_queue_lock);

	page->bucket = NULL;
}

static int
__evict_enqueue_page(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, WT_REF *ref) {

	WT_BTREE *btree;
	WT_EVICT_HANDLE *evict_handle;
	WT_EVICT_BUCKET *bucket;
	WT_EVICT_BUCKETSET *bucketset;
	WT_EVICT_HANDLE_DATA *evict_handle;
	WT_PAGE *page;
	WT_REF_STATE previous_state;
	uint64_t min_range, max_range, dst_bucket, read_gen;

	page = ref->page;
	/*
	 * Lock the page so it doesn't disappear.
	 * We aren't evicting the page, so we don't need to check for hazard pointers.
	 */
    WT_REF_LOCK(session, ref, &previous_state);

	/* Is the page already in a bucket? */
	if ((bucket = page->bucket) != NULL) {
		__wt_evict_bucket_range(bucket, &min_range, &max_range);
		/* Is the page already in the right bucket? */
		if ((read_gen = __wt_atomic_load64(&page->read_gen)) >= min_range && read_gen <= max_range)
			return;
		else
			__wt_evict_bucket_remove(page);
	}

	/* Evict handle has the bucket sets for this data handle */
	evict_handle = &dhandle->evict_handle_data;

	/* Find the bucket set for the page depending on its type */
	if (WT_PAGE_IS_INTERNAL(page))
		bucketset = &evict_handle->evict_bucketset[WT_EVICT_LEVEL_INTERNAL];
	else if (__wt_page_is_modified(page))
		bucketset = &evict_handle->evict_bucketset[WT_EVICT_LEVEL_DIRTY_LEAF];
	else
		bucketset = &evict_handle->evict_bucketset[WT_EVICT_LEVEL_CLEAN_LEAF];

	/*
	 * Find the right bucket. The page's read generation may change as we are looking
	 * for the right bucket. In that case, the page will end up in a lower bucket than.
	 * it should be. That's okay, because we are maintaining approximately sorted order.
	 * We expect such events to be rare, because read generations are updated infrequently.
	 */
  retry:
	if ((read_gen = __wt_atomic_load64(&page->read_gen)) <= bucketset->lowest_bucket_upper_range)
		dst_bucket = 0;
	else {
		dst_bucket =
			1 + (read_gen - bucketset->lowest_bucket_upper_range) / WT_EVICT_BUCKET_RANGE;
		if (dst_bucket >= WT_EVICT_NUM_BUCKETS) {
			__wt_evict_renumber_buckets(bucketset);
			S2C(session)->evict->evict_renumbered_buckets++;
			goto retry;
		}
	}

	bucket = &bucketset->buckets[dst_bucket];

	wt_spin_lock(session, &bucket->evict_queue_lock);
	TAILQ_INSERT_HEAD(page->bucket->evict_queue, page, evict_q);
	wt_spin_unlock(session, &page->bucket->evict_queue_lock);

	page->bucket = bucket;

	WT_REF_UNLOCK(ref, previous_state);
}

/* !!!
 * __wt_evict_touch_page --
 *     Update a page's eviction state (read generation) when it is accessed.
 *
 *     A page that is recently read will have a higher read generation, indicating that it is less
 *     likely to be evicted. This mechanism helps eviction to prioritize the order in which pages
 *     are evicted.
 *
 *     This function is called every time a page is touched in the cache.
 *
 *     Input parameters:
 *       (1) `page`: The page whose eviction state is being updated.
 *       (2) `internal_only`: A flag indicating whether the operation is internal. If true, the read
 *            generation is not updated, as internal operations (such as compaction or eviction)
 *            should not affect the page's eviction priority.
 *       (3) `wont_need`: A flag indicating that the page will not be needed in the future. If true,
 *            the page is marked for forced eviction.
 */
void
__wt_evict_touch_page(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, WT_REF *ref,
					  bool internal_only, bool wont_need)
{
	WT_PAGE *page;

	page = ref->page;

    /* Is this the first use of the page? */
    if (__wt_atomic_load64(&page->read_gen) == WT_READGEN_NOTSET) {
        if (wont_need)
            __wt_atomic_store64(&page->read_gen, WT_READGEN_WONT_NEED);
        else
            __wti_evict_read_gen_new(session, page);
    } else if (!internal_only)
        __wti_evict_read_gen_bump(session, page);

	__evict_enqueue_page(session, dhandle, ref);
}
