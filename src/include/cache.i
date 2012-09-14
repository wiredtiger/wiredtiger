/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __wt_eviction_check --
 *	Wake the eviction server if necessary.
 */
static inline void
__wt_eviction_check(WT_SESSION_IMPL *session, int *read_lockoutp, int wake)
{
	WT_CACHE *cache;
	WT_CONNECTION_IMPL *conn;
	uint64_t bytes_inuse, bytes_max;

	conn = S2C(session);
	cache = conn->cache;

	/*
	 * If we're over the maximum cache, shut out reads (which
	 * include page allocations) until we evict to back under the
	 * maximum cache.  Eviction will keep pushing out pages so we
	 * don't run on the edge all the time.
	 */
	bytes_inuse = __wt_cache_bytes_inuse(cache);
	bytes_max = conn->cache_size;
	if (read_lockoutp != NULL)
		*read_lockoutp = (bytes_inuse > bytes_max);

	/* Wake eviction when we're over the trigger cache size. */
	if (wake && bytes_inuse > cache->eviction_trigger * (bytes_max / 100))
		__wt_evict_server_wake(session);
}

/*
 * __wt_eviction_page_check --
 *	Return if a page should be forcibly evicted.
 */
static inline int
__wt_eviction_page_check(WT_SESSION_IMPL *session, WT_PAGE *page)
{
	WT_CONNECTION_IMPL *conn;
	WT_PAGE_MODIFY *mod;

	conn = S2C(session);
	mod = page->modify;

	/*
	 * Root pages and clean pages are never forcibly evicted.
	 * Nor are pages from files that are purely cache resident.
	 */
	if (WT_PAGE_IS_ROOT(page) ||
	    !__wt_page_is_modified(page) ||
	    F_ISSET(session->btree, WT_BTREE_NO_EVICTION))
		return (0);

	/* Check the page's memory footprint. */
	if ((int64_t)page->memory_footprint > conn->cache_size / 2 ||
	    page->memory_footprint > 20 * session->btree->maxleafpage)
		return (1);

	/*
	 * If the page's write-generation has wrapped and caught up with the
	 * page's disk generation (wildly unlikely as it requires 4B updates
	 * between page reconciliations, but is technically possible), forcibly
	 * evict the page.
	 */
	if (mod != NULL && mod->write_gen + 1 == mod->disk_gen)
		return (1);

	return (0);
}
