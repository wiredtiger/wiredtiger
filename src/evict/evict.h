#pragma once

/*
 * __wt_cache_eviction_worker --
 *     Worker function for __wt_cache_eviction_check: evict pages if the cache crosses its
 *     boundaries.
 */
extern int __wt_cache_eviction_worker(WT_SESSION_IMPL *session, bool busy, bool readonly,
  double pct_full) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curstat_cache_walk --
 *     Initialize the statistics for a cache cache_walk pass.
 */
extern void __wt_curstat_cache_walk(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_evict --
 *     Evict a page.
 */
extern int __wt_evict(WT_SESSION_IMPL *session, WT_REF *ref, WT_REF_STATE previous_state,
  uint32_t flags) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_evict_create --
 *     Start the eviction server.
 */
extern int __wt_evict_create(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_evict_destroy --
 *     Destroy the eviction threads.
 */
extern int __wt_evict_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_evict_file --
 *     Discard pages for a specific file.
 */
extern int __wt_evict_file(WT_SESSION_IMPL *session, WT_CACHE_OP syncop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_evict_file_exclusive_off --
 *     Release exclusive eviction access to a file.
 */
extern void __wt_evict_file_exclusive_off(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_evict_file_exclusive_on --
 *     Get exclusive eviction access to a file and discard any of the file's blocks queued for
 *     eviction.
 */
extern int __wt_evict_file_exclusive_on(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_evict_priority_clear --
 *     Clear a tree's eviction priority.
 */
extern void __wt_evict_priority_clear(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_evict_priority_set --
 *     Set a tree's eviction priority.
 */
extern void __wt_evict_priority_set(WT_SESSION_IMPL *session, uint64_t v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_evict_server_wake --
 *     Wake the eviction server thread.
 */
extern void __wt_evict_server_wake(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_evict_urgent --
 *     Set a page to be evicted as soon as possible.
 */
extern bool __wt_page_evict_urgent(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_release_evict --
 *     Release a reference to a page, and attempt to immediately evict it.
 */
extern int __wt_page_release_evict(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verbose_dump_cache --
 *     Output diagnostic information about the cache.
 */
extern int __wt_verbose_dump_cache(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
