#pragma once

/*
 * __wt_bulk_init --
 *     Bulk insert initialization.
 */
extern int __wt_bulk_init(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bulk_insert_fix --
 *     Fixed-length column-store bulk insert.
 */
extern int __wt_bulk_insert_fix(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk, bool deleted)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bulk_insert_fix_bitmap --
 *     Fixed-length column-store bulk insert.
 */
extern int __wt_bulk_insert_fix_bitmap(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bulk_insert_row --
 *     Row-store bulk insert.
 */
extern int __wt_bulk_insert_row(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bulk_insert_var --
 *     Variable-length column-store bulk insert.
 */
extern int __wt_bulk_insert_var(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk, bool deleted)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bulk_wrapup --
 *     Bulk insert cleanup.
 */
extern int __wt_bulk_wrapup(WT_SESSION_IMPL *session, WT_CURSOR_BULK *cbulk)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ovfl_discard_add --
 *     Add a new entry to the page's list of overflow records that have been discarded.
 */
extern int __wt_ovfl_discard_add(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL *cell)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ovfl_discard_free --
 *     Free the page's list of discarded overflow record addresses.
 */
extern void __wt_ovfl_discard_free(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ovfl_reuse_free --
 *     Free the page's list of overflow records tracked for reuse.
 */
extern void __wt_ovfl_reuse_free(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_cell_build_ovfl --
 *     Store overflow items in the file, returning the address cookie.
 */
extern int __wt_rec_cell_build_ovfl(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REC_KV *kv,
  uint8_t type, WT_TIME_WINDOW *tw, uint64_t rle) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_rec_dictionary_lookup --
 *     Check the dictionary for a matching value on this page.
 */
extern int __wt_rec_dictionary_lookup(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REC_KV *val,
  WT_REC_DICTIONARY **dpp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_reconcile --
 *     Reconcile an in-memory page into its on-disk format, and write it.
 */
extern int __wt_reconcile(WT_SESSION_IMPL *session, WT_REF *ref, WT_SALVAGE_COOKIE *salvage,
  uint32_t flags) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_split_page_size --
 *     Given a split percentage, calculate split page size in bytes.
 */
extern uint32_t __wt_split_page_size(int split_pct, uint32_t maxpagesize, uint32_t allocsize)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST
extern int __ut_ovfl_discard_verbose(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL *cell,
  const char *tag) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __ut_ovfl_discard_wrapup(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __ut_ovfl_track_init(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#endif
