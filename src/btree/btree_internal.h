#pragma once

extern bool __wti_cell_type_check(uint8_t cell_type, uint8_t dsk_type)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern bool __wti_delete_page_skip(WT_SESSION_IMPL *session, WT_REF *ref, bool visible_all)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const char *__wti_cell_type_string(uint8_t type)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_btcur_bounds_position(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, bool next,
  bool *need_walkp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_btcur_evict_reposition(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_btree_new_leaf_page(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_btree_prefetch(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_btree_tree_open(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_col_fix_read_auxheader(WT_SESSION_IMPL *session, const WT_PAGE_HEADER *dsk,
  WT_COL_FIX_AUXILIARY_HEADER *auxhdr) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_key_order_check(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, bool next)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_cursor_valid(WT_CURSOR_BTREE *cbt, bool *valid, bool check_bounds)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_debug_disk(WT_SESSION_IMPL *session, const WT_PAGE_HEADER *dsk, const char *ofile,
  bool dump_all_data, bool dump_key_data) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_debug_offset_blind(WT_SESSION_IMPL *session, wt_off_t offset, const char *ofile,
  bool dump_all_data, bool dump_key_data) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_debug_page(void *session_arg, WT_BTREE *btree, WT_REF *ref, const char *ofile,
  bool dump_all_data, bool dump_key_data) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_delete_page(WT_SESSION_IMPL *session, WT_REF *ref, bool *skipp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_delete_page_instantiate(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_page_inmem(WT_SESSION_IMPL *session, WT_REF *ref, const void *image,
  uint32_t flags, WT_PAGE **pagep, bool *preparedp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_page_inmem_prepare(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_row_ikey(WT_SESSION_IMPL *session, uint32_t cell_offset, const void *key,
  size_t size, WT_REF *ref) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_row_ikey_incr(WT_SESSION_IMPL *session, WT_PAGE *page, uint32_t cell_offset,
  const void *key, size_t size, WT_REF *ref) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_tree_walk_skip(WT_SESSION_IMPL *session, WT_REF **refp, uint64_t *skipleafcntp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_btcur_iterate_setup(WT_CURSOR_BTREE *cbt);
extern void __wti_free_ref(WT_SESSION_IMPL *session, WT_REF *ref, int page_type, bool free_pages);
extern void __wti_free_ref_index(
  WT_SESSION_IMPL *session, WT_PAGE *page, WT_PAGE_INDEX *pindex, bool free_pages);
extern void __wti_read_row_time_window(
  WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip, WT_TIME_WINDOW *tw);
extern void __wti_ref_addr_safe_free(WT_SESSION_IMPL *session, void *p, size_t len);

#ifdef HAVE_UNITTEST

#endif
