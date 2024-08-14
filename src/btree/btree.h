#pragma once

extern bool __wt_read_cell_time_window(WT_CURSOR_BTREE *cbt, WT_TIME_WINDOW *tw)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const char *__wt_addr_string(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size,
  WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const char *__wt_key_string(WT_SESSION_IMPL *session, const void *data_arg, size_t size,
  const char *key_format, WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern const char *__wt_page_type_string(u_int type) WT_GCC_FUNC_DECL_ATTRIBUTE(
  (visibility("default"))) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_close(WT_CURSOR_BTREE *cbt, bool lowlevel)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_compare(WT_CURSOR_BTREE *a_arg, WT_CURSOR_BTREE *b_arg, int *cmpp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_equals(WT_CURSOR_BTREE *a_arg, WT_CURSOR_BTREE *b_arg, int *equalp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_insert(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_insert_check(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_modify(WT_CURSOR_BTREE *cbt, WT_MODIFY *entries, int nentries)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_next(WT_CURSOR_BTREE *cbt, bool truncating)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_next_random(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_prev(WT_CURSOR_BTREE *cbt, bool truncating)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_range_truncate(WT_TRUNCATE_INFO *trunc_info)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_remove(WT_CURSOR_BTREE *cbt, bool positioned)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_reserve(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_reset(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_search(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_search_near(WT_CURSOR_BTREE *cbt, int *exactp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_search_prepared(WT_CURSOR *cursor, WT_UPDATE **updp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btcur_update(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btree_close(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btree_config_encryptor(WT_SESSION_IMPL *session, const char **cfg,
  WT_KEYED_ENCRYPTOR **kencryptorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btree_discard(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btree_open(WT_SESSION_IMPL *session, const char *op_cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btree_stat_init(WT_SESSION_IMPL *session, WT_CURSOR_STAT *cst)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_btree_switch_object(WT_SESSION_IMPL *session, uint32_t objectid)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_checkpoint_cleanup_create(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_checkpoint_cleanup_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_col_modify(WT_CURSOR_BTREE *cbt, uint64_t recno, const WT_ITEM *value,
  WT_UPDATE **updp_arg, u_int modify_type, bool exclusive, bool restore)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_col_search(WT_CURSOR_BTREE *cbt, uint64_t search_recno, WT_REF *leaf,
  bool leaf_safe, bool *leaf_foundp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_compact(WT_SESSION_IMPL *session) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_key_order_init(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_truncate(WT_CURSOR_BTREE *start, WT_CURSOR_BTREE *stop,
  int (*rmfunc)(WT_CURSOR_BTREE *, const WT_ITEM *, u_int))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_debug_addr(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size,
  const char *ofile) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_debug_addr_print(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_debug_cursor_page(void *cursor_arg, const char *ofile) WT_GCC_FUNC_DECL_ATTRIBUTE(
  (visibility("default"))) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_debug_cursor_tree_hs(void *cursor_arg, const char *ofile)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_debug_offset(WT_SESSION_IMPL *session, wt_off_t offset, uint32_t size,
  uint32_t checksum, const char *ofile, bool dump_all_data, bool dump_key_data)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_debug_set_verbose(WT_SESSION_IMPL *session, const char *v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_debug_tree(void *session_arg, WT_BTREE *btree, WT_REF *ref, const char *ofile)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_debug_tree_all(void *session_arg, WT_BTREE *btree, WT_REF *ref, const char *ofile)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_debug_tree_shape(WT_SESSION_IMPL *session, WT_REF *ref, const char *ofile)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_delete_page_rollback(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_delete_redo_window_cleanup(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_import_repair(WT_SESSION_IMPL *session, const char *uri, char **configp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_key_return(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_multi_to_ref(WT_SESSION_IMPL *session, WT_PAGE *page, WT_MULTI *multi,
  WT_REF **refp, size_t *incrp, bool closing) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ovfl_discard(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL *cell)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ovfl_read(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL_UNPACK_COMMON *unpack,
  WT_ITEM *store, bool *decoded) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ovfl_remove(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL_UNPACK_KV *unpack)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_page_alloc(WT_SESSION_IMPL *session, uint8_t type, uint32_t alloc_entries,
  bool alloc_refs, WT_PAGE **pagep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_page_in_func(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags
#ifdef HAVE_DIAGNOSTIC
  ,
  const char *func, int line
#endif
  ) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_page_modify_alloc(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_prefetch_page_in(WT_SESSION_IMPL *session, WT_PREFETCH_QUEUE_ENTRY *pe)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_random_descent(WT_SESSION_IMPL *session, WT_REF **refp, uint32_t flags,
  WT_RAND_STATE *rnd) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_row_ikey_alloc(WT_SESSION_IMPL *session, uint32_t cell_offset, const void *key,
  size_t size, WT_IKEY **ikeyp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_row_leaf_key_copy(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip,
  WT_ITEM *key) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_row_leaf_key_work(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip_arg,
  WT_ITEM *keyb, bool instantiate) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_row_modify(WT_CURSOR_BTREE *cbt, const WT_ITEM *key, const WT_ITEM *value,
  WT_UPDATE **updp_arg, u_int modify_type, bool exclusive, bool restore)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_row_search(WT_CURSOR_BTREE *cbt, WT_ITEM *srch_key, bool insert, WT_REF *leaf,
  bool leaf_safe, bool *leaf_foundp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_salvage(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_search_insert(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_INSERT_HEAD *ins_head, WT_ITEM *srch_key) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_split_insert(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_split_multi(WT_SESSION_IMPL *session, WT_REF *ref, int closing)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_split_reverse(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_split_rewrite(WT_SESSION_IMPL *session, WT_REF *ref, WT_MULTI *multi)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_sync_file(WT_SESSION_IMPL *session, WT_CACHE_OP syncop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tree_walk(WT_SESSION_IMPL *session, WT_REF **refp, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tree_walk_count(WT_SESSION_IMPL *session, WT_REF **refp, uint64_t *walkcntp,
  uint32_t flags) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_tree_walk_custom_skip(WT_SESSION_IMPL *session, WT_REF **refp,
  int (*skip_func)(WT_SESSION_IMPL *, WT_REF *, void *, bool, bool *), void *func_cookie,
  uint32_t flags) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_value_return_buf(WT_CURSOR_BTREE *cbt, WT_REF *ref, WT_ITEM *buf,
  WT_TIME_WINDOW *tw) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verify(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verify_dsk(WT_SESSION_IMPL *session, const char *tag, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verify_dsk_image(WT_SESSION_IMPL *session, const char *tag,
  const WT_PAGE_HEADER *dsk, size_t size, WT_ADDR *addr, uint32_t verify_flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_btcur_free_cached_memory(WT_CURSOR_BTREE *cbt);
extern void __wt_btcur_init(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt);
extern void __wt_btcur_open(WT_CURSOR_BTREE *cbt);
extern void __wt_checkpoint_cleanup_trigger(WT_SESSION_IMPL *session);
extern void __wt_cursor_key_order_reset(WT_CURSOR_BTREE *cbt);
extern void __wt_free_update_list(WT_SESSION_IMPL *session, WT_UPDATE **updp);
extern void __wt_page_out(WT_SESSION_IMPL *session, WT_PAGE **pagep);
extern void __wt_ref_addr_free(WT_SESSION_IMPL *session, WT_REF *ref);
extern void __wt_ref_out(WT_SESSION_IMPL *session, WT_REF *ref);
extern void __wt_root_ref_init(
  WT_SESSION_IMPL *session, WT_REF *root_ref, WT_PAGE *root, bool is_recno);
extern void __wt_update_obsolete_check(
  WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_UPDATE *upd, bool update_accounting);
extern void __wt_value_return(WT_CURSOR_BTREE *cbt, WT_UPDATE_VALUE *upd_value);

#ifdef HAVE_UNITTEST

#endif
