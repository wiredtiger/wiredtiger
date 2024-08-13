#pragma once

extern bool __wt_checksum_alt_match(const void *chunk, size_t len, uint32_t v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint32_t __wt_checksum_sw(const void *chunk, size_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint32_t __wt_checksum_with_seed_sw(uint32_t seed, const void *chunk, size_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE WT_BTREE *__wt_curhs_get_btree(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE WT_CELL *__wt_cell_leaf_value_parse(WT_PAGE *page, WT_CELL *cell)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE WT_CURSOR_BTREE *__wt_curhs_get_cbt(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE WT_FILE_SYSTEM *__wt_fs_file_system(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE WT_IKEY *__wt_ref_key_instantiated(WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE WT_VISIBLE_TYPE __wt_txn_upd_visible_type(WT_SESSION_IMPL *session, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_btree_dominating_cache(WT_SESSION_IMPL *session, WT_BTREE *btree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_btree_lsm_over_size(WT_SESSION_IMPL *session, uint64_t maxsize)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_btree_syncing_by_other_session(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_cache_aggressive(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_cache_full(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_cache_hs_dirty(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_cache_stuck(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_checksum_match(const void *chunk, size_t len, uint32_t v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_conf_get_compiled(WT_CONNECTION_IMPL *conn, const char *config,
  WT_CONF **confp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_conf_is_compiled(WT_CONNECTION_IMPL *conn, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_cursor_has_cached_memory(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_eviction_clean_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_eviction_clean_pressure(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_eviction_dirty_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_eviction_needed(WT_SESSION_IMPL *session, bool busy, bool readonly,
  double *pct_fullp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_eviction_updates_needed(WT_SESSION_IMPL *session, double *pct_fullp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_failpoint(WT_SESSION_IMPL *session, uint64_t conn_flag,
  u_int probability) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_isalnum(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_isalpha(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_isascii(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_isdigit(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_isprint(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_isspace(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_leaf_page_can_split(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_log_op(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_off_page(WT_PAGE *page, const void *p)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_op_timer_fired(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_page_can_evict(WT_SESSION_IMPL *session, WT_REF *ref, bool *inmem_splitp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_page_del_committed_set(WT_PAGE_DELETED *page_del)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_page_del_visible(WT_SESSION_IMPL *session, WT_PAGE_DELETED *page_del,
  bool hide_prepared) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_page_del_visible_all(WT_SESSION_IMPL *session, WT_PAGE_DELETED *page_del,
  bool hide_prepared) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_page_evict_clean(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_page_evict_retry(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_page_evict_soon_check(WT_SESSION_IMPL *session, WT_REF *ref,
  bool *inmem_split) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_page_is_empty(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_page_is_modified(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_page_is_reconciling(WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_readgen_evict_soon(uint64_t *readgen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_rec_need_split(WT_RECONCILE *r, size_t len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_ref_addr_copy(WT_SESSION_IMPL *session, WT_REF *ref, WT_ADDR_COPY *copy)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_ref_is_root(WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_row_leaf_value(WT_PAGE *page, WT_ROW *rip, WT_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_row_leaf_value_is_encoded(WT_ROW *rip)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_session_can_wait(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_spin_locked(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_spin_owned(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_split_descent_race(WT_SESSION_IMPL *session, WT_REF *ref,
  WT_PAGE_INDEX *saved_pindex) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_has_newest_and_visible_all(WT_SESSION_IMPL *session, uint64_t id,
  wt_timestamp_t timestamp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_snap_min_visible(
  WT_SESSION_IMPL *session, uint64_t id, wt_timestamp_t timestamp, wt_timestamp_t durable_timestamp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_timestamp_visible(WT_SESSION_IMPL *session, wt_timestamp_t timestamp,
  wt_timestamp_t durable_timestamp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_timestamp_visible_all(WT_SESSION_IMPL *session,
  wt_timestamp_t timestamp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_tw_start_visible(WT_SESSION_IMPL *session, WT_TIME_WINDOW *tw)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_tw_start_visible_all(WT_SESSION_IMPL *session, WT_TIME_WINDOW *tw)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_tw_stop_visible(WT_SESSION_IMPL *session, WT_TIME_WINDOW *tw)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_tw_stop_visible_all(WT_SESSION_IMPL *session, WT_TIME_WINDOW *tw)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_upd_value_visible_all(WT_SESSION_IMPL *session,
  WT_UPDATE_VALUE *upd_value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_upd_visible(WT_SESSION_IMPL *session, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_upd_visible_all(WT_SESSION_IMPL *session, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_visible(
  WT_SESSION_IMPL *session, uint64_t id, wt_timestamp_t timestamp, wt_timestamp_t durable_timestamp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_visible_all(WT_SESSION_IMPL *session, uint64_t id,
  wt_timestamp_t timestamp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE bool __wt_txn_visible_id_snapshot(
  uint64_t id, uint64_t snap_min, uint64_t snap_max, uint64_t *snapshot, uint32_t snapshot_count)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE const char *__wt_page_type_str(uint8_t val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE const char *__wt_prepare_state_str(uint8_t val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE const char *__wt_update_type_str(uint8_t val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE double __wt_eviction_dirty_target(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE double __wt_read_shared_double(double *to_read)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_btcur_bounds_early_exit(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  bool next, bool *key_out_of_boundsp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_btcur_skip_page(WT_SESSION_IMPL *session, WT_REF *ref, void *context,
  bool visible_all, bool *skipp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_btree_block_free(WT_SESSION_IMPL *session, const uint8_t *addr,
  size_t addr_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_buf_extend(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_buf_grow(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_buf_init(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_buf_initsize(WT_SESSION_IMPL *session, WT_ITEM *buf, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_buf_set(WT_SESSION_IMPL *session, WT_ITEM *buf, const void *data,
  size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_buf_setstr(WT_SESSION_IMPL *session, WT_ITEM *buf, const char *s)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_cache_eviction_check(WT_SESSION_IMPL *session, bool busy, bool readonly,
  bool *didworkp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_cell_pack_value_match(WT_CELL *page_cell, WT_CELL *val_cell,
  const uint8_t *val_data, bool *matchp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_cell_unpack_safe(WT_SESSION_IMPL *session, const WT_PAGE_HEADER *dsk,
  WT_CELL *cell, WT_CELL_UNPACK_ADDR *unpack_addr, WT_CELL_UNPACK_KV *unpack_value, const void *end)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_check_addr_validity(WT_SESSION_IMPL *session, WT_TIME_AGGREGATE *ta,
  bool expected_error) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_col_append_serial(WT_SESSION_IMPL *session, WT_PAGE *page,
  WT_INSERT_HEAD *ins_head, WT_INSERT ***ins_stack, WT_INSERT **new_insp, size_t new_ins_size,
  uint64_t *recnop, u_int skipdepth, bool exclusive)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_compare(WT_SESSION_IMPL *session, WT_COLLATOR *collator,
  const WT_ITEM *user_item, const WT_ITEM *tree_item, int *cmpp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_compare_bounds(WT_SESSION_IMPL *session, WT_CURSOR *cursor, WT_ITEM *key,
  uint64_t recno, bool upper, bool *key_out_of_bounds)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_compare_skip(WT_SESSION_IMPL *session, WT_COLLATOR *collator,
  const WT_ITEM *user_item, const WT_ITEM *tree_item, int *cmpp, size_t *matchp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_conf_check_choice(
  WT_SESSION_IMPL *session, const char **choices, const char *str, size_t len, const char **result)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_conf_check_one(WT_SESSION_IMPL *session, const WT_CONFIG_CHECK *check,
  WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_conf_gets_def_func(WT_SESSION_IMPL *session, const WT_CONF *conf,
  uint64_t keys, int def, WT_CONFIG_ITEM *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_curindex_get_valuev(WT_CURSOR *cursor, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_cursor_func_init(WT_CURSOR_BTREE *cbt, bool reenter)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_cursor_localkey(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_curtable_get_valuev(WT_CURSOR *cursor, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_dsk_cell_data_ref_addr(WT_SESSION_IMPL *session, int page_type,
  WT_CELL_UNPACK_ADDR *unpack, WT_ITEM *store) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_dsk_cell_data_ref_kv(WT_SESSION_IMPL *session, int page_type,
  WT_CELL_UNPACK_KV *unpack, WT_ITEM *store) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_extlist_read_pair(const uint8_t **p, wt_off_t *offp, wt_off_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_extlist_write_pair(uint8_t **p, wt_off_t off, wt_off_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fclose(WT_SESSION_IMPL *session, WT_FSTREAM **fstrp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fextend(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fflush(WT_SESSION_IMPL *session, WT_FSTREAM *fstr)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_file_lock(WT_SESSION_IMPL *session, WT_FH *fh, bool lock)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_filesize(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fprintf(WT_SESSION_IMPL *session, WT_FSTREAM *fstr, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fs_directory_list(
  WT_SESSION_IMPL *session, const char *dir, const char *prefix, char ***dirlistp, u_int *countp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fs_directory_list_free(WT_SESSION_IMPL *session, char ***dirlistp,
  u_int count) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fs_directory_list_single(
  WT_SESSION_IMPL *session, const char *dir, const char *prefix, char ***dirlistp, u_int *countp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fs_exist(WT_SESSION_IMPL *session, const char *name, bool *existp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fs_remove(WT_SESSION_IMPL *session, const char *name, bool durable,
  bool locked) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fs_rename(WT_SESSION_IMPL *session, const char *from, const char *to,
  bool durable) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fs_size(WT_SESSION_IMPL *session, const char *name, wt_off_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_fsync(WT_SESSION_IMPL *session, WT_FH *fh, bool block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_ftruncate(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_getline(WT_SESSION_IMPL *session, WT_FSTREAM *fstr, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_insert_serial(WT_SESSION_IMPL *session, WT_PAGE *page,
  WT_INSERT_HEAD *ins_head, WT_INSERT ***ins_stack, WT_INSERT **new_insp, size_t new_ins_size,
  u_int skipdepth, bool exclusive) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_lex_compare(const WT_ITEM *user_item, const WT_ITEM *tree_item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_lex_compare_short(const WT_ITEM *user_item, const WT_ITEM *tree_item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_lex_compare_skip(WT_SESSION_IMPL *session, const WT_ITEM *user_item,
  const WT_ITEM *tree_item, size_t *matchp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_log_cmp(WT_LSN *lsn1, WT_LSN *lsn2)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_page_cell_data_ref_kv(WT_SESSION_IMPL *session, WT_PAGE *page,
  WT_CELL_UNPACK_KV *unpack, WT_ITEM *store) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_page_dirty_and_evict_soon(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_page_modify_init(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_page_parent_modify_set(WT_SESSION_IMPL *session, WT_REF *ref,
  bool page_only) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_page_release(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_page_swap_func(
  WT_SESSION_IMPL *session, WT_REF *held, WT_REF *want, uint32_t flags
#ifdef HAVE_DIAGNOSTIC
  ,
  const char *func, int line
#endif
  ) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_read(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset, size_t len,
  void *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_rec_cell_build_val(WT_SESSION_IMPL *session, WT_RECONCILE *r,
  const void *data, size_t size, WT_TIME_WINDOW *tw, uint64_t rle)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_rec_dict_replace(
  WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_TIME_WINDOW *tw, uint64_t rle, WT_REC_KV *val)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_ref_block_free(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_row_leaf_key(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip,
  WT_ITEM *key, bool instantiate) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_row_leaf_key_instantiate(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_snprintf(char *buf, size_t size, const char *fmt, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 3, 4)))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_snprintf_len_incr(char *buf, size_t size, size_t *retsizep,
  const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 4, 5)))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_snprintf_len_set(char *buf, size_t size, size_t *retsizep,
  const char *fmt, ...) WT_GCC_FUNC_DECL_ATTRIBUTE((format(printf, 4, 5)))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_spin_init(WT_SESSION_IMPL *session, WT_SPINLOCK *t, const char *name)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_spin_trylock(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_spin_trylock_track(WT_SESSION_IMPL *session, WT_SPINLOCK *t)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_strcat(char *dest, size_t size, const char *src)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_strdup(WT_SESSION_IMPL *session, const char *str, void *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_struct_packv(WT_SESSION_IMPL *session, void *buffer, size_t size,
  const char *fmt, va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_struct_sizev(WT_SESSION_IMPL *session, size_t *sizep, const char *fmt,
  va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_struct_unpackv(WT_SESSION_IMPL *session, const void *buffer, size_t size,
  const char *fmt, va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_sync_and_rename(WT_SESSION_IMPL *session, WT_FSTREAM **fstrp,
  const char *from, const char *to) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_activity_check(WT_SESSION_IMPL *session, bool *txn_active)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_autocommit_check(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_begin(WT_SESSION_IMPL *session, WT_CONF *conf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_context_check(WT_SESSION_IMPL *session, bool requires_txn)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_context_prepare_check(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_id_check(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_idle_cache_check(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_modify(WT_SESSION_IMPL *session, WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_modify_check(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_UPDATE *upd, wt_timestamp_t *prev_tsp, u_int modify_type)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_modify_page_delete(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_op_set_key(WT_SESSION_IMPL *session, const WT_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_read(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt, WT_ITEM *key,
  uint64_t recno, WT_UPDATE *upd) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_read_upd_list(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_UPDATE *upd) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_read_upd_list_internal(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_UPDATE *upd, WT_UPDATE **prepare_updp, WT_UPDATE **restored_updp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_txn_search_check(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_upd_alloc(WT_SESSION_IMPL *session, const WT_ITEM *value,
  u_int modify_type, WT_UPDATE **updp, size_t *sizep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_upd_alloc_tombstone(WT_SESSION_IMPL *session, WT_UPDATE **updp,
  size_t *sizep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_update_serial(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_PAGE *page, WT_UPDATE **srch_upd, WT_UPDATE **updp, size_t upd_size, bool exclusive)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vfprintf(WT_SESSION_IMPL *session, WT_FSTREAM *fstr, const char *fmt,
  va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vpack_int(uint8_t **pp, size_t maxlen, int64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vpack_negint(uint8_t **pp, size_t maxlen, uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vpack_posint(uint8_t **pp, size_t maxlen, uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vpack_uint(uint8_t **pp, size_t maxlen, uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vsnprintf(char *buf, size_t size, const char *fmt, va_list ap)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vsnprintf_len_set(char *buf, size_t size, size_t *retsizep,
  const char *fmt, va_list ap) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vunpack_int(const uint8_t **pp, size_t maxlen, int64_t *xp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vunpack_negint(const uint8_t **pp, size_t maxlen, uint64_t *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vunpack_posint(const uint8_t **pp, size_t maxlen, uint64_t *retp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_vunpack_uint(const uint8_t **pp, size_t maxlen, uint64_t *xp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE int __wt_write(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset, size_t len,
  const void *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_cell_pack_addr(WT_SESSION_IMPL *session, WT_CELL *cell,
  u_int cell_type, uint64_t recno, WT_PAGE_DELETED *page_del, WT_TIME_AGGREGATE *ta, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_cell_pack_copy(WT_SESSION_IMPL *session, WT_CELL *cell,
  WT_TIME_WINDOW *tw, uint64_t rle, uint64_t v) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_cell_pack_del(WT_SESSION_IMPL *session, WT_CELL *cell,
  WT_TIME_WINDOW *tw, uint64_t rle) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_cell_pack_int_key(WT_CELL *cell, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_cell_pack_leaf_key(WT_CELL *cell, uint8_t prefix, size_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_cell_pack_ovfl(WT_SESSION_IMPL *session, WT_CELL *cell, uint8_t type,
  WT_TIME_WINDOW *tw, uint64_t rle, size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_cell_pack_value(WT_SESSION_IMPL *session, WT_CELL *cell,
  WT_TIME_WINDOW *tw, uint64_t rle, size_t size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_cell_total_len(void *unpack_arg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_strnlen(const char *s, size_t maxlen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_update_list_memsize(WT_UPDATE *upd)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_vsize_int(int64_t x) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_vsize_negint(uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_vsize_posint(uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE size_t __wt_vsize_uint(uint64_t x)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE u_char __wt_hex(int c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE u_char __wt_tolower(u_char c) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE u_int __wt_cell_type(WT_CELL *cell)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE u_int __wt_cell_type_raw(WT_CELL *cell)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE u_int __wt_skip_choose_depth(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint32_t __wt_lsn_offset(WT_LSN *lsn)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_btree_bytes_evictable(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_btree_bytes_inuse(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_btree_bytes_updates(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_btree_dirty_inuse(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_btree_dirty_leaf_inuse(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_cache_bytes_image(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_cache_bytes_inuse(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_cache_bytes_other(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_cache_bytes_plus_overhead(WT_CACHE *cache, uint64_t sz)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_cache_bytes_updates(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_cache_dirty_inuse(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_cache_dirty_leaf_inuse(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_cache_pages_inuse(WT_CACHE *cache)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_cache_read_gen(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_cell_rle(WT_CELL_UNPACK_KV *unpack)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_clock(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_clock_to_nsec(uint64_t end, uint64_t begin)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_rdtsc(void) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_safe_sub(uint64_t v1, uint64_t v2)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_txn_id_alloc(WT_SESSION_IMPL *session, bool publish)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE uint64_t __wt_txn_oldest_id(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
static WT_INLINE void __wt_btree_disable_bulk(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_buf_free(WT_SESSION_IMPL *session, WT_ITEM *buf);
static WT_INLINE void __wt_cache_decr_check_size(
  WT_SESSION_IMPL *session, size_t *vp, size_t v, const char *fld);
static WT_INLINE void __wt_cache_decr_check_uint64(
  WT_SESSION_IMPL *session, uint64_t *vp, uint64_t v, const char *fld);
static WT_INLINE void __wt_cache_dirty_decr(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_cache_dirty_incr(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_cache_page_byte_dirty_decr(
  WT_SESSION_IMPL *session, WT_PAGE *page, size_t size);
static WT_INLINE void __wt_cache_page_byte_updates_decr(
  WT_SESSION_IMPL *session, WT_PAGE *page, size_t size);
static WT_INLINE void __wt_cache_page_evict(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_cache_page_image_decr(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_cache_page_image_incr(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_cache_page_inmem_decr(
  WT_SESSION_IMPL *session, WT_PAGE *page, size_t size);
static WT_INLINE void __wt_cache_page_inmem_incr(
  WT_SESSION_IMPL *session, WT_PAGE *page, size_t size);
static WT_INLINE void __wt_cache_read_gen_bump(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_cache_read_gen_incr(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_cache_read_gen_new(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_cell_get_ta(WT_CELL_UNPACK_ADDR *unpack_addr, WT_TIME_AGGREGATE **tap);
static WT_INLINE void __wt_cell_get_tw(WT_CELL_UNPACK_KV *unpack_value, WT_TIME_WINDOW **twp);
static WT_INLINE void __wt_cell_type_reset(
  WT_SESSION_IMPL *session, WT_CELL *cell, u_int old_type, u_int new_type);
static WT_INLINE void __wt_cell_unpack_addr(WT_SESSION_IMPL *session, const WT_PAGE_HEADER *dsk,
  WT_CELL *cell, WT_CELL_UNPACK_ADDR *unpack_addr);
static WT_INLINE void __wt_cell_unpack_kv(WT_SESSION_IMPL *session, const WT_PAGE_HEADER *dsk,
  WT_CELL *cell, WT_CELL_UNPACK_KV *unpack_value);
static WT_INLINE void __wt_cond_wait(
  WT_SESSION_IMPL *session, WT_CONDVAR *cond, uint64_t usecs, bool (*run_func)(WT_SESSION_IMPL *));
static WT_INLINE void __wt_cursor_bound_reset(WT_CURSOR *cursor);
static WT_INLINE void __wt_cursor_dhandle_decr_use(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_cursor_dhandle_incr_use(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_cursor_free_cached_memory(WT_CURSOR *cursor);
static WT_INLINE void __wt_epoch(WT_SESSION_IMPL *session, struct timespec *tsp);
static WT_INLINE void __wt_op_timer_start(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_op_timer_stop(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_page_evict_soon(WT_SESSION_IMPL *session, WT_REF *ref);
static WT_INLINE void __wt_page_modify_clear(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_page_modify_set(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_page_only_modify_set(WT_SESSION_IMPL *session, WT_PAGE *page);
static WT_INLINE void __wt_rec_auximage_copy(
  WT_SESSION_IMPL *session, WT_RECONCILE *r, uint32_t count, WT_REC_KV *kv);
static WT_INLINE void __wt_rec_auxincr(
  WT_SESSION_IMPL *session, WT_RECONCILE *r, uint32_t v, size_t size);
static WT_INLINE void __wt_rec_cell_build_addr(WT_SESSION_IMPL *session, WT_RECONCILE *r,
  WT_ADDR *addr, WT_CELL_UNPACK_ADDR *vpack, uint64_t recno, WT_PAGE_DELETED *page_del);
static WT_INLINE void __wt_rec_image_copy(WT_SESSION_IMPL *session, WT_RECONCILE *r, WT_REC_KV *kv);
static WT_INLINE void __wt_rec_incr(
  WT_SESSION_IMPL *session, WT_RECONCILE *r, uint32_t v, size_t size);
static WT_INLINE void __wt_rec_time_window_clear_obsolete(WT_SESSION_IMPL *session,
  WT_UPDATE_SELECT *upd_select, WT_CELL_UNPACK_KV *vpack, WT_RECONCILE *r);
static WT_INLINE void __wt_ref_key(WT_PAGE *page, WT_REF *ref, void *keyp, size_t *sizep);
static WT_INLINE void __wt_ref_key_clear(WT_REF *ref);
static WT_INLINE void __wt_ref_key_onpage_set(
  WT_PAGE *page, WT_REF *ref, WT_CELL_UNPACK_ADDR *unpack);
static WT_INLINE void __wt_row_leaf_key_free(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip);
static WT_INLINE void __wt_row_leaf_key_info(WT_PAGE *page, void *copy, WT_IKEY **ikeyp,
  WT_CELL **cellp, void *datap, size_t *sizep, uint8_t *prefixp);
static WT_INLINE void __wt_row_leaf_key_set(WT_PAGE *page, WT_ROW *rip, WT_CELL_UNPACK_KV *unpack);
static WT_INLINE void __wt_row_leaf_value_cell(
  WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip, WT_CELL_UNPACK_KV *vpack);
static WT_INLINE void __wt_row_leaf_value_set(WT_ROW *rip, WT_CELL_UNPACK_KV *unpack);
static WT_INLINE void __wt_scr_free(WT_SESSION_IMPL *session, WT_ITEM **bufp);
static WT_INLINE void __wt_seconds(WT_SESSION_IMPL *session, uint64_t *secondsp);
static WT_INLINE void __wt_seconds32(WT_SESSION_IMPL *session, uint32_t *secondsp);
static WT_INLINE void __wt_set_shared_double(double *to_set, double value);
static WT_INLINE void __wt_spin_backoff(uint64_t *yield_count, uint64_t *sleep_usecs);
static WT_INLINE void __wt_spin_destroy(WT_SESSION_IMPL *session, WT_SPINLOCK *t);
static WT_INLINE void __wt_spin_lock(WT_SESSION_IMPL *session, WT_SPINLOCK *t);
static WT_INLINE void __wt_spin_lock_track(WT_SESSION_IMPL *session, WT_SPINLOCK *t);
static WT_INLINE void __wt_spin_unlock(WT_SESSION_IMPL *session, WT_SPINLOCK *t);
static WT_INLINE void __wt_spin_unlock_if_owned(WT_SESSION_IMPL *session, WT_SPINLOCK *t);
static WT_INLINE void __wt_struct_size_adjust(WT_SESSION_IMPL *session, size_t *sizep);
static WT_INLINE void __wt_timer_evaluate_ms(
  WT_SESSION_IMPL *session, WT_TIMER *start_time, uint64_t *time_diff_ms);
static WT_INLINE void __wt_timer_start(WT_SESSION_IMPL *session, WT_TIMER *start_time);
static WT_INLINE void __wt_timing_stress(
  WT_SESSION_IMPL *session, uint32_t flag, struct timespec *tsp);
static WT_INLINE void __wt_timing_stress_sleep_random(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_tree_modify_set(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_txn_cursor_op(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_txn_err_set(WT_SESSION_IMPL *session, int ret);
static WT_INLINE void __wt_txn_op_delete_apply_prepare_state(
  WT_SESSION_IMPL *session, WT_REF *ref, bool commit);
static WT_INLINE void __wt_txn_op_delete_commit_apply_timestamps(
  WT_SESSION_IMPL *session, WT_REF *ref);
static WT_INLINE void __wt_txn_op_set_recno(WT_SESSION_IMPL *session, uint64_t recno);
static WT_INLINE void __wt_txn_op_set_timestamp(WT_SESSION_IMPL *session, WT_TXN_OP *op);
static WT_INLINE void __wt_txn_pinned_timestamp(
  WT_SESSION_IMPL *session, wt_timestamp_t *pinned_tsp);
static WT_INLINE void __wt_txn_read_last(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_txn_unmodify(WT_SESSION_IMPL *session);
static WT_INLINE void __wt_upd_value_assign(WT_UPDATE_VALUE *upd_value, WT_UPDATE *upd);
static WT_INLINE void __wt_upd_value_clear(WT_UPDATE_VALUE *upd_value);
static WT_INLINE void __wt_usec_to_timespec(time_t usec, struct timespec *tsp);
static inline bool __wt_get_page_modify_ta(WT_SESSION_IMPL *session, WT_PAGE *page,
  WT_TIME_AGGREGATE **ta) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
