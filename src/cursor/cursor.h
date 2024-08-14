#pragma once

extern const char *__wt_json_tokname(int toktype) WT_GCC_FUNC_DECL_ATTRIBUTE(
  (visibility("default"))) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_apply_single_idx(WT_SESSION_IMPL *session, WT_INDEX *idx, WT_CURSOR *cur,
  WT_CURSOR_TABLE *ctable, int (*f)(WT_CURSOR *)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_backup_file_remove(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_backup_load_incr(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *blkcfg,
  WT_ITEM *bitstring, uint64_t nbits) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_backup_open(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_backup_set_blkincr(WT_SESSION_IMPL *session, uint64_t i, uint64_t granularity,
  const char *id, uint64_t id_len) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curbackup_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *other,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curconfig_open(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curds_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_DATA_SOURCE *dsrc, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curfile_insert_check(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curfile_next_random(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curfile_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curhs_cache(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curhs_open(WT_SESSION_IMPL *session, WT_CURSOR *owner, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curhs_range_truncate(WT_TRUNCATE_INFO *trunc_info)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curhs_search_near_after(WT_SESSION_IMPL *session, WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curhs_search_near_before(WT_SESSION_IMPL *session, WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curindex_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curjoin_join(WT_SESSION_IMPL *session, WT_CURSOR_JOIN *cjoin, WT_INDEX *idx,
  WT_CURSOR *ref_cursor, uint8_t flags, uint8_t range, uint64_t count, uint32_t bloom_bit_count,
  uint32_t bloom_hash_count) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curjoin_joined(WT_CURSOR *cursor) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curjoin_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curlog_open(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curmetadata_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_bounds_restore(WT_SESSION_IMPL *session, WT_CURSOR *cursor,
  WT_CURSOR_BOUNDS_STATE *bounds_state) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_bounds_save(WT_SESSION_IMPL *session, WT_CURSOR *cursor,
  WT_CURSOR_BOUNDS_STATE *bounds_state) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_cache_get(WT_SESSION_IMPL *session, const char *uri, uint64_t hash_value,
  WT_CURSOR *to_dup, const char **cfg, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_cached(WT_CURSOR *cursor) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_config_notsup(WT_CURSOR *cursor, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_copy_release_item(WT_CURSOR *cursor, WT_ITEM *item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_dup_position(WT_CURSOR *to_dup, WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_equals(WT_CURSOR *cursor, WT_CURSOR *other, int *equalp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_get_key(WT_CURSOR *cursor, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_get_raw_key(WT_CURSOR *cursor, WT_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_get_raw_key_value(WT_CURSOR *cursor, WT_ITEM *key, WT_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_get_raw_value(WT_CURSOR *cursor, WT_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_get_value(WT_CURSOR *cursor, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_init(WT_CURSOR *cursor, const char *uri, WT_CURSOR *owner, const char *cfg[],
  WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_kv_not_set(WT_CURSOR *cursor, bool key) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_modify_value_format_notsup(WT_CURSOR *cursor, WT_MODIFY *entries,
  int nentries) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_notsup(WT_CURSOR *cursor) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_reconfigure(WT_CURSOR *cursor, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_cursor_reopen_notsup(WT_CURSOR *cursor, bool check_only)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curstat_init(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *curjoin,
  const char *cfg[], WT_CURSOR_STAT *cst) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curstat_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *other,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curtable_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_curversion_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_json_strncpy(WT_SESSION *wt_session, char **pdst, size_t dstlen, const char *src,
  size_t srclen) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_json_token(WT_SESSION *wt_session, const char *src, int *toktype,
  const char **tokstart, size_t *toklen) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_table_range_truncate(WT_TRUNCATE_INFO *trunc_info)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_verbose_dump_backup(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern size_t __wt_json_unpack_str(u_char *dest, size_t dest_len, const u_char *src, size_t src_len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern ssize_t __wt_json_strlen(const char *src, size_t srclen) WT_GCC_FUNC_DECL_ATTRIBUTE(
  (visibility("default"))) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern uint64_t __wt_cursor_checkpoint_id(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_backup_destroy(WT_SESSION_IMPL *session);
extern void __wt_cursor_close(WT_CURSOR *cursor);
extern void __wt_cursor_get_hash(
  WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *to_dup, uint64_t *hash_value);
extern void __wt_cursor_set_key(WT_CURSOR *cursor, ...);
extern void __wt_cursor_set_notsup(WT_CURSOR *cursor);
extern void __wt_cursor_set_raw_key(WT_CURSOR *cursor, WT_ITEM *key);
extern void __wt_cursor_set_raw_value(WT_CURSOR *cursor, WT_ITEM *value);
extern void __wt_cursor_set_value(WT_CURSOR *cursor, ...);
extern void __wt_curstat_dsrc_final(WT_CURSOR_STAT *cst);

#ifdef HAVE_UNITTEST

#endif
