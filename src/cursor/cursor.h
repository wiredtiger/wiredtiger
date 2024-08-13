#pragma once

/*
 * __wt_apply_single_idx --
 *     Apply an operation to a single index of a table.
 */
extern int __wt_apply_single_idx(WT_SESSION_IMPL *session, WT_INDEX *idx, WT_CURSOR *cur,
  WT_CURSOR_TABLE *ctable, int (*f)(WT_CURSOR *)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_backup_destroy --
 *     Destroy any backup information.
 */
extern void __wt_backup_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_backup_file_remove --
 *     Remove the incremental and meta-data backup files.
 */
extern int __wt_backup_file_remove(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_backup_load_incr --
 *     Load the incremental.
 */
extern int __wt_backup_load_incr(WT_SESSION_IMPL *session, WT_CONFIG_ITEM *blkcfg,
  WT_ITEM *bitstring, uint64_t nbits) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_backup_open --
 *     Restore any incremental backup information. We use the metadata's block information as the
 *     authority on whether incremental backup was in use on last shutdown.
 */
extern int __wt_backup_open(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_backup_set_blkincr --
 *     Given an index set the incremental block element to the given granularity and id string.
 */
extern int __wt_backup_set_blkincr(WT_SESSION_IMPL *session, uint64_t i, uint64_t granularity,
  const char *id, uint64_t id_len) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curbackup_open --
 *     WT_SESSION->open_cursor method for the backup cursor type.
 */
extern int __wt_curbackup_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *other,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curconfig_open --
 *     WT_SESSION->open_cursor method for config cursors.
 */
extern int __wt_curconfig_open(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curds_open --
 *     Initialize a data-source cursor.
 */
extern int __wt_curds_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_DATA_SOURCE *dsrc, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curfile_insert_check --
 *     WT_CURSOR->insert_check method for the btree cursor type.
 */
extern int __wt_curfile_insert_check(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curfile_next_random --
 *     WT_CURSOR->next method for the btree cursor type when configured with next_random. This is
 *     exported because it is called directly within LSM.
 */
extern int __wt_curfile_next_random(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curfile_open --
 *     WT_SESSION->open_cursor method for the btree cursor type.
 */
extern int __wt_curfile_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curhs_cache --
 *     Cache a new history store table cursor. Open and then close a history store cursor without
 *     saving it in the session.
 */
extern int __wt_curhs_cache(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curhs_open --
 *     Initialize a history store cursor.
 */
extern int __wt_curhs_open(WT_SESSION_IMPL *session, WT_CURSOR *owner, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curhs_range_truncate --
 *     Discard a cursor range from the history store tree.
 */
extern int __wt_curhs_range_truncate(WT_TRUNCATE_INFO *trunc_info)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curhs_search_near_after --
 *     Set the cursor position at the requested position or after it.
 */
extern int __wt_curhs_search_near_after(WT_SESSION_IMPL *session, WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curhs_search_near_before --
 *     Set the cursor position at the requested position or before it.
 */
extern int __wt_curhs_search_near_before(WT_SESSION_IMPL *session, WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curindex_open --
 *     WT_SESSION->open_cursor method for index cursors.
 */
extern int __wt_curindex_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curjoin_join --
 *     Add a new join to a join cursor.
 */
extern int __wt_curjoin_join(WT_SESSION_IMPL *session, WT_CURSOR_JOIN *cjoin, WT_INDEX *idx,
  WT_CURSOR *ref_cursor, uint8_t flags, uint8_t range, uint64_t count, uint32_t bloom_bit_count,
  uint32_t bloom_hash_count) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curjoin_joined --
 *     Produce an error that this cursor is being used in a join call.
 */
extern int __wt_curjoin_joined(WT_CURSOR *cursor) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curjoin_open --
 *     Initialize a join cursor. Join cursors are read-only.
 */
extern int __wt_curjoin_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curlog_open --
 *     Initialize a log cursor.
 */
extern int __wt_curlog_open(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curmetadata_open --
 *     WT_SESSION->open_cursor method for metadata cursors. Metadata cursors are a similar to a file
 *     cursor on the special metadata table, except that the metadata for the metadata table (which
 *     is stored in the turtle file) can also be queried. Metadata cursors are read-only by default.
 */
extern int __wt_curmetadata_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_bounds_restore --
 *     Restore the cursor's bounds state. We want to change only related flags as we can't guarantee
 *     the initial flag state of the column group cursors are the same.
 */
extern int __wt_cursor_bounds_restore(WT_SESSION_IMPL *session, WT_CURSOR *cursor,
  WT_CURSOR_BOUNDS_STATE *bounds_state) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_bounds_save --
 *     Save cursor bounds to restore the state.
 */
extern int __wt_cursor_bounds_save(WT_SESSION_IMPL *session, WT_CURSOR *cursor,
  WT_CURSOR_BOUNDS_STATE *bounds_state) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_cache_get --
 *     Open a matching cursor from the cache.
 */
extern int __wt_cursor_cache_get(WT_SESSION_IMPL *session, const char *uri, uint64_t hash_value,
  WT_CURSOR *to_dup, const char **cfg, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_cached --
 *     No actions on a closed and cached cursor are allowed.
 */
extern int __wt_cursor_cached(WT_CURSOR *cursor) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_checkpoint_id --
 *     Return the checkpoint ID for checkpoint cursors, otherwise 0.
 */
extern uint64_t __wt_cursor_checkpoint_id(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_close --
 *     WT_CURSOR->close default implementation.
 */
extern void __wt_cursor_close(WT_CURSOR *cursor) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_config_notsup --
 *     Unsupported cursor API call which takes config.
 */
extern int __wt_cursor_config_notsup(WT_CURSOR *cursor, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_copy_release_item --
 *     Release memory used by the key or value item in cursor copy debug mode.
 */
extern int __wt_cursor_copy_release_item(WT_CURSOR *cursor, WT_ITEM *item)
  WT_GCC_FUNC_DECL_ATTRIBUTE((cold)) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_dup_position --
 *     Set a cursor to another cursor's position.
 */
extern int __wt_cursor_dup_position(WT_CURSOR *to_dup, WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_equals --
 *     WT_CURSOR->equals default implementation.
 */
extern int __wt_cursor_equals(WT_CURSOR *cursor, WT_CURSOR *other, int *equalp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_get_hash --
 *     Get hash value from the given uri.
 */
extern void __wt_cursor_get_hash(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *to_dup,
  uint64_t *hash_value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_get_key --
 *     WT_CURSOR->get_key default implementation.
 */
extern int __wt_cursor_get_key(WT_CURSOR *cursor, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_get_raw_key --
 *     Temporarily force raw mode in a cursor to get a canonical copy of the key.
 */
extern int __wt_cursor_get_raw_key(WT_CURSOR *cursor, WT_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_get_raw_key_value --
 *     WT_CURSOR->get_raw_key_value default implementation
 */
extern int __wt_cursor_get_raw_key_value(WT_CURSOR *cursor, WT_ITEM *key, WT_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_get_raw_value --
 *     Temporarily force raw mode in a cursor to get a canonical copy of the value.
 */
extern int __wt_cursor_get_raw_value(WT_CURSOR *cursor, WT_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_get_value --
 *     WT_CURSOR->get_value default implementation.
 */
extern int __wt_cursor_get_value(WT_CURSOR *cursor, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_init --
 *     Default cursor initialization.
 */
extern int __wt_cursor_init(WT_CURSOR *cursor, const char *uri, WT_CURSOR *owner, const char *cfg[],
  WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_kv_not_set --
 *     Standard error message for key/values not set.
 */
extern int __wt_cursor_kv_not_set(WT_CURSOR *cursor, bool key) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_modify_value_format_notsup --
 *     Unsupported value format for cursor modify.
 */
extern int __wt_cursor_modify_value_format_notsup(WT_CURSOR *cursor, WT_MODIFY *entries,
  int nentries) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_notsup --
 *     Unsupported cursor actions.
 */
extern int __wt_cursor_notsup(WT_CURSOR *cursor) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_reconfigure --
 *     Set runtime-configurable settings.
 */
extern int __wt_cursor_reconfigure(WT_CURSOR *cursor, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_reopen_notsup --
 *     Unsupported cursor reopen.
 */
extern int __wt_cursor_reopen_notsup(WT_CURSOR *cursor, bool check_only)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_set_key --
 *     WT_CURSOR->set_key default implementation.
 */
extern void __wt_cursor_set_key(WT_CURSOR *cursor, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_set_notsup --
 *     Reset the cursor methods to not-supported.
 */
extern void __wt_cursor_set_notsup(WT_CURSOR *cursor)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_set_raw_key --
 *     Temporarily force raw mode in a cursor to set a canonical copy of the key.
 */
extern void __wt_cursor_set_raw_key(WT_CURSOR *cursor, WT_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_set_raw_value --
 *     Temporarily force raw mode in a cursor to set a canonical copy of the value.
 */
extern void __wt_cursor_set_raw_value(WT_CURSOR *cursor, WT_ITEM *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_set_value --
 *     WT_CURSOR->set_value default implementation.
 */
extern void __wt_cursor_set_value(WT_CURSOR *cursor, ...)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curstat_dsrc_final --
 *     Finalize a data-source statistics cursor.
 */
extern void __wt_curstat_dsrc_final(WT_CURSOR_STAT *cst)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curstat_init --
 *     Initialize a statistics cursor.
 */
extern int __wt_curstat_init(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *curjoin,
  const char *cfg[], WT_CURSOR_STAT *cst) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curstat_open --
 *     WT_SESSION->open_cursor method for the statistics cursor type.
 */
extern int __wt_curstat_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *other,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curtable_open --
 *     WT_SESSION->open_cursor method for table cursors.
 */
extern int __wt_curtable_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_curversion_open --
 *     Initialize a version cursor.
 */
extern int __wt_curversion_open(WT_SESSION_IMPL *session, const char *uri, WT_CURSOR *owner,
  const char *cfg[], WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_json_strlen --
 *     Return the number of bytes represented by a string in JSON format, or -1 if the format is
 *     incorrect.
 */
extern ssize_t __wt_json_strlen(const char *src, size_t srclen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_json_strncpy --
 *     Copy bytes of string in JSON format to a destination, up to dstlen bytes. If dstlen is
 *     greater than the needed size, the result if zero padded.
 */
extern int __wt_json_strncpy(WT_SESSION *wt_session, char **pdst, size_t dstlen, const char *src,
  size_t srclen) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_json_token --
 *     Return the type, start position and length of the next JSON token in the input. String tokens
 *     include the quotes. JSON can be entirely parsed using calls to this tokenizer, each call
 *     using a src pointer that is the previously returned tokstart + toklen. The token type
 *     returned is one of: 0 : EOF 's' : string 'i' : intnum 'f' : floatnum ':' : colon ',' : comma
 *     '{' : lbrace '}' : rbrace '[' : lbracket ']' : rbracket 'N' : null 'T' : true 'F' : false
 */
extern int __wt_json_token(WT_SESSION *wt_session, const char *src, int *toktype,
  const char **tokstart, size_t *toklen) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_json_tokname --
 *     Return a descriptive name from the token type returned by __wt_json_token.
 */
extern const char *__wt_json_tokname(int toktype)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_json_unpack_str --
 *     Unpack a string into JSON escaped format. Can be called with NULL buf for sizing and won't
 *     overwrite the buffer end in any case.
 */
extern size_t __wt_json_unpack_str(u_char *dest, size_t dest_len, const u_char *src, size_t src_len)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_table_range_truncate --
 *     Truncate of a cursor range, table implementation.
 */
extern int __wt_table_range_truncate(WT_TRUNCATE_INFO *trunc_info)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verbose_dump_backup --
 *     Print out the current state of the in-memory incremental backup structure.
 */
extern int __wt_verbose_dump_backup(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
