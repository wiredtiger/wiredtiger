#pragma once

extern int __wt_ext_metadata_insert(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *key, const char *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_metadata_remove(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *key) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_metadata_search(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *key, char **valuep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_ext_metadata_update(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *key, const char *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_apply_all(WT_SESSION_IMPL *session,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_block_metadata(WT_SESSION_IMPL *session, const char *config, WT_CKPT *ckpt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_checkpoint(WT_SESSION_IMPL *session, const char *fname, const char *checkpoint,
  WT_CKPT *ckpt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_checkpoint_by_name(WT_SESSION_IMPL *session, const char *uri,
  const char *checkpoint, int64_t *orderp, uint64_t *timep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_checkpoint_clear(WT_SESSION_IMPL *session, const char *fname)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_checkpoint_last_name(
  WT_SESSION_IMPL *session, const char *fname, const char **namep, int64_t *orderp, uint64_t *timep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_ckptlist_get(WT_SESSION_IMPL *session, const char *fname, bool update,
  WT_CKPT **ckptbasep, size_t *allocated) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_ckptlist_get_from_config(WT_SESSION_IMPL *session, bool update,
  WT_CKPT **ckptbasep, size_t *allocatedp, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_ckptlist_set(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle,
  WT_CKPT *ckptbase, WT_LSN *ckptlsn) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_ckptlist_to_meta(WT_SESSION_IMPL *session, WT_CKPT *ckptbase, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_ckptlist_update_config(WT_SESSION_IMPL *session, WT_CKPT *ckptbase,
  const char *oldcfg, char **newcfgp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_correct_base_write_gen(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_load_prior_state(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_read_checkpoint_oldest(WT_SESSION_IMPL *session, const char *ckpt_name,
  wt_timestamp_t *timestampp, uint64_t *ckpttime) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_read_checkpoint_snapshot(WT_SESSION_IMPL *session, const char *ckpt_name,
  uint64_t *snap_write_gen, uint64_t *snap_min, uint64_t *snap_max, uint64_t **snapshot,
  uint32_t *snapshot_count, uint64_t *ckpttime) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_read_checkpoint_timestamp(WT_SESSION_IMPL *session, const char *ckpt_name,
  wt_timestamp_t *timestampp, uint64_t *ckpttime) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_sysinfo_clear(WT_SESSION_IMPL *session, const char *name, size_t namelen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_sysinfo_set(WT_SESSION_IMPL *session, bool full, const char *name,
  size_t namelen) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_track_checkpoint(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_track_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_track_drop(WT_SESSION_IMPL *session, const char *filename)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_track_drop_object(WT_SESSION_IMPL *session, WT_BUCKET_STORAGE *bstorage,
  const char *filename) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_track_fileop(WT_SESSION_IMPL *session, const char *olduri, const char *newuri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_track_handle_lock(WT_SESSION_IMPL *session, bool created)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_track_init(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_track_off(WT_SESSION_IMPL *session, bool need_sync, bool unroll)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_track_on(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_track_sub_off(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_meta_update_connection(WT_SESSION_IMPL *session, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_btree_id_to_uri(WT_SESSION_IMPL *session, uint32_t btree_id, char **uri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_cursor(WT_SESSION_IMPL *session, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_cursor_close(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_cursor_open(WT_SESSION_IMPL *session, const char *config,
  WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_cursor_release(WT_SESSION_IMPL *session, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_get_ckptlist(WT_SESSION *session, const char *name, WT_CKPT **ckptbasep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_insert(WT_SESSION_IMPL *session, const char *key, const char *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_remove(WT_SESSION_IMPL *session, const char *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_search(WT_SESSION_IMPL *session, const char *key, char **valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_turtle_rewrite(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_metadata_update(WT_SESSION_IMPL *session, const char *key, const char *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_read_metadata_file(WT_SESSION_IMPL *session, const char *file,
  int (*meta_entry_worker_func)(WT_SESSION_IMPL *, WT_ITEM *, WT_ITEM *, void *), void *state,
  bool *file_exist) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_reset_blkmod(WT_SESSION_IMPL *session, const char *orig_config, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_turtle_exists(WT_SESSION_IMPL *session, bool *existp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_turtle_init(WT_SESSION_IMPL *session, bool verify_meta, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_turtle_validate_version(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_meta_checkpoint_free(WT_SESSION_IMPL *session, WT_CKPT *ckpt);
extern void __wt_meta_ckptlist_free(WT_SESSION_IMPL *session, WT_CKPT **ckptbasep);
extern void __wt_meta_saved_ckptlist_free(WT_SESSION_IMPL *session);
extern void __wt_meta_track_discard(WT_SESSION_IMPL *session);
extern void __wt_meta_track_sub_on(WT_SESSION_IMPL *session);
extern void __wt_metadata_free_ckptlist(WT_SESSION *session, WT_CKPT *ckptbase)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));

#ifdef HAVE_UNITTEST
extern int __ut_ckpt_verify_modified_bits(WT_ITEM *original_bitmap, WT_ITEM *new_bitmap, bool *ok)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#endif
