#pragma once

/*
 * __wt_ext_metadata_insert --
 *     Insert a row into the metadata (external API version).
 */
extern int __wt_ext_metadata_insert(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *key, const char *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_metadata_remove --
 *     Remove a row from the metadata (external API version).
 */
extern int __wt_ext_metadata_remove(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *key) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_metadata_search --
 *     Return a copied row from the metadata (external API version). The caller is responsible for
 *     freeing the allocated memory.
 */
extern int __wt_ext_metadata_search(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *key, char **valuep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ext_metadata_update --
 *     Update a row in the metadata (external API version).
 */
extern int __wt_ext_metadata_update(WT_EXTENSION_API *wt_api, WT_SESSION *wt_session,
  const char *key, const char *value) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_apply_all --
 *     Apply a function to all files listed in the metadata, apart from the metadata file.
 */
extern int __wt_meta_apply_all(WT_SESSION_IMPL *session,
  int (*file_func)(WT_SESSION_IMPL *, const char *[]),
  int (*name_func)(WT_SESSION_IMPL *, const char *, bool *), const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_block_metadata --
 *     Build a version of the file's metadata for the block manager to store.
 */
extern int __wt_meta_block_metadata(WT_SESSION_IMPL *session, const char *config, WT_CKPT *ckpt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_checkpoint --
 *     Return a file's checkpoint information.
 */
extern int __wt_meta_checkpoint(WT_SESSION_IMPL *session, const char *fname, const char *checkpoint,
  WT_CKPT *ckpt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_checkpoint_by_name --
 *     Look up the requested named checkpoint in the metadata and return its order and time
 *     information.
 */
extern int __wt_meta_checkpoint_by_name(WT_SESSION_IMPL *session, const char *uri,
  const char *checkpoint, int64_t *orderp, uint64_t *timep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_checkpoint_clear --
 *     Clear a file's checkpoint.
 */
extern int __wt_meta_checkpoint_clear(WT_SESSION_IMPL *session, const char *fname)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_checkpoint_free --
 *     Clean up a single checkpoint structure.
 */
extern void __wt_meta_checkpoint_free(WT_SESSION_IMPL *session, WT_CKPT *ckpt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_checkpoint_last_name --
 *     Return the last unnamed checkpoint's name. Return the order number and wall-clock time if
 *     requested so the caller can check for races with a currently running checkpoint.
 */
extern int __wt_meta_checkpoint_last_name(
  WT_SESSION_IMPL *session, const char *fname, const char **namep, int64_t *orderp, uint64_t *timep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_ckptlist_free --
 *     Discard the checkpoint array.
 */
extern void __wt_meta_ckptlist_free(WT_SESSION_IMPL *session, WT_CKPT **ckptbasep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_ckptlist_get --
 *     Load all available checkpoint information for a file. Either use a cached copy of the
 *     checkpoints or rebuild from the metadata.
 */
extern int __wt_meta_ckptlist_get(WT_SESSION_IMPL *session, const char *fname, bool update,
  WT_CKPT **ckptbasep, size_t *allocated) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_ckptlist_get_from_config --
 *     Provided a metadata config, load all available checkpoint information for a file.
 */
extern int __wt_meta_ckptlist_get_from_config(WT_SESSION_IMPL *session, bool update,
  WT_CKPT **ckptbasep, size_t *allocatedp, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_ckptlist_set --
 *     Set a file's checkpoint value from the WT_CKPT list.
 */
extern int __wt_meta_ckptlist_set(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle,
  WT_CKPT *ckptbase, WT_LSN *ckptlsn) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_ckptlist_to_meta --
 *     Convert a checkpoint list into its metadata representation.
 */
extern int __wt_meta_ckptlist_to_meta(WT_SESSION_IMPL *session, WT_CKPT *ckptbase, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_ckptlist_update_config --
 *     Provided a metadata config and list of checkpoints, set a file's checkpoint value.
 */
extern int __wt_meta_ckptlist_update_config(WT_SESSION_IMPL *session, WT_CKPT *ckptbase,
  const char *oldcfg, char **newcfgp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_correct_base_write_gen --
 *     Update the connection's base write generation from all files in metadata at the end of the
 *     recovery checkpoint.
 */
extern int __wt_meta_correct_base_write_gen(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_load_prior_state --
 *     Initialize the connection's base write generation and most recent checkpoint time.
 */
extern int __wt_meta_load_prior_state(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_read_checkpoint_oldest --
 *     Fetch a checkpoint's oldest timestamp from the metadata. If the checkpoint name passed is
 *     null, returns the timestamp from the most recent checkpoint.
 */
extern int __wt_meta_read_checkpoint_oldest(WT_SESSION_IMPL *session, const char *ckpt_name,
  wt_timestamp_t *timestampp, uint64_t *ckpttime) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_read_checkpoint_snapshot --
 *     Fetch the snapshot data for a checkpoint from the metadata file. Reads the selected named
 *     checkpoint's snapshot, or if the checkpoint name passed is null, the most recent checkpoint's
 *     snapshot. The snapshot list returned is allocated and must be freed by the caller. Can be
 *     called with NULL return parameters to avoid (in particular) bothering to allocate the
 *     snapshot data if it's not needed. Note that if you retrieve the snapshot data you must also
 *     retrieve the snapshot count.
 */
extern int __wt_meta_read_checkpoint_snapshot(WT_SESSION_IMPL *session, const char *ckpt_name,
  uint64_t *snap_write_gen, uint64_t *snap_min, uint64_t *snap_max, uint64_t **snapshot,
  uint32_t *snapshot_count, uint64_t *ckpttime) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_read_checkpoint_timestamp --
 *     Fetch a checkpoint's checkpoint timestamp, aka stable timestamp, from the metadata. If the
 *     checkpoint name passed is null, returns the timestamp from the most recent checkpoint.
 *
 * Here "checkpoint timestamp" means "the stable timestamp saved with a checkpoint". This variance
 *     in terminology is confusing, but at this point not readily avoided.
 */
extern int __wt_meta_read_checkpoint_timestamp(WT_SESSION_IMPL *session, const char *ckpt_name,
  wt_timestamp_t *timestampp, uint64_t *ckpttime) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_saved_ckptlist_free --
 *     Discard the saved checkpoint list.
 */
extern void __wt_meta_saved_ckptlist_free(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_sysinfo_clear --
 *     Clear the system information (for a named checkpoint) from the metadata.
 */
extern int __wt_meta_sysinfo_clear(WT_SESSION_IMPL *session, const char *name, size_t namelen)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_sysinfo_set --
 *     Set the system information in the metadata.
 */
extern int __wt_meta_sysinfo_set(WT_SESSION_IMPL *session, bool full, const char *name,
  size_t namelen) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_checkpoint --
 *     Track a handle involved in a checkpoint.
 */
extern int __wt_meta_track_checkpoint(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_destroy --
 *     Release resources allocated for metadata tracking.
 */
extern int __wt_meta_track_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_discard --
 *     Cleanup metadata tracking when closing a session.
 */
extern void __wt_meta_track_discard(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_drop --
 *     Track a file drop, where the remove is deferred until commit.
 */
extern int __wt_meta_track_drop(WT_SESSION_IMPL *session, const char *filename)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_drop_object --
 *     Track a shared object file drop, where the remove is deferred until commit.
 */
extern int __wt_meta_track_drop_object(WT_SESSION_IMPL *session, WT_BUCKET_STORAGE *bstorage,
  const char *filename) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_fileop --
 *     Track a filesystem operation.
 */
extern int __wt_meta_track_fileop(WT_SESSION_IMPL *session, const char *olduri, const char *newuri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_handle_lock --
 *     Track a locked handle.
 */
extern int __wt_meta_track_handle_lock(WT_SESSION_IMPL *session, bool created)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_init --
 *     Initialize metadata tracking.
 */
extern int __wt_meta_track_init(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_off --
 *     Turn off metadata operation tracking, unrolling on error.
 */
extern int __wt_meta_track_off(WT_SESSION_IMPL *session, bool need_sync, bool unroll)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_on --
 *     Turn on metadata operation tracking.
 */
extern int __wt_meta_track_on(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_sub_off --
 *     Commit a group of operations independent of the main transaction.
 */
extern int __wt_meta_track_sub_off(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_track_sub_on --
 *     Start a group of operations that can be committed independent of the main transaction.
 */
extern void __wt_meta_track_sub_on(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_meta_update_connection --
 *     Update the connection's base write generation and most recent checkpoint time from the config
 *     string.
 */
extern int __wt_meta_update_connection(WT_SESSION_IMPL *session, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_btree_id_to_uri --
 *     Given a btree id, find the matching entry in the metadata and return a copy of the uri. The
 *     caller has to free the returned uri.
 */
extern int __wt_metadata_btree_id_to_uri(WT_SESSION_IMPL *session, uint32_t btree_id, char **uri)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_cursor --
 *     Returns the session's cached metadata cursor, unless it's in use, in which case it opens and
 *     returns another metadata cursor.
 */
extern int __wt_metadata_cursor(WT_SESSION_IMPL *session, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_cursor_close --
 *     Close a metadata cursor.
 */
extern int __wt_metadata_cursor_close(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_cursor_open --
 *     Opens a cursor on the metadata.
 */
extern int __wt_metadata_cursor_open(WT_SESSION_IMPL *session, const char *config,
  WT_CURSOR **cursorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_cursor_release --
 *     Release a metadata cursor.
 */
extern int __wt_metadata_cursor_release(WT_SESSION_IMPL *session, WT_CURSOR **cursorp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_free_ckptlist --
 *     Public entry point to __wt_meta_ckptlist_free (for wt list).
 */
extern void __wt_metadata_free_ckptlist(WT_SESSION *session, WT_CKPT *ckptbase)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_get_ckptlist --
 *     Public entry point to __wt_meta_ckptlist_get (for wt list).
 */
extern int __wt_metadata_get_ckptlist(WT_SESSION *session, const char *name, WT_CKPT **ckptbasep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_insert --
 *     Insert a row into the metadata.
 */
extern int __wt_metadata_insert(WT_SESSION_IMPL *session, const char *key, const char *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_remove --
 *     Remove a row from the metadata.
 */
extern int __wt_metadata_remove(WT_SESSION_IMPL *session, const char *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_search --
 *     Return a copied row from the metadata. The caller is responsible for freeing the allocated
 *     memory.
 */
extern int __wt_metadata_search(WT_SESSION_IMPL *session, const char *key, char **valuep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_turtle_rewrite --
 *     Rewrite the turtle file. We wrap this because the lower functions expect a URI key and config
 *     value pair for the metadata. This function exists to push out the other contents to the
 *     turtle file such as a change in compatibility information.
 */
extern int __wt_metadata_turtle_rewrite(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_metadata_update --
 *     Update a row in the metadata.
 */
extern int __wt_metadata_update(WT_SESSION_IMPL *session, const char *key, const char *value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_read_metadata_file --
 *     Open a text-based metadata file and iterate over the key value pairs calling the worker
 *     function for each of them.
 */
extern int __wt_read_metadata_file(WT_SESSION_IMPL *session, const char *file,
  int (*meta_entry_worker_func)(WT_SESSION_IMPL *, WT_ITEM *, WT_ITEM *, void *), void *state,
  bool *file_exist) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_reset_blkmod --
 *     Reset the incremental backup information, and recreate incremental backup information to
 *     indicate copying the entire file.
 */
extern int __wt_reset_blkmod(WT_SESSION_IMPL *session, const char *orig_config, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_turtle_exists --
 *     Return if the turtle file exists on startup.
 */
extern int __wt_turtle_exists(WT_SESSION_IMPL *session, bool *existp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_turtle_init --
 *     Check the turtle file and create if necessary.
 */
extern int __wt_turtle_init(WT_SESSION_IMPL *session, bool verify_meta, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_turtle_validate_version --
 *     Retrieve version numbers from the turtle file and validate them against our WiredTiger
 *     version.
 */
extern int __wt_turtle_validate_version(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST
extern int __ut_ckpt_verify_modified_bits(WT_ITEM *original_bitmap, WT_ITEM *new_bitmap, bool *ok)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#endif
