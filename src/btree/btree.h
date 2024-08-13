#pragma once

/*
 * __wt_addr_string --
 *     Load a buffer with a printable, nul-terminated representation of an address.
 */
extern const char *__wt_addr_string(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size,
  WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_close --
 *     Close a btree cursor.
 */
extern int __wt_btcur_close(WT_CURSOR_BTREE *cbt, bool lowlevel)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_compare --
 *     Return a comparison between two cursors.
 */
extern int __wt_btcur_compare(WT_CURSOR_BTREE *a_arg, WT_CURSOR_BTREE *b_arg, int *cmpp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_equals --
 *     Return an equality comparison between two cursors.
 */
extern int __wt_btcur_equals(WT_CURSOR_BTREE *a_arg, WT_CURSOR_BTREE *b_arg, int *equalp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_free_cached_memory --
 *     Discard internal buffers held by this cursor.
 */
extern void __wt_btcur_free_cached_memory(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_init --
 *     Initialize a cursor used for internal purposes.
 */
extern void __wt_btcur_init(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_insert --
 *     Insert a record into the tree.
 */
extern int __wt_btcur_insert(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_insert_check --
 *     Check whether an update would conflict. This can replace WT_CURSOR::insert, so it only checks
 *     for conflicts without updating the tree. It is used to maintain snapshot isolation for
 *     transactions that span multiple chunks in an LSM tree.
 */
extern int __wt_btcur_insert_check(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_modify --
 *     Modify a record in the tree.
 */
extern int __wt_btcur_modify(WT_CURSOR_BTREE *cbt, WT_MODIFY *entries, int nentries)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_next --
 *     Move to the next record in the tree.
 */
extern int __wt_btcur_next(WT_CURSOR_BTREE *cbt, bool truncating)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_next_random --
 *     Move to a random record in the tree. There are two algorithms, one where we select a record
 *     at random from the whole tree on each retrieval and one where we first select a record at
 *     random from the whole tree, and then subsequently sample forward from that location. The
 *     sampling approach allows us to select reasonably uniform random points from unbalanced trees.
 */
extern int __wt_btcur_next_random(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_open --
 *     Open a btree cursor.
 */
extern void __wt_btcur_open(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_prev --
 *     Move to the previous record in the tree.
 */
extern int __wt_btcur_prev(WT_CURSOR_BTREE *cbt, bool truncating)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_range_truncate --
 *     Discard a cursor range from the tree.
 */
extern int __wt_btcur_range_truncate(WT_TRUNCATE_INFO *trunc_info)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_remove --
 *     Remove a record from the tree.
 */
extern int __wt_btcur_remove(WT_CURSOR_BTREE *cbt, bool positioned)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_reserve --
 *     Reserve a record in the tree.
 */
extern int __wt_btcur_reserve(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_reset --
 *     Invalidate the cursor position.
 */
extern int __wt_btcur_reset(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_search --
 *     Search for a matching record in the tree.
 */
extern int __wt_btcur_search(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_search_near --
 *     Search for a record in the tree.
 */
extern int __wt_btcur_search_near(WT_CURSOR_BTREE *cbt, int *exactp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_search_prepared --
 *     Search and return exact matching records only.
 */
extern int __wt_btcur_search_prepared(WT_CURSOR *cursor, WT_UPDATE **updp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btcur_update --
 *     Update a record in the tree.
 */
extern int __wt_btcur_update(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_close --
 *     Close a Btree.
 */
extern int __wt_btree_close(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_config_encryptor --
 *     Return an encryptor handle based on the configuration.
 */
extern int __wt_btree_config_encryptor(WT_SESSION_IMPL *session, const char **cfg,
  WT_KEYED_ENCRYPTOR **kencryptorp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_discard --
 *     Discard a Btree.
 */
extern int __wt_btree_discard(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_open --
 *     Open a Btree.
 */
extern int __wt_btree_open(WT_SESSION_IMPL *session, const char *op_cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_stat_init --
 *     Initialize the Btree statistics.
 */
extern int __wt_btree_stat_init(WT_SESSION_IMPL *session, WT_CURSOR_STAT *cst)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_btree_switch_object --
 *     Switch to a writeable object for a tiered btree.
 */
extern int __wt_btree_switch_object(WT_SESSION_IMPL *session, uint32_t objectid)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checkpoint_cleanup_create --
 *     Start the checkpoint cleanup thread.
 */
extern int __wt_checkpoint_cleanup_create(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checkpoint_cleanup_destroy --
 *     Destroy the checkpoint cleanup thread.
 */
extern int __wt_checkpoint_cleanup_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_checkpoint_cleanup_trigger --
 *     Trigger the checkpoint cleanup thread.
 */
extern void __wt_checkpoint_cleanup_trigger(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_col_modify --
 *     Column-store delete, insert, and update.
 */
extern int __wt_col_modify(WT_CURSOR_BTREE *cbt, uint64_t recno, const WT_ITEM *value,
  WT_UPDATE **updp_arg, u_int modify_type, bool exclusive, bool restore)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_col_search --
 *     Search a column-store tree for a specific record-based key.
 */
extern int __wt_col_search(WT_CURSOR_BTREE *cbt, uint64_t search_recno, WT_REF *leaf,
  bool leaf_safe, bool *leaf_foundp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_compact --
 *     Compact a file.
 */
extern int __wt_compact(WT_SESSION_IMPL *session) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_key_order_init --
 *     Initialize key ordering checks for cursor movements after a successful search.
 */
extern int __wt_cursor_key_order_init(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_key_order_reset --
 *     Turn off key ordering checks for cursor movements.
 */
extern void __wt_cursor_key_order_reset(WT_CURSOR_BTREE *cbt)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_cursor_truncate --
 *     Discard a cursor range from row-store or variable-width column-store tree.
 */
extern int __wt_cursor_truncate(WT_CURSOR_BTREE *start, WT_CURSOR_BTREE *stop,
  int (*rmfunc)(WT_CURSOR_BTREE *, const WT_ITEM *, u_int))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_debug_addr --
 *     Read and dump a disk page in debugging mode, using an addr/size pair.
 */
extern int __wt_debug_addr(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size,
  const char *ofile) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_debug_addr_print --
 *     Print out an address.
 */
extern int __wt_debug_addr_print(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_debug_cursor_page --
 *     Dump the in-memory information for a cursor-referenced page.
 */
extern int __wt_debug_cursor_page(void *cursor_arg, const char *ofile)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_debug_cursor_tree_hs --
 *     Dump the history store tree given a user cursor.
 */
extern int __wt_debug_cursor_tree_hs(void *cursor_arg, const char *ofile)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_debug_offset --
 *     Read and dump a disk page in debugging mode, using a file offset/size/checksum triplet.
 */
extern int __wt_debug_offset(WT_SESSION_IMPL *session, wt_off_t offset, uint32_t size,
  uint32_t checksum, const char *ofile, bool dump_all_data, bool dump_key_data)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_debug_set_verbose --
 *     Set verbose flags from the debugger.
 */
extern int __wt_debug_set_verbose(WT_SESSION_IMPL *session, const char *v)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_debug_tree --
 *     Dump the in-memory information for a tree, not including leaf pages.
 */
extern int __wt_debug_tree(void *session_arg, WT_BTREE *btree, WT_REF *ref, const char *ofile)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_debug_tree_all --
 *     Dump the in-memory information for a tree, including leaf pages.
 */
extern int __wt_debug_tree_all(void *session_arg, WT_BTREE *btree, WT_REF *ref, const char *ofile)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_debug_tree_shape --
 *     Dump the shape of the in-memory tree.
 */
extern int __wt_debug_tree_shape(WT_SESSION_IMPL *session, WT_REF *ref, const char *ofile)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_delete_page_rollback --
 *     Transaction rollback for a fast-truncate operation.
 */
extern int __wt_delete_page_rollback(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_delete_redo_window_cleanup --
 *     Clear old transaction IDs from already-loaded page_del structures to make them look like we
 *     just unpacked the information. Called after the tree write generation is bumped during
 *     recovery so that old transaction IDs don't come back to life. Note that this can only fail if
 *     something goes wrong in the tree walk; it doesn't itself ever fail.
 */
extern int __wt_delete_redo_window_cleanup(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_free_update_list --
 *     Walk a WT_UPDATE forward-linked list and free the per-thread combination of a WT_UPDATE
 *     structure and its associated data.
 */
extern void __wt_free_update_list(WT_SESSION_IMPL *session, WT_UPDATE **updp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_import_repair --
 *     Import a WiredTiger file into the database and reconstruct its metadata.
 */
extern int __wt_import_repair(WT_SESSION_IMPL *session, const char *uri, char **configp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_key_return --
 *     Change the cursor to reference an internal return key.
 */
extern int __wt_key_return(WT_CURSOR_BTREE *cbt) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_key_string --
 *     Load a buffer with a printable, nul-terminated representation of a key.
 */
extern const char *__wt_key_string(WT_SESSION_IMPL *session, const void *data_arg, size_t size,
  const char *key_format, WT_ITEM *buf) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_multi_to_ref --
 *     Move a multi-block entry into a WT_REF structure.
 */
extern int __wt_multi_to_ref(WT_SESSION_IMPL *session, WT_PAGE *page, WT_MULTI *multi,
  WT_REF **refp, size_t *incrp, bool closing) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ovfl_discard --
 *     Discard an on-page overflow value, and reset the page's cell.
 */
extern int __wt_ovfl_discard(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL *cell)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ovfl_read --
 *     Bring an overflow item into memory.
 */
extern int __wt_ovfl_read(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL_UNPACK_COMMON *unpack,
  WT_ITEM *store, bool *decoded) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ovfl_remove --
 *     Remove an overflow value.
 */
extern int __wt_ovfl_remove(WT_SESSION_IMPL *session, WT_PAGE *page, WT_CELL_UNPACK_KV *unpack)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_alloc --
 *     Create or read a page into the cache.
 */
extern int __wt_page_alloc(WT_SESSION_IMPL *session, uint8_t type, uint32_t alloc_entries,
  bool alloc_refs, WT_PAGE **pagep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_in_func --
 *     Acquire a hazard pointer to a page; if the page is not in-memory, read it from the disk and
 *     build an in-memory version.
 */
extern int __wt_page_in_func(WT_SESSION_IMPL *session, WT_REF *ref, uint32_t flags
#ifdef HAVE_DIAGNOSTIC
  ,
  const char *func, int line
#endif
  ) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_modify_alloc --
 *     Allocate a page's modification structure.
 */
extern int __wt_page_modify_alloc(WT_SESSION_IMPL *session, WT_PAGE *page)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_out --
 *     Discard an in-memory page, freeing all memory associated with it.
 */
extern void __wt_page_out(WT_SESSION_IMPL *session, WT_PAGE **pagep)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_page_type_string --
 *     Return a string representing the page type.
 */
extern const char *__wt_page_type_string(u_int type)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_prefetch_page_in --
 *     Does the heavy lifting of reading a page into the cache. Immediately releases the page since
 *     reading it in is the useful side effect here. Must be called while holding a dhandle.
 */
extern int __wt_prefetch_page_in(WT_SESSION_IMPL *session, WT_PREFETCH_QUEUE_ENTRY *pe)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_random_descent --
 *     Find a random page in a tree for either sampling or eviction.
 */
extern int __wt_random_descent(WT_SESSION_IMPL *session, WT_REF **refp, uint32_t flags,
  WT_RAND_STATE *rnd) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_read_cell_time_window --
 *     Read the time window from the cell.
 */
extern bool __wt_read_cell_time_window(WT_CURSOR_BTREE *cbt, WT_TIME_WINDOW *tw)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ref_addr_free --
 *     Free the address in a reference, if necessary.
 */
extern void __wt_ref_addr_free(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_ref_out --
 *     Discard an in-memory page, freeing all memory associated with it.
 */
extern void __wt_ref_out(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_root_ref_init --
 *     Initialize a tree root reference, and link in the root page.
 */
extern void __wt_root_ref_init(WT_SESSION_IMPL *session, WT_REF *root_ref, WT_PAGE *root,
  bool is_recno) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_ikey_alloc --
 *     Instantiate a key in a WT_IKEY structure.
 */
extern int __wt_row_ikey_alloc(WT_SESSION_IMPL *session, uint32_t cell_offset, const void *key,
  size_t size, WT_IKEY **ikeyp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_key_copy --
 *     Get a copy of a row-store leaf-page key.
 */
extern int __wt_row_leaf_key_copy(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip,
  WT_ITEM *key) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_leaf_key_work --
 *     Return a reference to a row-store leaf-page key, optionally instantiate the key into the
 *     in-memory page.
 */
extern int __wt_row_leaf_key_work(WT_SESSION_IMPL *session, WT_PAGE *page, WT_ROW *rip_arg,
  WT_ITEM *keyb, bool instantiate) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_modify --
 *     Row-store insert, update and delete.
 */
extern int __wt_row_modify(WT_CURSOR_BTREE *cbt, const WT_ITEM *key, const WT_ITEM *value,
  WT_UPDATE **updp_arg, u_int modify_type, bool exclusive, bool restore)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_row_search --
 *     Search a row-store tree for a specific key.
 */
extern int __wt_row_search(WT_CURSOR_BTREE *cbt, WT_ITEM *srch_key, bool insert, WT_REF *leaf,
  bool leaf_safe, bool *leaf_foundp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_salvage --
 *     Salvage a Btree.
 */
extern int __wt_salvage(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_search_insert --
 *     Search a row-store insert list, creating a skiplist stack as we go.
 */
extern int __wt_search_insert(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_INSERT_HEAD *ins_head, WT_ITEM *srch_key) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_split_insert --
 *     Split a page's last insert list entries into a separate page.
 */
extern int __wt_split_insert(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_split_multi --
 *     Split a page into multiple pages.
 */
extern int __wt_split_multi(WT_SESSION_IMPL *session, WT_REF *ref, int closing)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_split_reverse --
 *     Reverse split (rewrite a parent page's index to reflect an empty page).
 */
extern int __wt_split_reverse(WT_SESSION_IMPL *session, WT_REF *ref)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_split_rewrite --
 *     Rewrite an in-memory page with a new version.
 */
extern int __wt_split_rewrite(WT_SESSION_IMPL *session, WT_REF *ref, WT_MULTI *multi)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_sync_file --
 *     Flush pages for a specific file.
 */
extern int __wt_sync_file(WT_SESSION_IMPL *session, WT_CACHE_OP syncop)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tree_walk --
 *     Move to the next/previous page in the tree.
 */
extern int __wt_tree_walk(WT_SESSION_IMPL *session, WT_REF **refp, uint32_t flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tree_walk_count --
 *     Move to the next/previous page in the tree, tracking how many references were visited to get
 *     there.
 */
extern int __wt_tree_walk_count(WT_SESSION_IMPL *session, WT_REF **refp, uint64_t *walkcntp,
  uint32_t flags) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tree_walk_custom_skip --
 *     Walk the tree calling a custom function to decide whether to skip refs.
 */
extern int __wt_tree_walk_custom_skip(WT_SESSION_IMPL *session, WT_REF **refp,
  int (*skip_func)(WT_SESSION_IMPL *, WT_REF *, void *, bool, bool *), void *func_cookie,
  uint32_t flags) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_update_obsolete_check --
 *     Check for obsolete updates and force evict the page if the update list is too long.
 */
extern void __wt_update_obsolete_check(WT_SESSION_IMPL *session, WT_CURSOR_BTREE *cbt,
  WT_UPDATE *upd, bool update_accounting) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_value_return --
 *     Change the cursor to reference an update return value.
 */
extern void __wt_value_return(WT_CURSOR_BTREE *cbt, WT_UPDATE_VALUE *upd_value)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_value_return_buf --
 *     Change a buffer to reference an internal original-page return value. If we see an overflow
 *     removed cell, we have raced with checkpoint freeing the overflow cell. Return restart for the
 *     caller to retry the read.
 */
extern int __wt_value_return_buf(WT_CURSOR_BTREE *cbt, WT_REF *ref, WT_ITEM *buf,
  WT_TIME_WINDOW *tw) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verify --
 *     Verify a file.
 */
extern int __wt_verify(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verify_dsk --
 *     Verify a single Btree page as read from disk.
 */
extern int __wt_verify_dsk(WT_SESSION_IMPL *session, const char *tag, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_verify_dsk_image --
 *     Verify a single block as read from disk.
 */
extern int __wt_verify_dsk_image(WT_SESSION_IMPL *session, const char *tag,
  const WT_PAGE_HEADER *dsk, size_t size, WT_ADDR *addr, uint32_t verify_flags)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
