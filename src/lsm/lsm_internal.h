#pragma once

extern bool __wti_lsm_chunk_visible_all(WT_SESSION_IMPL *session, WT_LSM_CHUNK *chunk)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_clsm_close(WT_CURSOR *cursor) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_clsm_init_merge(WT_CURSOR *cursor, u_int start_chunk, uint32_t start_id,
  u_int nchunks) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_clsm_open_bulk(WT_CURSOR_LSM *clsm, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_checkpoint_chunk(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree,
  WT_LSM_CHUNK *chunk) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_free_chunks(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_get_chunk_to_flush(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, bool force,
  WT_LSM_CHUNK **chunkp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_manager_pop_entry(WT_SESSION_IMPL *session, uint32_t type,
  WT_LSM_WORK_UNIT **entryp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_manager_push_entry(WT_SESSION_IMPL *session, uint32_t type, uint32_t flags,
  WT_LSM_TREE *lsm_tree) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_manager_start(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_merge(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, u_int id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_merge_update_tree(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree,
  u_int start_chunk, u_int nchunks, WT_LSM_CHUNK *chunk)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_meta_read(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_meta_write(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree,
  const char *newconfig) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_tree_bloom_name(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, uint32_t id,
  const char **retp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_tree_chunk_name(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, uint32_t id,
  uint32_t generation, const char **retp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_tree_close_all(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_tree_retire_chunks(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree,
  u_int start_chunk, u_int nchunks) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_tree_set_chunk_size(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree,
  WT_LSM_CHUNK *chunk) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_tree_setup_bloom(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree,
  WT_LSM_CHUNK *chunk) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_tree_setup_chunk(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree,
  WT_LSM_CHUNK *chunk) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_tree_switch(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_work_bloom(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_work_enable_evict(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_work_switch(WT_SESSION_IMPL *session, WT_LSM_WORK_UNIT **entryp, bool *ran)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_worker_start(WT_SESSION_IMPL *session, WT_LSM_WORKER_ARGS *args)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_lsm_worker_stop(WT_SESSION_IMPL *session, WT_LSM_WORKER_ARGS *args)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_lsm_manager_clear_tree(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree);
extern void __wti_lsm_manager_free_work_unit(WT_SESSION_IMPL *session, WT_LSM_WORK_UNIT *entry);
extern void __wti_lsm_tree_readlock(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree);
extern void __wti_lsm_tree_readunlock(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree);
extern void __wti_lsm_tree_throttle(
  WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree, bool decrease_only);
extern void __wti_lsm_tree_writelock(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree);
extern void __wti_lsm_tree_writeunlock(WT_SESSION_IMPL *session, WT_LSM_TREE *lsm_tree);

#ifdef HAVE_UNITTEST

#endif
