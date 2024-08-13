#pragma once

/*
 * __wt_tiered_close --
 *     Close a tiered data handle.
 */
extern int __wt_tiered_close(WT_SESSION_IMPL *session, WT_TIERED *tiered, bool final)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_conn_config --
 *     Parse and setup the storage server options for the connection.
 */
extern int __wt_tiered_conn_config(WT_SESSION_IMPL *session, const char **cfg, bool reconfig)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_discard --
 *     Discard a tiered data handle.
 */
extern int __wt_tiered_discard(WT_SESSION_IMPL *session, WT_TIERED *tiered, bool final)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_flush_work_wait --
 *     Wait for all flush work units in the work queue to be processed.
 */
extern void __wt_tiered_flush_work_wait(WT_SESSION_IMPL *session, uint32_t timeout)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_get_flush --
 *     Get the first flush work unit from the queue. If a non zero generation value is given, only
 *     return work units less than that value. The id information cannot change between our caller
 *     and here. The caller is responsible for freeing the work unit.
 */
extern void __wt_tiered_get_flush(WT_SESSION_IMPL *session, uint64_t generation,
  WT_TIERED_WORK_UNIT **entryp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_get_flush_finish --
 *     Get the first flush_finish work unit from the queue. The id information cannot change between
 *     our caller and here. The caller is responsible for freeing the work unit.
 */
extern void __wt_tiered_get_flush_finish(WT_SESSION_IMPL *session, WT_TIERED_WORK_UNIT **entryp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_get_remove_local --
 *     Get a remove local work unit if it is less than the time given. The caller is responsible for
 *     freeing the work unit.
 */
extern void __wt_tiered_get_remove_local(WT_SESSION_IMPL *session, uint64_t now,
  WT_TIERED_WORK_UNIT **entryp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_name --
 *     Given a dhandle structure and object number generate the URI name of the given type.
 */
extern int __wt_tiered_name(WT_SESSION_IMPL *session, WT_DATA_HANDLE *dhandle, uint32_t id,
  uint32_t flags, const char **retp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_open --
 *     Open a tiered data handle.
 */
extern int __wt_tiered_open(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_put_flush_finish --
 *     Add a flush_finish work unit to the queue.
 */
extern int __wt_tiered_put_flush_finish(WT_SESSION_IMPL *session, WT_TIERED *tiered, uint32_t id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_put_remove_local --
 *     Add a remove local work unit for the given ID to the queue.
 */
extern int __wt_tiered_put_remove_local(WT_SESSION_IMPL *session, WT_TIERED *tiered, uint32_t id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_remove_work --
 *     Remove all work on the queue that applies to the given tiered handle.
 */
extern void __wt_tiered_remove_work(WT_SESSION_IMPL *session, WT_TIERED *tiered, bool locked)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_requeue_work --
 *     Push an existing work unit to the queue. Assumes it was previously returned from one of the
 *     get functions, and it is being re-queued.
 */
extern void __wt_tiered_requeue_work(WT_SESSION_IMPL *session, WT_TIERED_WORK_UNIT *entry)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_set_metadata --
 *     Generate the tiered metadata information string into the given buffer.
 */
extern int __wt_tiered_set_metadata(WT_SESSION_IMPL *session, WT_TIERED *tiered, WT_ITEM *buf)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_switch --
 *     Switch metadata, external version.
 */
extern int __wt_tiered_switch(WT_SESSION_IMPL *session, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_tree_close --
 *     Close a tiered tree data handle.
 */
extern int __wt_tiered_tree_close(WT_SESSION_IMPL *session, WT_TIERED_TREE *tiered_tree)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_tree_open --
 *     Open a tiered tree data handle.
 */
extern int __wt_tiered_tree_open(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_tiered_work_free --
 *     Free a work unit and account for it in the flush state.
 */
extern void __wt_tiered_work_free(WT_SESSION_IMPL *session, WT_TIERED_WORK_UNIT *entry)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
