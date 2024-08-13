#pragma once

/*
 * __wt_rollback_to_stable_init --
 *     Initialize the data structures for the rollback to stable subsystem
 */
extern void __wt_rollback_to_stable_init(WT_CONNECTION_IMPL *conn)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
