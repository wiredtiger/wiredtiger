#pragma once

extern int __wti_tiered_bucket_config(WT_SESSION_IMPL *session, const char *cfg[],
  WT_BUCKET_STORAGE **bstoragep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_tiered_put_flush(WT_SESSION_IMPL *session, WT_TIERED *tiered, uint32_t id,
  uint64_t generation) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_tiered_put_remove_shared(WT_SESSION_IMPL *session, WT_TIERED *tiered, uint32_t id)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_tiered_get_remove_shared(WT_SESSION_IMPL *session, WT_TIERED_WORK_UNIT **entryp);

#ifdef HAVE_UNITTEST

#endif
