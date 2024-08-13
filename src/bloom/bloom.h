#pragma once

extern int __wt_bloom_close(WT_BLOOM *bloom) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bloom_create(WT_SESSION_IMPL *session, const char *uri, const char *config,
  uint64_t count, uint32_t factor, uint32_t k, WT_BLOOM **bloomp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bloom_drop(WT_BLOOM *bloom, const char *config) WT_GCC_FUNC_DECL_ATTRIBUTE(
  (visibility("default"))) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bloom_finalize(WT_BLOOM *bloom) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bloom_get(WT_BLOOM *bloom, WT_ITEM *key) WT_GCC_FUNC_DECL_ATTRIBUTE(
  (visibility("default"))) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bloom_hash_get(WT_BLOOM *bloom, WT_BLOOM_HASH *bhash)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bloom_inmem_get(WT_BLOOM *bloom, WT_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bloom_intersection(WT_BLOOM *bloom, WT_BLOOM *other)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_bloom_open(WT_SESSION_IMPL *session, const char *uri, uint32_t factor, uint32_t k,
  WT_CURSOR *owner, WT_BLOOM **bloomp) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_bloom_hash(WT_BLOOM *bloom, WT_ITEM *key, WT_BLOOM_HASH *bhash);
extern void __wt_bloom_insert(WT_BLOOM *bloom, WT_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")));

#ifdef HAVE_UNITTEST

#endif
