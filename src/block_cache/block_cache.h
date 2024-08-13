#pragma once

extern int __wt_blkcache_get_handle(WT_SESSION_IMPL *session, WT_BM *bm, uint32_t objectid,
  bool reading, WT_BLOCK **blockp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_blkcache_open(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  bool forced_salvage, bool readonly, uint32_t allocsize, WT_BM **bmp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_blkcache_read(WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr,
  size_t addr_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_blkcache_setup(WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_blkcache_sweep_handles(WT_SESSION_IMPL *session, WT_BM *bm)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_blkcache_write(WT_SESSION_IMPL *session, WT_ITEM *buf, uint8_t *addr,
  size_t *addr_sizep, size_t *compressed_sizep, bool checkpoint, bool checkpoint_io,
  bool compressed) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_chunkcache_create_from_metadata(WT_SESSION_IMPL *session, const char *name,
  uint32_t id, wt_off_t file_offset, uint64_t cache_offset, size_t chunk_size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_chunkcache_free_external(
  WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t objectid, wt_off_t offset, uint32_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_chunkcache_get(WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t objectid,
  wt_off_t offset, uint32_t size, void *dst, bool *cache_hit)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_chunkcache_ingest(WT_SESSION_IMPL *session, const char *local_name,
  const char *sp_obj_name, uint32_t objectid) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_chunkcache_reconfig(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_chunkcache_salvage(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_chunkcache_setup(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wt_chunkcache_teardown(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wt_blkcache_destroy(WT_SESSION_IMPL *session);
extern void __wt_blkcache_release_handle(
  WT_SESSION_IMPL *session, WT_BLOCK *block, bool *last_release);
extern void __wt_blkcache_set_readonly(WT_SESSION_IMPL *session) WT_GCC_FUNC_DECL_ATTRIBUTE((cold));

#ifdef HAVE_UNITTEST
extern int __ut_chunkcache_bitmap_alloc(WT_SESSION_IMPL *session, size_t *bit_index)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __ut_chunkcache_bitmap_free(WT_SESSION_IMPL *session, size_t bit_index);

#endif
