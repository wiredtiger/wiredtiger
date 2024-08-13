#pragma once

extern int __wti_blkcache_map(WT_SESSION_IMPL *session, WT_BLOCK *block, void **mapped_regionp,
  size_t *lengthp, void **mapped_cookiep) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_blkcache_map_read(WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr,
  size_t addr_size, bool *foundp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_blkcache_put(WT_SESSION_IMPL *session, WT_ITEM *data, const uint8_t *addr,
  size_t addr_size, bool write) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_blkcache_tiered_open(WT_SESSION_IMPL *session, const char *uri, uint32_t objectid,
  WT_BLOCK **blockp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_blkcache_unmap(WT_SESSION_IMPL *session, WT_BLOCK *block, void *mapped_region,
  size_t length, void *mapped_cookie) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern int __wti_bm_close_block(WT_SESSION_IMPL *session, WT_BLOCK *block)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __wti_blkcache_get(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size,
  WT_BLKCACHE_ITEM **blkcache_retp, bool *foundp, bool *skip_cache_putp);
extern void __wti_blkcache_get_read_handle(WT_BLOCK *block);
extern void __wti_blkcache_remove(WT_SESSION_IMPL *session, const uint8_t *addr, size_t addr_size);

#ifdef HAVE_UNITTEST

#endif
