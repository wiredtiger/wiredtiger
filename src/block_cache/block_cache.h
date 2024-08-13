#pragma once

/*
 * __wt_blkcache_destroy --
 *     Destroy the block cache and free all memory.
 */
extern void __wt_blkcache_destroy(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_blkcache_get_handle --
 *     Get a cached block handle for an object, creating it if it doesn't exist.
 */
extern int __wt_blkcache_get_handle(WT_SESSION_IMPL *session, WT_BM *bm, uint32_t objectid,
  bool reading, WT_BLOCK **blockp) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_blkcache_open --
 *     Open a file.
 */
extern int __wt_blkcache_open(WT_SESSION_IMPL *session, const char *uri, const char *cfg[],
  bool forced_salvage, bool readonly, uint32_t allocsize, WT_BM **bmp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_blkcache_read --
 *     Read an address-cookie referenced block into a buffer.
 */
extern int __wt_blkcache_read(WT_SESSION_IMPL *session, WT_ITEM *buf, const uint8_t *addr,
  size_t addr_size) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_blkcache_release_handle --
 *     Update block handle when a read operation completes.
 */
extern void __wt_blkcache_release_handle(WT_SESSION_IMPL *session, WT_BLOCK *block,
  bool *last_release) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_blkcache_set_readonly --
 *     Set the block API to read-only.
 */
extern void __wt_blkcache_set_readonly(WT_SESSION_IMPL *session) WT_GCC_FUNC_DECL_ATTRIBUTE((cold))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_blkcache_setup --
 *     Set up the block cache.
 */
extern int __wt_blkcache_setup(WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_blkcache_sweep_handles --
 *     Free blocks from the manager's handle array if possible.
 */
extern int __wt_blkcache_sweep_handles(WT_SESSION_IMPL *session, WT_BM *bm)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_blkcache_write --
 *     Write a buffer into a block, returning the block's address cookie.
 */
extern int __wt_blkcache_write(WT_SESSION_IMPL *session, WT_ITEM *buf, uint8_t *addr,
  size_t *addr_sizep, size_t *compressed_sizep, bool checkpoint, bool checkpoint_io,
  bool compressed) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_chunkcache_create_from_metadata --
 *     Create a new chunk from some stored properties, and link it to the relevant chunk data (on
 *     disk).
 */
extern int __wt_chunkcache_create_from_metadata(WT_SESSION_IMPL *session, const char *name,
  uint32_t id, wt_off_t file_offset, uint64_t cache_offset, size_t chunk_size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_chunkcache_free_external --
 *     Find chunks in the chunk cache using object id, and free the chunks.
 */
extern int __wt_chunkcache_free_external(
  WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t objectid, wt_off_t offset, uint32_t size)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_chunkcache_get --
 *     Return the data to the caller if we have it. Otherwise read it from storage and cache it.
 *
 * During these operations we are holding one or more bucket locks. A bucket lock protects the
 *     linked list (i.e., the chain) or chunks hashing into the same bucket. We hold the bucket lock
 *     whenever we are looking for and are inserting a new chunk into that bucket. We must hold the
 *     lock throughout the entire operation: realizing that the chunk is not present, deciding to
 *     cache it, allocating the chunks metadata and inserting it into the chain. If we release the
 *     lock during this process, another thread might cache the same chunk; we do not want that. We
 *     insert the new chunk into the cache in the not valid state. Once we insert the chunk, we can
 *     release the lock. As long as the chunk is marked as invalid, no other thread will try to
 *     re-cache it or to read it. As a result, we can read data from the remote storage into this
 *     chunk without holding the lock: this is what the current code does. We can even allocate the
 *     space for that chunk outside the critical section: the current code does not do that. Once we
 *     read the data into the chunk, we atomically set the valid flag, so other threads can use it.
 */
extern int __wt_chunkcache_get(WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t objectid,
  wt_off_t offset, uint32_t size, void *dst, bool *cache_hit)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_chunkcache_ingest --
 *     Read all the contents from a file and insert it into the chunk cache.
 */
extern int __wt_chunkcache_ingest(WT_SESSION_IMPL *session, const char *local_name,
  const char *sp_obj_name, uint32_t objectid) WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_chunkcache_reconfig --
 *     Re-configure the chunk cache.
 */
extern int __wt_chunkcache_reconfig(WT_SESSION_IMPL *session, const char **cfg)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_chunkcache_salvage --
 *     Remove any knowledge of any extant chunk cache metadata. We can always rebuild the cache
 *     later, so make no attempt at a "real" salvage.
 */
extern int __wt_chunkcache_salvage(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_chunkcache_setup --
 *     Set up the chunk cache.
 */
extern int __wt_chunkcache_setup(WT_SESSION_IMPL *session, const char *cfg[])
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_chunkcache_teardown --
 *     Tear down the chunk cache.
 */
extern int __wt_chunkcache_teardown(WT_SESSION_IMPL *session)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST
extern int __ut_chunkcache_bitmap_alloc(WT_SESSION_IMPL *session, size_t *bit_index)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));
extern void __ut_chunkcache_bitmap_free(WT_SESSION_IMPL *session, size_t bit_index);

#endif
