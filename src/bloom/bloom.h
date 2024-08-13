#pragma once

/*
 * __wt_bloom_close --
 *     Close the Bloom filter, release any resources.
 */
extern int __wt_bloom_close(WT_BLOOM *bloom) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bloom_create --
 *     Creates and configures a WT_BLOOM handle, allocates a bitstring in memory to use while
 *     populating the bloom filter. count - is the expected number of inserted items factor - is the
 *     number of bits to use per inserted item k - is the number of hash values to set or test per
 *     item
 */
extern int __wt_bloom_create(WT_SESSION_IMPL *session, const char *uri, const char *config,
  uint64_t count, uint32_t factor, uint32_t k, WT_BLOOM **bloomp)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bloom_drop --
 *     Drop a Bloom filter, release any resources.
 */
extern int __wt_bloom_drop(WT_BLOOM *bloom, const char *config)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bloom_finalize --
 *     Writes the Bloom filter to stable storage. After calling finalize, only read operations can
 *     be performed on the bloom filter.
 */
extern int __wt_bloom_finalize(WT_BLOOM *bloom) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bloom_get --
 *     Tests whether the given key is in the Bloom filter. Returns zero if found, WT_NOTFOUND if
 *     not.
 */
extern int __wt_bloom_get(WT_BLOOM *bloom, WT_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bloom_hash --
 *     Calculate the hash values for a given key.
 */
extern void __wt_bloom_hash(WT_BLOOM *bloom, WT_ITEM *key, WT_BLOOM_HASH *bhash)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bloom_hash_get --
 *     Tests whether the key (as given by its hash signature) is in the Bloom filter. Returns zero
 *     if found, WT_NOTFOUND if not.
 */
extern int __wt_bloom_hash_get(WT_BLOOM *bloom, WT_BLOOM_HASH *bhash)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bloom_inmem_get --
 *     Tests whether the given key is in the Bloom filter. This can be used in place of
 *     __wt_bloom_get for Bloom filters that are memory only.
 */
extern int __wt_bloom_inmem_get(WT_BLOOM *bloom, WT_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bloom_insert --
 *     Adds the given key to the Bloom filter.
 */
extern void __wt_bloom_insert(WT_BLOOM *bloom, WT_ITEM *key)
  WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
    WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bloom_intersection --
 *     Modify the Bloom filter to contain the intersection of this filter with another.
 */
extern int __wt_bloom_intersection(WT_BLOOM *bloom, WT_BLOOM *other)
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

/*
 * __wt_bloom_open --
 *     Open a Bloom filter object for use by a single session. The filter must have been created and
 *     finalized.
 */
extern int __wt_bloom_open(WT_SESSION_IMPL *session, const char *uri, uint32_t factor, uint32_t k,
  WT_CURSOR *owner, WT_BLOOM **bloomp) WT_GCC_FUNC_DECL_ATTRIBUTE((visibility("default")))
  WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result));

#ifdef HAVE_UNITTEST

#endif
