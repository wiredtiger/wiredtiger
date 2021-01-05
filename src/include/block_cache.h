/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WiredTiger's block cache. It is used to cache blocks identical to those that live on disk in a
 * faster storage medium, such as NVRAM.
 */

#ifdef HAVE_LIBMEMKIND
#include <memkind.h>
#endif /* HAVE_LIBMEMKIND */

#define BLKCACHE_HASHSIZE_DEFAULT 32768
#define BLKCACHE_HASHSIZE_MIN 512
#define BLKCACHE_HASHSIZE_MAX 1024*1024*1024

#define BLKCACHE_TRACE 0

#define WT_BLKCACHE_FULL   -2
#define WT_BLKCACHE_BYPASS -3

/*
 * WT_BLKCACHE_ID --
 *    Checksum, offset and size uniquely identify a block.
 *    These are the same items used to compute the cookie.
 */
struct __wt_blkcache_id {
    uint64_t checksum;
    uint64_t offset;
    uint64_t size;
};

/*
 * WT_BLKCACHE_ITEM --
 *     Block cache item. It links with other items in the same hash bucket.
 */
struct __wt_blkcache_item {

    struct __wt_blkcache_id id;
    TAILQ_ENTRY(__wt_blkcache_item) hashq;
    void *data;
};

/*
 * WT_BLKCACHE --
 *     Block cache metadata includes the hashtable of cached items, number
 *     of cached data blocks and the total amount of space they occupy.
 */
struct __wt_blkcache {
    /* Locked: Block manager cache. Locks are per-bucket. */
    TAILQ_HEAD(__wt_blkcache_hash, __wt_blkcache_item) * hash;
    WT_SPINLOCK * hash_locks;

    char *nvram_device_path;
    float fraction_in_dram;
    int refs_since_filesize_estimated;
    int type;
    size_t bytes_used;
    size_t estimated_file_size;
    size_t hash_size;
    size_t num_data_blocks;
    size_t max_bytes;
    size_t system_ram;

#ifdef HAVE_LIBMEMKIND
    struct memkind *pmem_kind;
#endif /* HAVE_LIBMEMKIND */
};

#define BLKCACHE_UNCONFIGURED 0
#define BLKCACHE_DRAM 1
#define BLKCACHE_NVRAM 2

#define BLKCACHE_PERCENT_FILE_IN_DRAM 50
