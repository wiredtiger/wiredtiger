/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WiredTiger's block cache. It is used to cache blocks identical to those
 * that live on disk in a faster storage medium, such as NVRAM.
 */

/*
 * WT_BLKCACHE_ID --
 *     File handle, offset and size uniquely identify a block.
 */
struct __wt_blkcache_id {
    WT_FH *fh;
    wt_off_t offset;
    size_t size;
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
    TAILQ_HEAD(__wt_blkcache_hash, __wt_blkcache_item) hash[WT_HASH_ARRAY_SIZE];
    WT_SPINLOCK hash_locks[WT_HASH_ARRAY_SIZE];

    size_t bytes_used;
    size_t num_data_blocks;
    size_t max_bytes;

    char *nvram_device_path;
    int type;
};

#define BLKCACHE_UNCONFIGURED 0
#define BLKCACHE_DRAM         1
#define BLKCACHE_NVRAM        2

#ifdef HAVE_LIBMEMKIND
#include <memkind.h>
struct memkind *pmem_kind = NULL;

#define NVRAM_ALLOC_DATA(session, size, retp)		 \
    do {				         \
	*retp = memkind_malloc(pmem_kind, size); \
	if (retp == NULL)			 \
	    return __wt_errno();		 \
    } while (0)

#define NVRAM_FREE_DATA(session, ptr) memkind_free(pmem_kind, ptr)

#else

#define DRAM_ALLOC_DATA(session, size, retp) __wt_malloc(session, size, retp)
#define DRAM_FREE_DATA(session, ptr) __wt_free_int(session, ptr)

#endif /* HAVE_LIBMEMKIND */
