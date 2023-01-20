/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WiredTiger's chunk cache. Locally caches chunks of remote objects.
 */

#define WT_CHUNKCACHE_DEFAULT_HASHSIZE 32*1024
#define WT_CHUNKCACHE_DEFAULT_CHUNKSIZE 1024*1024
#define WT_CHUNKCACHE_DRAM 0
#define WT_CHUNKCACHE_MINHASHSIZE 64
#define WT_CHUNKCACHE_MAXHASHSIZE 1024 * 1024
#define WT_CHUNKCACHE_NAMEMAX 50
#define WT_CHUNKCACHE_SSD 1
#define WT_CHUNKCACHE_UNCONFIGURED 0

struct __wt_chunkcache_hashid {
    char objectname[WT_CHUNKCACHE_NAMEMAX];
    uint32_t objectid;
    wt_off_t offset;
};

/*
 * The encapsulation of a cached chunk.
 */
struct __wt_chunkcache_chunk {
    TAILQ_ENTRY(__wt_chunkcache_chunk) next_chunk;
    TAILQ_ENTRY(__wt_chunkcache_chunk) next_lru_item;

    struct __wt_chunklist_head *my_queuehead_ptr;
    wt_off_t chunk_offset;
    size_t chunk_size;
    char *chunk_location;
    uint32_t valid;
    uint bucket_id;  /* Lets us find the corresponding bucket for quick removal */
    bool being_evicted;
    WT_CHUNKCACHE_HASHID hash_id;
};

struct __wt_chunkcache_bucket {
    /* This queue contains all chunks that mapped to this bucket */
    TAILQ_HEAD(__wt_chunkchain_head, __wt_chunkcache_chain) colliding_chunks;
};

/*
 * WT_CHUNKCACHE --
 *     The chunkcache is a hashtable of chunks. Each chunk list
 *     is uniquely identified by the file name, object id and offset.
 *     If more than one chunk maps to the same hash bucket, the colliding
 *     chunks are placed into a linked list. There is a per-bucket spinlock.
 */
struct __wt_chunkcache {
    /* Hashtable buckets. Locks are per bucket. */
    WT_CHUNKCACHE_BUCKET *hashtable;
    WT_SPINLOCK *bucket_locks;
    WT_SPINLOCK chunkcache_lru_lock; /* Locks the LRU queue */
    TAILQ_HEAD(__wt_chunkcache_lru, __wt_chunkcache_chunk) chunkcache_lru_list;
#ifdef ENABLE_MEMKIND
    struct memkind *memkind; /* Lets us use jemalloc over a file */
#endif
    uint64_t capacity;
    bool chunkcache_exiting;
    bool configured;
    size_t chunk_size;
    char *dev_path;   /* The storage path to use if we are on a file system or a block device */
    uint hashtable_size;
    int type;
    uint64_t bytes_used; /* Amount of data currently in cache */
};

#define BLOCK_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) \
(chunk_off <= block_off && (chunk_off + (wt_off_t)chunk_size) >= (block_off + (wt_off_t)block_size))

#define BLOCK_BEGINS_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) \
    ((block_off > chunk_off) && (block_off < (chunk_off + (wt_off_t)chunk_size)) && ((block_off + (wt_off_t)block_size) > (chunk_off + (wt_off_t)chunk_size)))

#define BLOCK_ENDS_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) \
    ((block_off < chunk_off) && (block_off + (wt_off_t)block_size) < (chunk_off + (wt_off_t)chunk_size)))

#define BLOCK_MIDDLE_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) \
    ((block_off < chunk_off) &&  (block_off + (wt_off_t)block_size) > (chunk_off + (wt_off_t)chunk_size))

#define BLOCK_PART_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) \
    BLOCK_BEGINS_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) || \
        BLOCK_ENDS_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) || \
        BLOCK_MIDDLE_IN_CHUNK)chunk_off, block_off, chunk_size, block_size)

#define CHUNK_MARK_VALID(session, chunkcache, chunk)                    \
    if (!chunk->valid) {                                                \
        (void)__wt_atomic_addv32(&chunk->valid, 1);                     \
        __wt_spin_lock(session, &chunkcache->chunkcache_lru_lock);      \
        TAILQ_INSERT_HEAD(&chunkcache->chunkcache_lru_list, chunk, next_lru_item); \
        __wt_spin_unlock(session, &chunkcache->chunkcache_lru_lock);    \
    }

#define CHUNK_OFFSET(chunkcache, offset) \
    (wt_off_t)((size_t)offset / chunkcache->chunk_size) * chunkcache->chunk_size
