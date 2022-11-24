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

#define WT_CHUNKCACHE_DEFAULT_HASHSIZE 32
#define WT_CHUNKCACHE_DEFAULT_CHUNKSIZE 1024*1024
#define WT_CHUNKCACHE_DRAM 0
#define WT_CHUNKCACHE_MINHASHSIZE 1
#define WT_CHUNKCACHE_MAXHASHSIZE 1024
#define WT_CHUNKCACHE_NAMEMAX 50
#define WT_CHUNKCACHE_SSD 1
#define WT_CHUNKCACHE_UNCONFIGURED 0

struct __wt_chunkcache_hashid {
    char objectname[WT_CHUNKCACHE_NAMEMAX];
    uint32_t objectid;
};

/*
 * This data structure contains a list of cached chunks for a given object.
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
    bool chunk_in_eviction;
};

/*
 * A list of chunks for a given object. This list is part of a queue of object
 * chains that collided in the same bucket.
 */
struct __wt_chunkcache_chain {
    TAILQ_ENTRY(__wt_chunkcache_chain) next_link;
    /* File name and object ID uniquely identify local and remote objects. */
    TAILQ_HEAD(__wt_chunklist_head, __wt_chunkcache_chunk) chunks;
    WT_CHUNKCACHE_HASHID hash_id;
};

/*
 * This data structure represents a bucket in the chunk cache hash table. A bucket may contain
 * several chunk lists, if multiple chunk lists hashed to the same bucket. We call the collection of
 * chunk lists hashing to the same bucket a chunk chain.
 */
struct __wt_chunkcache_bucket {
    /* This queue contains queues for all objects that collided in this hash bucket */
    TAILQ_HEAD(__wt_chunkchain_head, __wt_chunkcache_chain) chainq;
};

/*
 * WT_CHUNKCACHE --
 *     The chunkcache is a hashtable of chunk lists. Each chunk list
 *     is uniquely identified by the file name and the object id.
 *     Lists of chunks are sorted by offset.
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
    bool configured;
    size_t default_chunk_size;
    char *dir_path;   /* The directory to use if we are on a file system */
    uint hashtable_size;
    int type;
    uint64_t bytes_used; /* Amount of data currently in cache */
};

