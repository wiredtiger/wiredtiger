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

#define WT_CHUNKCACHE_NAMEMAX 50
#define WT_CHUNKCACHE_DRAM 0

struct __wt_chunkcache_hashid {
    char objectname[WT_CHUNKCACHE_NAMEMAX];
    char objectid;
};

/*
 * This data structure contains a list of cached chunks for a given object.
 */
struct __wt_chunkcache_chunk {
    TAILQ_ENTRY(__wt_chunkcache_chunk) next_chunk;

    wt_off_t chunk_offset;
    size_t chunk_size;
    void *chunk_location;
    uint32_t valid;
};

struct __wt_chunkcache_chain {
    TAILQ_ENTRY(__wt_chunkcache_chain) next_link;
    /* File name and object ID uniquely identify local and remote objects. */
    WT_CHUNKCACHE_HASHID hash_id;
    TAILQ_HEAD(__wt_chunklist_head, __wt_chunkcache_chunk) * chunks;
};

/*
 * This data structure represents a bucket in the chunk cache hash table. A bucket may contain
 * several chunk lists, if multiple chunk lists hashed to the same bucket. We call the collection of
 * chunk lists hashing to the same bucket a chunk chain.
 */
struct __wt_chunkcache_bucket {
    /* This queue contains all objects that collided in this hash bucket */
    TAILQ_HEAD(__wt_chunkchain_head, __wt_chunkcache_chain) * chainq;
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
    int hashtable_size;
    int type;
};

/*
 * __wt_chunkcache_complete_read --
 *     The upper layer calls this function once it has completed the read for the chunk. At this
 *     point we mark the chunk as valid. The chunk cannot be accessed before it is set to valid.
 */
inline void
__wt_chunkcache_complete_read(WT_SESSION_IMPL session, WT_CHUNKCACHE_CHUNK *chunk)
{
    /* Atomically mark the chunk as valid */
    (void)__wt_atomic_addv32(&chunk->valid, 1);
}
