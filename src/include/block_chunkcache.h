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


/*
 * This data structure contains a list of cached chunks for a given object.
 */
struct __wt_chunkcache_chunk {
    TAILQ_ENTRY(__wt_chunkcache_chunk) chunk;

    wt_off_t chunk_offset;
    size_t chunk_size;
    void *chunk_location;
};

struct __wt_chunkcache_chain {
    /* File name and object ID uniquely identify local and remote objects. */
    const char *name;
    uint32_t objectid;
    TAILQ_HEAD(__wt_chunklist_head, __wt_chunkcache_chunk) *chunks;
};

/*
 * This data structure represents a bucket in the chunk cache hash table.
 * A bucket may contain several chunk lists, if multiple chunk lists hashed
 * to the same bucket. We call the collection of chunk lists hashing to the
 * same bucket a chunk chain.
 */
struct __wt_chunkcache_bucket {
    /* This queue contains all objects that collided in this hash bucket */
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
    WT_CHUNKCACHE_BUCKET * hashtable;
    WT_SPINLOCK *chunkcache_locks;
    int hashtable_size;
};

