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




struct __wt_chunkcache_chunk {
    TAILQ_ENTRY(__wt_chunkcache_chunk) chunklist;

    wt_off_t chunk_offset;
    size_t chunk_size;
    void *chunk_location;
};


struct __wt_chunkcache_bucket {
    /* This queue contains all objects that collided in this hash bucket */
    TAILQ_ENTRY(__wt_chunkcache_bucket) hashq;

    /* File name and object ID uniquely identify local and remote objects. */
    const char *name;
    uint32_t objectid;
    TAILQ_HEAD(__wt_chunkcache_list, __wt_chunkcache_chunk) chunklist_head;
}

/*
 * WT_CHUNKCACHE --
 *     The chunkcache is a hashtable of chunk lists. Each chunk list
 *     is uniquely identified by the file name and the object id.
 *     Lists of chunks are sorted by offset.
 */
struct __wt_chunkcache {
    /* Locked: Hashtable of cached objects. Locks are per object. */
    TAILQ_HEAD(__wt_chunkcache_hash, __wt_chunkcache_bucket) * hash;
    WT_SPINLOCK *chunkcache_locks;
};

