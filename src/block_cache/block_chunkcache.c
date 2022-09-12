/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define WT_CHUNKCACHE_NAMEMAX 50

/*
 * __wt_chunkcache_check
 *     Check if the chunk cache already has the data of size 'size' in the given block at the
 *     given offset, and copy it into the supplied buffer if it is. Otherwise, decide if we
 *     want to read and cache a larger chunk of data than what the upper layer asked for.
 */
void
__wt_chunkcache_check(WT_SESSION_IMPL session, WT_BLOCK *block, uint32_t objectid, wt_off_t offset,
                       uint32_t size, uint32_t *chunksize, void **chunk_location,
                       bool *chunkcache_has_data, void *dst)
{
    WT_CHUNKCACHE *chunkcache;
    WT_CHUNKCACHE_BUCKET *bucket;

    struct {
        objectname[WT_CHUNKCACHE_NAMEMAX];
        objectid;
    } hash_id;

    uint64_t hash;

    chunkcache = S2C(session)->chunkcache;
    *chunk_location = NULL
    *chunksize = 0;
    *chunkcache_has_data = false;

    bzero(&hash_id, sizeof(hash_id));
    hash_id.objectid = objectid;
    memcpy(&hashid.objectname, block->name, WT_MIN(strlen(block->name), WT_CHUNKCACHE_NAMEMAX));

    hash =  __wt_hash_city64(hash_id, sizeof(hash_id));
    bucket = chunkcache->hashtable[hash % chunkcache->hashtable_size];
    __wt_spin_lock(session, &chunkcache->bucket_locks[bucket]);
    TAILQ_FOREACH(chunklist, bucket->chainq, chunkchain) {
        if (strncmp(chunklist->name, &hash_id.objectname, strlen(&hash_id.objectname)) == 0
            && hash_id.objectid == chunklist.objectid) {
            /* Found the list of chunks corresponding to the given object. See if we have the needed chunk. */
            TAILQ_FOREACH(chunk, chunklist->chunks, chunkn) {
                if (chunk->chunk_offset <= offset && (chunk->chunk_offset + chunk->chunk_size) >= (offset + size)) {
                    memcpy(dst, chunk->chunk_location[offset-chunk->chunk_offset], size);
                    *chunk_has_data = true;
                }
            }
        }
    }
}
