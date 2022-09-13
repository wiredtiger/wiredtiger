/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"


/*
 * __blkcache_alloc --
 *     Allocate a block of memory in the cache.
 */
static int
__chunkcache_alloc(WT_SESSION_IMPL *session, WT_CHUNKCACHE_CHUNK *chunk)
{
    WT_CHUNKCACHE *chunkcache;

    chunkcache = &S2C(session)->chunkcache;

    if (chunkcache->type == CHUNKCACHE_DRAM)
        return (__wt_malloc(session, size, retp));
    else {
#ifdef ENABLE_MEMKIND
        chunk->chunk_location = memkind_malloc(chunkcache->mem_kind, chunk->chunk_size);
        if (chunk->chunk_location == NULL)
            return WT_ERROR;
#else
        WT_RET_MSG(session, EINVAL, "Chunk cache requires libmemkind, unless it is configured to be in DRAM");
#endif
    }
    return (0);
}

static int
__chunkcache_alloc_chunk(WT_SESSION_IMPL *session, WT_CHUNKCACHE_CHUNK **chunk,  wt_off_t offset, uint32_t size)
{
    WT_CHUNKCACHE_CHUNK *newchunk;

    *chunk = NULL;

    if (__wt_calloc_one(session, WT_CHUNKCACHE_CHUNK, &newchunk) != 0)
        return WT_ERROR;

    newchunk->chunk_size = size;
    newchunk->chunk_offset = offset;
    if (__chunkcache_alloc(session, newchunk) != 0) {
        __wt_free(session, newchunk);
        return WT_ERROR;
    }

    *chunk = newchunk;
    return (0);
}

static uint32_t
__chunkcache_can_admit(void) {
    return WT_CHUNK_SIZE;
}

/*
 * __wt_chunkcache_check --
 *     Check if the chunk cache already has the data of size 'size' in the given block at the
 *     given offset, and copy it into the supplied buffer if it is. Otherwise, decide if we
 *     want to read and cache a larger chunk of data than what the upper layer asked for.
 */
void
__wt_chunkcache_check(WT_SESSION_IMPL session, WT_BLOCK *block, uint32_t objectid, wt_off_t offset,
                      uint32_t size, WT_CHUNKCACHE_CHUNK **chunk, bool *chunkcache_has_data, void *dst)
{
    WT_CHUNKCACHE *chunkcache;
    WT_CHUNKCACHE_BUCKET *bucket;
    WT_CHUNKCACHE_CHAIN *newchain;
    WT_CHUNKCACHE_CHUNK *newchunk;
    WT_CHUNKCACHE_HASHID hash_id;
    uint64_t hash;

    chunkcache = S2C(session)->chunkcache;
    *chunk = NULL;
    *chunkcache_has_data = false;
    lastchunk = NULL;

    bzero(&hash_id, sizeof(hash_id));
    hash_id.objectid = objectid;
    memcpy(&hash_id.objectname, block->name, WT_MIN(strlen(block->name), WT_CHUNKCACHE_NAMEMAX));

    hash =  __wt_hash_city64(hash_id, sizeof(hash_id));
    bucket = chunkcache->hashtable[hash % chunkcache->hashtable_size];

    __wt_spin_lock(session, &chunkcache->bucket_locks[bucket]);
    TAILQ_FOREACH(chunkchain, bucket->chainq, next_link) {
        if (memcmp(&chunkchain->hash_id, &hash_id, sizeof(hash_id) == 0)) {
            /* Found the list of chunks corresponding to the given object. See if we have the needed chunk. */
            TAILQ_FOREACH(chunk, chunkchain->chunks, next_chunk) {
                if (chunk->chunk_offset <= offset && (chunk->chunk_offset + chunk->chunk_size) >= (offset + size)) {
                    memcpy(dst, chunk->chunk_location[offset-chunk->chunk_offset], size);
                    *chunk_has_data = true;
                    /* Increment hit statistics. XXX */
                    goto done;
                }
                else if (chunk->chunk_offset > offset) {
                    break;
                }
            }
            /*
             * The chunk list is present, but the chunk is not there.
             * Do we want to allocate space for it and insert it?
             */
            if (__chunkcache_alloc_chunk(session, &newchunk, offset, size) == 0) {
                if (chunk == NULL)
                    TAILQ_INSERT_HEAD(chunkchain->chunks, newchunk, next_chunk);
                else if (chunk->chunk_offset > newchunk->chunk_offset)
                    TAILQ_INSERT_BEFORE(chunk, newchunk, next_chunk);
                else
                    TAILQ_INSERT_AFTER(chunkchain->chunks, chunk, newchunk, next_chunk);

                /* Setting this pointer tells the block manager to read data for this chunk. */
                *chunk = newchunk;

                /* Increment allocation stats. XXX */
                goto done;
            }
        }
    }
    /*
     * The chunk list for this file and object id is not present.
     * Do we want to allocate it?
     */
    if (__chunkcache_alloc_chunk(session, &newchunk, offset, size) == 0) {
        if (__wt_calloc_one(session, WT_CHUNKCACHE_CHAIN, &newchain) != 0) {
            __chunkcache_free_chunk(session, newchunk);
            goto done;
        }
        newchain.hash_id = hash_id;
        TAILQ_INSERT_HEAD(bucket->chainq, newchain, chainq);

        /* Insert the new chunk. */
        TAILQ_INSERT_HEAD(newchain->chunks, newchunk, chunks);
        /* Increment allocation stats. XXX */
    }
  done:
    __wt_spin_unlock(session, &chunkcache->bucket_locks[bucket]);
}



