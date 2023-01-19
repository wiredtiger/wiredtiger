/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __chunkcache_alloc_space --
 *     Allocate memory for the chunk in the cache.
 */
static int
__chunkcache_alloc_space(WT_SESSION_IMPL *session, WT_CHUNKCACHE_CHUNK *chunk)
{
    WT_CHUNKCACHE *chunkcache;
    WT_DECL_RET;

    chunkcache = &S2C(session)->chunkcache;

    if (chunkcache->type == WT_CHUNKCACHE_DRAM)
        ret = __wt_malloc(session, chunk->chunk_size, &chunk->chunk_location);
    else {
#ifdef ENABLE_MEMKIND
        chunk->chunk_location = memkind_malloc(chunkcache->mem_kind, chunk->chunk_size);
        if (chunk->chunk_location == NULL)
            ret = WT_ERROR;
#else
        WT_RET_MSG(session, EINVAL,
          "Chunk cache requires libmemkind, unless it is configured to be in DRAM");
#endif
    }
    if (ret == 0) {
        __wt_atomic_add64(&chunkcache->bytes_used, chunk->chunk_size);
        WT_STAT_CONN_INCRV(session, chunk_cache_bytes, chunk->chunk_size);
    }
    return (ret);
}

/*
 * __chunkcache_admit_size --
 *     Decide if we can admit the chunk given the limit on cache capacity and return the
 *     size of the chunk to be admitted.
 */
static size_t
__chunkcache_admit_size(WT_SESSION_IMPL *session)
{
    WT_CHUNKCACHE *chunkcache;

    chunkcache = &S2C(session)->chunkcache;

    if ((chunkcache->bytes_used + chunkcache->default_chunk_size) < chunkcache->capacity)
        return (chunkcache->default_chunk_size);

    WT_STAT_CONN_INCR(session, chunk_cache_exceeded_capacity);
    __wt_verbose(session, WT_VERB_CHUNKCACHE,
                 "exceeded chunkcache capacity of %" PRIu64 " bytes", chunkcache->capacity);
    return 0;
}

/*
 * __chunkcache_alloc_chunk --
 *     Allocate the metadata for the chunk and call the function that will allocate its cache space.
 */
static int
__chunkcache_alloc_chunk(
    WT_SESSION_IMPL *session, WT_CHUNKCACHE_CHUNK **chunk, wt_off_t offset, WT_BLOCK *block,
    struct __wt_chunklist_head *qptr, uint bucket_id)
{
    WT_CHUNKCACHE *chunkcache;
    WT_CHUNKCACHE_CHUNK *newchunk;
    size_t chunk_size;

    *chunk = NULL;
    chunkcache = &S2C(session)->chunkcache;

    WT_ASSERT(session, offset > 0);

    /*
     * Calculate the size and the offset for the chunk. The chunk storage area is broken into
     * equally sized chunks of configured size. We calculate the offset of the
     * chunk into which the block's offset falls. Chunks are equally sized and are
     * not necessarily a multiple of a block. So a block may begin in one chunk and
     * and in another. It may also span multiple chunks, if the chunk size is configured
     * much smaller than a block size (we hope that never happens). In the allocation
     * function we don't care about the block's size. If more than one chunk is needed
     * to cover the entire block, another function will take care of allocating multiple
     * chunks.
     */

    if ((chunk_size = __chunkcache_admit_size(session)) == 0)
        return (WT_ERROR);
    if (__wt_calloc(session, 1, sizeof(WT_CHUNKCACHE_CHUNK), &newchunk) != 0)
        return (WT_ERROR);

    /* Chunk cannot be larger than the file */
    newchunk->chunk_size = WT_MIN(chunk_size, (size_t)block->size);
    newchunk->chunk_offset = (wt_off_t)(((size_t)offset / chunkcache->default_chunk_size) *
                                        chunkcache->default_chunk_size);

    newchunk->my_queuehead_ptr = qptr;
    newchunk->bucket_id = bucket_id;

    printf("offset-convert: from %" PRIu64 " to %" PRIu64 "\n", offset, newchunk->chunk_offset);
    printf("chunk size = %ld\n",  newchunk->chunk_size);

    if (__chunkcache_alloc_space(session, newchunk) != 0) {
        __wt_free(session, newchunk);
        return (WT_ERROR);
    }

    WT_STAT_CONN_INCR(session, chunk_cache_chunks);
    *chunk = newchunk;
    return (0);
}

/*
 * __chunkcache_free_chunk --
 *     Free the memory occupied by the chunk.
 */
static void
__chunkcache_free_chunk(WT_SESSION_IMPL *session, WT_CHUNKCACHE_CHUNK *chunk)
{
    WT_CHUNKCACHE *chunkcache;

    chunkcache = &S2C(session)->chunkcache;

    (void)__wt_atomic_sub64(&chunkcache->bytes_used, chunk->chunk_size);
    WT_STAT_CONN_DECRV(session, chunk_cache_bytes, chunk->chunk_size);

    if (chunkcache->type == WT_CHUNKCACHE_DRAM)
        __wt_free(session, chunk->chunk_location);
    else {
#ifdef ENABLE_MEMKIND
        memkind_free(chunkcache->pmem_kind, chunk->chunk_location);
#else
        __wt_err(session, EINVAL,
                 "Chunk cache requires libmemkind, unless it is configured to be in DRAM");
#endif
    }
    __wt_free(session, chunk);
     WT_STAT_CONN_DECR(session, chunk_cache_chunks);
}

/*
 * __chunkcache_remove_chunk --
 *     Remove the chunk from its chunk chain. We have a separate function to free
 *     the underlying cache space, because other code may remove chunks
 *     without freeing them, letting the thread doing eviction to free the chunk.
 */
static void
__chunkcache_remove_chunk(WT_SESSION_IMPL *session, WT_CHUNKCACHE_CHUNK *chunk)
{
    WT_CHUNKCACHE *chunkcache;

    chunkcache = &S2C(session)->chunkcache;

    __wt_spin_lock(session, &chunkcache->bucket_locks[chunk->bucket_id]);
    WT_ASSERT(session, TAILQ_NEXT(chunk, next_chunk) != NULL || chunk->next_chunk.tqe_prev != NULL);
    TAILQ_REMOVE(chunk->my_queuehead_ptr, chunk, next_chunk);
    __wt_spin_unlock(session, &chunkcache->bucket_locks[chunk->bucket_id]);

}

/*
 * __chunkcache_evict_one --
 *     Evict a single chunk for the chunk cache.
 */
static bool
__chunkcache_evict_one(WT_SESSION_IMPL *session)
{
    WT_CHUNKCACHE *chunkcache;
    WT_CHUNKCACHE_CHUNK *chunk_to_evict;

    chunkcache = &S2C(session)->chunkcache;
    chunk_to_evict = NULL;

    /*
     * We must remove the evicted chunk from the LRU list and from its chunk chain.
     * We must lock the chunk chain before we lock the LRU list. But to find the
     * queue where the to-be-evicted chunk lives, we must look inside the LRU list.
     * We resolve this circularity as follows:
     *
     * 1. With the LRU list lock held, we remove the chunk at the list's tail and mark
     *    that chunk as being evicted.
     *    That prevents the code responsible for removing outdated chunks from freeing
     *    the chunk before we do.
     * 2. We remove the chunk from its chunk's chain, acquiring appropriate locks.
     * 3. We free the chunk.
     */
    __wt_spin_lock(session, &chunkcache->chunkcache_lru_lock);
    chunk_to_evict = TAILQ_LAST(&chunkcache->chunkcache_lru_list, __wt_chunkcache_lru);
    if (chunk_to_evict != NULL && chunk_to_evict->valid)
        TAILQ_REMOVE(&chunkcache->chunkcache_lru_list, chunk_to_evict, next_lru_item);
    chunk_to_evict->being_evicted = true;
    __wt_spin_unlock(session, &chunkcache->chunkcache_lru_lock);

    if (chunk_to_evict == NULL || !chunk_to_evict->valid)
        return false;

    printf("\nevict: offset=%" PRId64 ", size=%ld\n",
           chunk_to_evict->chunk_offset, chunk_to_evict->chunk_size);
    __chunkcache_remove_chunk(session, chunk_to_evict);
    __chunkcache_free_chunk(session, chunk_to_evict);
    /* Free the metadata */
    WT_STAT_CONN_INCR(session, chunk_cache_chunks_evicted);

    return true;
}

/*
 * __chunkcache_eviction_thread --
 *     Periodically sweep the cache and evict chunks at the end of the LRU list.
 */
static WT_THREAD_RET
__chunkcache_eviction_thread(void *arg)
{
    WT_CHUNKCACHE *chunkcache;
    WT_SESSION_IMPL *session;

    session = (WT_SESSION_IMPL *)arg;
    chunkcache = &S2C(session)->chunkcache;

    while (!chunkcache->chunkcache_exiting) {
        /* Try evicting a chunk if we have exceeded capacity */
        if ((chunkcache->bytes_used + chunkcache->default_chunk_size) > chunkcache->capacity)
            __chunkcache_evict_one(session);
        __wt_sleep(1, 0); /* TODO: choose a more appropriate frequency */
    }
    return (WT_THREAD_RET_VALUE);
}

/*
 * __chunkcache_remove_lru --
 *     Remove the chunk from the LRU list.
 */
static void
__chunkcache_remove_lru(WT_SESSION_IMPL *session, WT_CHUNKCACHE_CHUNK *chunk)
{
    WT_CHUNKCACHE *chunkcache;

    chunkcache = &S2C(session)->chunkcache;

    /* Lock the LRU list and remove the chunk from it.
     * Between the time we decided that the chunk must be removed
     * and the time that we try to remove it, another thread might
     * have removed it, so we check that the chunk is still part
     * of the list before removing it.
     */
     __wt_spin_lock(session, &chunkcache->chunkcache_lru_lock);
     if (TAILQ_NEXT(chunk, next_lru_item) != NULL ||
         chunk->next_lru_item.tqe_prev != NULL) {
         TAILQ_REMOVE(&chunkcache->chunkcache_lru_list, chunk, next_lru_item);
         printf("Removed from LRU list: offset=%" PRIu64 ", size=%ld\n",
                chunk->chunk_offset, chunk->chunk_size);
    }
    __wt_spin_unlock(session, &chunkcache->chunkcache_lru_lock);
}

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


/*
 * __wt_chunkcache_check --
 *     Check if the chunk cache already has the data of size 'size' in the given block at the given
 *     offset, and copy it into the supplied buffer if it is. Otherwise, decide if we want to read
 *     and cache a larger chunk of data than what the upper layer asked for.
 */
void
__wt_chunkcache_check(WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t objectid, wt_off_t offset,
  uint32_t size, WT_CHUNKCACHE_CHUNK **chunk_to_read, bool *chunkcache_has_data, void *dst)
{
    WT_CHUNKCACHE *chunkcache;
    WT_CHUNKCACHE_BUCKET *bucket;
    WT_CHUNKCACHE_CHAIN *chunkchain, *newchain;
    WT_CHUNKCACHE_CHUNK *chunk, *newchunk;
    WT_CHUNKCACHE_HASHID hash_id;
    uint bucket_id;
    uint64_t hash;

    chunkcache = &S2C(session)->chunkcache;
    *chunk_to_read = NULL;
    *chunkcache_has_data = false;

    if (!chunkcache->configured)
        return;

    WT_STAT_CONN_INCR(session, chunk_cache_lookups);
    bzero(&hash_id, sizeof(hash_id));
    hash_id.objectid = objectid;
    memcpy(&hash_id.objectname, block->name, WT_MIN(strlen(block->name), WT_CHUNKCACHE_NAMEMAX));

    hash = __wt_hash_city64((void*) &hash_id, sizeof(hash_id));
    bucket_id = (uint)(hash % chunkcache->hashtable_size);
    bucket = &chunkcache->hashtable[bucket_id];

    __wt_spin_lock(session, &chunkcache->bucket_locks[bucket_id]);
    printf("\ncheck: %s(%d), offset=%" PRId64 ", size=%d\n",
      (char*)&hash_id.objectname, hash_id.objectid, offset, size);

    TAILQ_FOREACH(chunkchain, &bucket->chainq, next_link) {
        if (memcmp(&chunkchain->hash_id, &hash_id, sizeof(hash_id)) == 0) {
            /*
             * Found the chain of chunks for the object. See if we have the needed chunk.
             * With the current chunk allocation policy, a chunk always
             * begins on a block boundary, so if the current block is only partially in
             * a chunk, we can't have the remaining part of that block in the next chunk.
             */
            TAILQ_FOREACH(chunk, &chunkchain->chunks, next_chunk) {
                if (chunk->valid &&
                    BLOCK_IN_CHUNK(chunk->chunk_offset, offset, chunk->chunk_size, size)) {
                    *chunkcache_has_data = true;
                    memcpy(dst, chunk->chunk_location + (offset - chunk->chunk_offset), size);
                    WT_STAT_CONN_INCR(session, chunk_cache_hits);

                    printf("found: offset=%" PRIu64 ", size=%ld\n", chunk->chunk_offset,
                           chunk->chunk_size);
                    /*
                     * Move to the head of the LRU list if we find the chunk in that list.
                     * If the chunk is being evicted we let it be evicted.
                     */
                    __chunkcache_remove_lru(session, chunk);
                    if (!chunk->being_evicted) {
                        __wt_spin_lock(session, &chunkcache->chunkcache_lru_lock);
                        TAILQ_INSERT_HEAD(&chunkcache->chunkcache_lru_list, chunk, next_lru_item);
                        __wt_spin_unlock(session, &chunkcache->chunkcache_lru_lock);
                    } /* XXX. Address the case when the block begins in chunk. */
                    goto done;
                } else if (chunk->chunk_offset > offset)
                    break;
            }
            /*
             * The chunk list is present, but the chunk is not there. Do we want to allocate space
             * for it and insert it? By default the chunk size is calculated to be the minimum
             * of what the size that the chunk cache can admit, but we reduce the chunk size
             * if the default would cause us to read past the end of the file.
             */
            if (__chunkcache_alloc_chunk(session, &newchunk, offset, size, block,
                                         &chunkchain->chunks, bucket_id) == 0) {
                printf("allocate: %s(%d), offset=%" PRId64 ", size=%ld\n",
                       (char*)&hash_id.objectname, hash_id.objectid, newchunk->chunk_offset,
                       newchunk->chunk_size);
                if (chunk == NULL) { /* chunk chain is empty */
                    TAILQ_INSERT_HEAD(&chunkchain->chunks, newchunk, next_chunk);
                    printf("insert-first: %s(%d), offset=0, size=0\n",
                           (char*)&hash_id.objectname, hash_id.objectid);
                }
                else if (chunk->chunk_offset > newchunk->chunk_offset) {
                    TAILQ_INSERT_BEFORE(chunk, newchunk, next_chunk);
                    printf("insert-before: %s(%d), offset=%" PRId64 ", size=%ld\n",
                           (char*)&hash_id.objectname, hash_id.objectid,
                           chunk->chunk_offset, chunk->chunk_size);
                }
                else {
                    TAILQ_INSERT_AFTER(&chunkchain->chunks, chunk, newchunk, next_chunk);
                    printf("insert-after:  %s(%d), offset=%" PRId64 ", size=%ld\n",
                           (char*)&hash_id.objectname, hash_id.objectid,
                           chunk->chunk_offset, chunk->chunk_size);
                }
                /* Set the pointer so that the block manager reads data for this chunk. */
                *chunk_to_read = newchunk;
                goto done;
            }
        }
    }
    /*
     * The chunk list for this file and object id is not present. Do we want to allocate it?
     */
    if (__wt_calloc(session, 1, sizeof(WT_CHUNKCACHE_CHAIN), &newchain) == 0) {
        newchain->hash_id = hash_id;
        TAILQ_INSERT_HEAD(&bucket->chainq, newchain, next_link);

        /* Insert the new chunk. */
        TAILQ_INIT(&(newchain->chunks));

        if (__chunkcache_alloc_chunk(session, &newchunk, offset, size, block,
                                     &(newchain->chunks), bucket_id) != 0)
            goto done;

        TAILQ_INSERT_HEAD(&newchain->chunks, newchunk, next_chunk);
        *chunk_to_read = newchunk;
        printf("allocate: %s(%d), offset=%" PRId64 ", size=%ld, file_size=%" PRIu64 "\n",
               (char*)&hash_id.objectname,
               hash_id.objectid, newchunk->chunk_offset, newchunk->chunk_size, block->size);
        printf("insert-first: %s(%d), offset=0, size=0\n",
                           (char*)&hash_id.objectname, hash_id.objectid);
    }
done:
    __wt_spin_unlock(session, &chunkcache->bucket_locks[bucket_id]);
}

/*
 * __wt_chunkcache_complete_read --
 *     The upper layer calls this function once it has completed the read for the chunk. At this
 *     point we mark the chunk as valid and copy the needed part of the chunk to the buffer
 *     provided by the caller. The chunk cannot be accessed before it is set to valid.
 */
void
__wt_chunkcache_complete_read(WT_SESSION_IMPL *session, WT_BLOCK *block, WT_CHUNKCACHE_CHUNK *chunk,
                              wt_off_t offset,
                              uint32_t size, void *dst, bool *chunkcache_has_data)
{
    WT_CHUNKCACHE *chunkcache;
    WT_CHUNK *newchunk, *next_chunk, *prev_chunk;
    wt_off_t left_to_read, already_read;

    chunkcache = &S2C(session)->chunkcache;
    *chunkcache_has_data = false;

    WT_ASSERT(session, offset > 0);

    printf("complete-read: chunk->chunk_offset=%" PRIu64 ", offset=%" PRIu64 ", \
           chunk->chunk_size=%" PRIu64 ", size=%" PRIu64 "\n",
           chunk->chunk_offset, offset, (uint64_t)chunk->chunk_size, (uint64_t)size);

    /* Easy case: the entire block is in the chunk */
    if (BLOCK_IN_CHUNK) {
        memcpy(dst, chunk->chunk_location + (offset - chunk->chunk_offset), size);
        *chunkcache_has_data = true;

        /* Atomically mark the chunk as valid */
        (void)__wt_atomic_addv32(&chunk->valid, 1);

        /* Insert it at the head of the LRU list */
        __wt_spin_lock(session, &chunkcache->chunkcache_lru_lock);
        TAILQ_INSERT_HEAD(&chunkcache->chunkcache_lru_list, chunk, next_lru_item);
        __wt_spin_unlock(session, &chunkcache->chunkcache_lru_lock);
    } else {
        /*
         * Complicated case: the block begins in the chunk we have read, but
         * ends in the next chunk or may even span several chunks.
         * We need to see if those chunks have the data we need, and if not,
         * read it. We do not mark the current chunk as valid until we are done
         * with it. An invalid chunk would cannot be removed from the chunk chain,
         * and we use this invariant to shorten the time when we hold locks.
         */
        WT_ASSERT(session,
                  BLOCK_BEGINS_IN_CHUNK(chunk->chunk_offset, offset, chunk->chunk_size, size));
        WT_STAT; /* XXX */

        /* Copy the part of the block that we already have */
        left_to_read = (offset + size) - (chunk->chunk_offset + chunk->chunk_size);
        already_read = chunk->chunk_offset + chunk->chunk_size - offset;
        WT_ASSERT(session, (already_read + left_to_read) == size);
        memcpy(dst, chunk->chunk_location + (offset - chunk->chunk_offset), already_read);

        /* Now read the other chunk (or chunks) containing the remainder of the block. */
        prev_chunk = chunk;
        while (left_to_read > 0) {
            /* See if the next chunk is already cached */
            __wt_spin_lock(session, &chunkcache->bucket_locks[chunk->bucket_id]);
            if ((next_chunk = (WT_CHUNKCACHE_CHUNK *)TAILQ_NEXT(chunk, next_chunk)) != NULL &&
                next_chunk->chunk_offset == (chunk->chunk_offset + (wt_off_t)chunk->chunk_size))
                newchunk = next_chunk;
            else {
                /* Don't hold the lock while we are allocating the new chunk and doing I/O */
                __wt_spin_unlock(session, &chunkcache->bucket_locks[newchunk->bucket_id]);

                /* Allocate the new chunk */
                newchunk_offset = prev_chunk->chunk_offset + prev_chunk->chunk_size;
                WT_RET(__chunkcache_alloc_chunk(session, &newchunk, newchunk_offset, block,
                                                prev_chunk->my_queuehead_ptr, prev_chunk->bucket_id));
                /* Read the data into it */
                if (__wt_read(session, block->fh, newchunk->chunk_offset, newchunk->chunk_size,
                              newchunk->chunk_location) != 0) {
                    __chunkcache_free_chunk(newchunk);
                    return WT_ERR;
                }
                /* Insert it into the chunk chain */
                __wt_spin_lock(session, &chunkcache->bucket_locks[newchunk->bucket_id]);
                TAILQ_INSERT_AFTER(newchunk->my_queuehead_ptr, prev_chunk, newchunk, next_chunk);
                __wt_spin_unlock(session, &chunkcache->bucket_locks[newchunk->bucket_id]);

                /* Insert it into the LRU list */
                __wt_spin_lock(session, &chunkcache->chunkcache_lru_lock);
                TAILQ_INSERT_HEAD(&chunkcache->chunkcache_lru_list, newchunk, next_lru_item);
                __wt_spin_unlock(session, &chunkcache->chunkcache_lru_lock);
            }
            /* Copy the data that we just read into the destination buffer */
            memcpy(dst + already_read, newchunk->chunk_location,
                   WT_MIN(left_to_read, newchunk->chunk_size));

            if (!prev_chunk->valid)
                (void)__wt_atomic_addv32(&prev_chunk->valid, 1);
            prev_chunk = newchunk;
            already_read += WT_MIN(left_to_read, newchunk->chunk_size);
            left_to_read -= newchunk->chunk_size;
        }
        *chunkcache_has_data = true;
    }
}

/*
 * __wt_chunkcache_remove
 *     Remove the chunk containing an outdated block.
 */
void
__wt_chunkcache_remove(WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t objectid,
                       wt_off_t offset, uint32_t size)
{
    WT_CHUNKCACHE *chunkcache;
    WT_CHUNKCACHE_BUCKET *bucket;
    WT_CHUNKCACHE_CHAIN *chunkchain;
    WT_CHUNKCACHE_CHUNK *chunk;
    WT_CHUNKCACHE_HASHID hash_id;
    bool being_evicted;
    uint bucket_id;
    uint64_t hash;

    being_evicted = false;
    chunkcache = &S2C(session)->chunkcache;
    chunk = NULL;

    if (!chunkcache->configured)
        return;

    bzero(&hash_id, sizeof(hash_id));
    hash_id.objectid = objectid;
    memcpy(&hash_id.objectname, block->name, WT_MIN(strlen(block->name), WT_CHUNKCACHE_NAMEMAX));

    hash = __wt_hash_city64((void*) &hash_id, sizeof(hash_id));
    bucket_id = (uint)(hash % chunkcache->hashtable_size);
    bucket = &chunkcache->hashtable[bucket_id];

    __wt_spin_lock(session, &chunkcache->bucket_locks[bucket_id]);
    printf("\nremove-check: %s(%d), offset=%" PRId64 ", size=%d\n",
           (char*)&hash_id.objectname, hash_id.objectid, offset, size);

    TAILQ_FOREACH(chunkchain, &bucket->chainq, next_link) {
        if (memcmp(&chunkchain->hash_id, &hash_id, sizeof(hash_id)) == 0)
            /* Found the chain of chunks for the object. See if we have the needed chunk. */
            TAILQ_FOREACH(chunk, &chunkchain->chunks, next_chunk) {
                /* The block cannot be in this or any subsequent chunks */
                if (offset > (chunk->chunk_offset + chunk->chunk_size))
                    goto done;

                if (chunk->valid &&
                    (BLOCK_PART_IN_CHUNK(chunk->chunk_offset, offset, chunk->chunk_size, size))) {
                    TAILQ_REMOVE(&chunkchain->chunks, chunk, next_chunk);
                    /*
                     * If the chunk is being evicted, the eviction code would have
                     * removed it from the LRU list and will free it for us.
                     */
                     __wt_spin_lock(session, &chunkcache->chunkcache_lru_lock);
                     being_evicted = chunk->being_evicted;
                     __wt_spin_unlock(session, &chunkcache->chunkcache_lru_lock);
                    if (!being_evicted) {
                        __chunkcache_remove_lru(session, chunk);
                        __chunkcache_free_chunk(session, chunk);
                        printf("\nremove: %s(%d), offset=%" PRId64 ", size=%d\n",
                               (char*)&hash_id.objectname, hash_id.objectid, offset, size);
                        break;
                    }
                }
            }
    }
  done:
    __wt_spin_unlock(session, &chunkcache->bucket_locks[bucket_id]);
}

/*
 * __wt_chunkcache_setup --
 *     Set up the chunk cache.
 */
int
__wt_chunkcache_setup(WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
{
    WT_CHUNKCACHE *chunkcache;
    WT_CONFIG_ITEM cval;
    uint i;
    wt_thread_t evict_thread_tid;

    chunkcache = &S2C(session)->chunkcache;

    if (chunkcache->type != WT_CHUNKCACHE_UNCONFIGURED && !reconfig)
        WT_RET_MSG(session, EINVAL, "chunk cache setup requested, but cache is already configured");
    if (reconfig)
        WT_RET_MSG(session, EINVAL, "reconfiguration of chunk cache not supported");

    WT_RET(__wt_config_gets(session, cfg, "chunk_cache.enabled", &cval));
    if (cval.val == 0)
        return (0);

    WT_RET(__wt_config_gets(session, cfg, "chunk_cache.capacity", &cval));
    if ((chunkcache->capacity = (uint64_t)cval.val) <= 0)
        WT_RET_MSG(session, EINVAL, "chunk cache capacity must be greater than zero");

    WT_RET(__wt_config_gets(session, cfg, "chunk_cache.chunk_size", &cval));
    if ((chunkcache->default_chunk_size = (uint64_t)cval.val) <= 0)
        WT_RET_MSG(session, EINVAL, "chunk size must be greater than zero");

    WT_RET(__wt_config_gets(session, cfg, "chunk_cache.hashsize", &cval));
    if ((chunkcache->hashtable_size = (u_int)cval.val) == 0)
        chunkcache->hashtable_size = WT_CHUNKCACHE_DEFAULT_HASHSIZE;
    else if (chunkcache->hashtable_size < WT_CHUNKCACHE_MINHASHSIZE ||
             chunkcache->hashtable_size > WT_CHUNKCACHE_MAXHASHSIZE)
        WT_RET_MSG(session, EINVAL,
                   "chunk cache hashtable size must be between %d and %d entries and we have %d",
                   WT_CHUNKCACHE_MINHASHSIZE, WT_CHUNKCACHE_MAXHASHSIZE,
                   chunkcache->hashtable_size);

    WT_RET(__wt_config_gets(session, cfg, "chunk_cache.type", &cval));
    if (cval.len ==0 ||
        WT_STRING_MATCH("dram", cval.str, cval.len) || WT_STRING_MATCH("DRAM", cval.str, cval.len))
        chunkcache->type = WT_CHUNKCACHE_DRAM;
    else if (WT_STRING_MATCH("file", cval.str, cval.len) ||
             WT_STRING_MATCH("FILE", cval.str, cval.len)) {
#ifdef ENABLE_MEMKIND
        chunkcache->type = WT_CHUNKCACHE_FILE;
        WT_RET(__wt_config_gets(session, cfg, "chunk_cache.device_path", &cval));
        WT_RET(__wt_strndup(session, cval.str, cval.len, &chunkcache->dev_path));
        if (!__wt_absolute_path(chunkcache->dev_path))
            WT_RET_MSG(session, EINVAL, "File directory must be an absolute path");
#else
        WT_RET_MSG(session, EINVAL, "chunk cache of type FILE requires libmemkind");
#endif
    }

    WT_RET(__wt_spin_init(session, &chunkcache->chunkcache_lru_lock, "chunkcache LRU lock"));
    WT_RET(__wt_calloc_def(session, chunkcache->hashtable_size, &chunkcache->hashtable));
    WT_RET(__wt_calloc_def(session, chunkcache->hashtable_size, &chunkcache->bucket_locks));

    for (i = 0; i < chunkcache->hashtable_size; i++) {
        TAILQ_INIT(&chunkcache->hashtable[i].chainq); /* chunk cache collision chains */
        WT_RET(__wt_spin_init(session, &chunkcache->bucket_locks[i], "chunk cache bucket locks"));
    }

    if (chunkcache->type != WT_CHUNKCACHE_DRAM) {
#ifdef ENABLE_MEMKIND
        if ((ret = memkind_create_pmem(chunkcache->dev_path, 0, &chunkcache->memkind)) != 0)
            WT_RET_MSG(session, ret, "chunk cache failed to initialize: memkind_create_pmem");
#else
        WT_RET_MSG(session, EINVAL, "Chunk cache that is not in DRAM requires libmemkind");
#endif
    }

    WT_RET(__wt_thread_create(
            session, &evict_thread_tid, __chunkcache_eviction_thread, (void *)session));

     chunkcache->configured = true;
    __wt_verbose(session, WT_VERB_CHUNKCACHE,
                 "configured cache of type %s, with capacity %" PRIu64 "",
                 (chunkcache->type == WT_CHUNKCACHE_DRAM)?"DRAM":"FILE", chunkcache->capacity);
    return (0);
}
