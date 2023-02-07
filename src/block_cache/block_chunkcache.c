/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#define WT_BLOCK_BEGINS_IN_CHUNK(chunk_off, block_off, chunk_size, block_size)   \
    (block_off >= chunk_off && (block_off < chunk_off + (wt_off_t)chunk_size) && \
      (block_off + (wt_off_t)block_size) > (chunk_off + (wt_off_t)chunk_size))

#define WT_BLOCK_ENDS_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) \
    (block_off < chunk_off &&                                                \
      (block_off + (wt_off_t)block_size) <= (chunk_off + (wt_off_t)chunk_size))

#define WT_BLOCK_MIDDLE_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) \
    ((block_off < chunk_off) &&                                                \
      (block_off + (wt_off_t)block_size) > (chunk_off + (wt_off_t)chunk_size))

#define WT_BLOCK_IN_CHUNK(chunk_off, block_off, chunk_size, block_size)          \
    (block_off >= chunk_off && (block_off < chunk_off + (wt_off_t)chunk_size) && \
      (block_off + (wt_off_t)block_size) <= (chunk_off + (wt_off_t)chunk_size))

#define WT_BLOCK_PART_IN_CHUNK(chunk_off, block_off, chunk_size, block_size)    \
    WT_BLOCK_BEGINS_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) ||   \
      WT_BLOCK_ENDS_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) ||   \
      WT_BLOCK_MIDDLE_IN_CHUNK(chunk_off, block_off, chunk_size, block_size) || \
      WT_BLOCK_IN_CHUNK(chunk_off, block_off, chunk_size, block_size)

#define WT_CHUNK_MARK_VALID(session, chunkcache, chunk)                            \
    if (!chunk->valid) {                                                           \
        (void)__wt_atomic_addv32(&chunk->valid, 1);                                \
        __wt_spin_lock(session, &chunkcache->chunkcache_lru_lock);                 \
        TAILQ_INSERT_HEAD(&chunkcache->chunkcache_lru_list, chunk, next_lru_item); \
        __wt_spin_unlock(session, &chunkcache->chunkcache_lru_lock);               \
    }

#define WT_CHUNK_OFFSET(chunkcache, offset) \
    (wt_off_t)(((size_t)offset / chunkcache->chunk_size) * chunkcache->chunk_size)

#define WT_BUCKET_CHUNKS(chunkcache, bucket_id) &(chunkcache->hashtable[bucket_id].colliding_chunks)

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
        WT_STAT_CONN_INCR(session, chunkcache_chunks);
        WT_STAT_CONN_INCRV(session, chunkcache_bytes, chunk->chunk_size);
    }
    return (ret);
}

/*
 * __chunkcache_admit_size --
 *     Decide if we can admit the chunk given the limit on cache capacity and return the size of the
 *     chunk to be admitted.
 */
static size_t
__chunkcache_admit_size(WT_SESSION_IMPL *session)
{
    WT_CHUNKCACHE *chunkcache;

    chunkcache = &S2C(session)->chunkcache;

    if ((chunkcache->bytes_used + chunkcache->chunk_size) < chunkcache->capacity)
        return (chunkcache->chunk_size);

    WT_STAT_CONN_INCR(session, chunkcache_exceeded_capacity);
    __wt_verbose(session, WT_VERB_CHUNKCACHE, "exceeded chunkcache capacity of %" PRIu64 " bytes",
      chunkcache->capacity);
    return (0);
}

/*
 * __chunkcache_alloc_chunk --
 *     Allocate the chunk and its metadata for a block at a given offset.
 */
static int
__chunkcache_alloc_chunk(WT_SESSION_IMPL *session, WT_CHUNKCACHE_CHUNK **newchunk, wt_off_t offset,
  WT_BLOCK *block, WT_CHUNKCACHE_HASHID *hash_id)
{
    WT_CHUNKCACHE *chunkcache;
    size_t chunk_size;
    uint64_t hash;

    *newchunk = NULL;
    chunkcache = &S2C(session)->chunkcache;

    WT_ASSERT(session, offset > 0);

    /*
     * Calculate the size and the offset for the chunk. The chunk storage area is broken into
     * equally sized chunks of configured size. We calculate the offset of the chunk into which the
     * block's offset falls. Chunks are equally sized and are not necessarily a multiple of a block.
     * So a block may begin in one chunk and end in another. It may also span multiple chunks, if
     * the chunk size is configured much smaller than a block size (we hope that never happens). In
     * the allocation function we don't care about the block's size. If more than one chunk is
     * needed to cover the entire block, another function will take care of allocating multiple
     * chunks.
     */

    if ((chunk_size = __chunkcache_admit_size(session)) == 0)
        return (WT_ERROR);
    if (__wt_calloc(session, 1, sizeof(WT_CHUNKCACHE_CHUNK), newchunk) != 0)
        return (WT_ERROR);

    /* Convert the block offset to the offset of the enclosing chunk */
    (*newchunk)->chunk_offset = WT_CHUNK_OFFSET(chunkcache, offset);
    /* Chunk cannot be larger than the file */
    (*newchunk)->chunk_size = WT_MIN(chunk_size, (size_t)(block->size - (*newchunk)->chunk_offset));

    /* Part of the hash ID was populated by the caller, but we must set the offset */
    (*newchunk)->hash_id = *hash_id;
    (*newchunk)->hash_id.offset = (*newchunk)->chunk_offset;
    hash = __wt_hash_city64((void *)hash_id, sizeof(WT_CHUNKCACHE_HASHID));
    (*newchunk)->bucket_id = (unsigned int)(hash % chunkcache->hashtable_size);

    if (__chunkcache_alloc_space(session, *newchunk) != 0) {
        __wt_free(session, *newchunk);
        return (WT_ERROR);
    }
    __wt_verbose(session, WT_VERB_CHUNKCACHE, "allocate: %s(%u), offset=%" PRIu64 ", size=%" PRIu64,
      (char *)&(*newchunk)->hash_id.objectname, (*newchunk)->hash_id.objectid,
      (uint64_t)(*newchunk)->chunk_offset, (uint64_t)(*newchunk)->chunk_size);

    return (0);
}

/*
 * __chunkcache_free_chunk --
 *     Free the memory occupied by the chunk and the metadata.
 */
static void
__chunkcache_free_chunk(WT_SESSION_IMPL *session, WT_CHUNKCACHE_CHUNK *chunk)
{
    WT_CHUNKCACHE *chunkcache;

    chunkcache = &S2C(session)->chunkcache;

    (void)__wt_atomic_sub64(&chunkcache->bytes_used, chunk->chunk_size);
    WT_STAT_CONN_DECRV(session, chunkcache_bytes, chunk->chunk_size);
    WT_STAT_CONN_DECR(session, chunkcache_chunks);

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
}

/*
 * __chunkcache_makehash --
 *     Populate the hash data structure, which uniquely identifies the chunk, and return the
 *     hashtable bucket number corresponding to this hash.
 */
static inline uint
__chunkcache_makehash(WT_CHUNKCACHE *chunkcache, WT_CHUNKCACHE_HASHID *hash_id, WT_BLOCK *block,
  uint32_t objectid, wt_off_t offset)
{
    uint64_t hash;

    bzero(hash_id, sizeof(WT_CHUNKCACHE_HASHID));
    hash_id->objectid = objectid;
    memcpy(&hash_id->objectname, block->name, WT_MIN(strlen(block->name), WT_CHUNKCACHE_NAMEMAX));
    hash_id->offset = WT_CHUNK_OFFSET(chunkcache, offset);
    hash = __wt_hash_city64((void *)hash_id, sizeof(WT_CHUNKCACHE_HASHID));

    /* Return the bucket ID */
    return (uint)(hash % chunkcache->hashtable_size);
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
    bool found_eviction_candidate;

    chunkcache = &S2C(session)->chunkcache;
    found_eviction_candidate = false;

    /*
     * 1. With the LRU list lock held, we remove the chunk at the list's tail and mark
     *    that chunk as being evicted.
     *    That prevents the code that removes outdated chunks from freeing the chunk before we do.
     * 2. Remove the chunk from its chunk's chain, acquiring appropriate locks.
     * 3. Free the chunk.
     */
    __wt_spin_lock(session, &chunkcache->chunkcache_lru_lock);
    TAILQ_FOREACH_REVERSE(
      chunk_to_evict, &chunkcache->chunkcache_lru_list, __wt_chunkcache_lru, next_lru_item)
    {
        if (chunk_to_evict->valid) {
            TAILQ_REMOVE(&chunkcache->chunkcache_lru_list, chunk_to_evict, next_lru_item);
            chunk_to_evict->being_evicted = true;
            found_eviction_candidate = true;
            break;
        }
    }
    __wt_spin_unlock(session, &chunkcache->chunkcache_lru_lock);

    if (!found_eviction_candidate)
        return (false);

    __wt_verbose(session, WT_VERB_CHUNKCACHE, "evict: %s(%u), offset=%" PRIu64 ", size=%" PRIu64,
      (char *)&chunk_to_evict->hash_id.objectname, chunk_to_evict->hash_id.objectid,
      (uint64_t)chunk_to_evict->chunk_offset, (uint64_t)chunk_to_evict->chunk_size);

    __wt_spin_lock(session, &chunkcache->bucket_locks[chunk_to_evict->bucket_id]);
    TAILQ_REMOVE(
      WT_BUCKET_CHUNKS(chunkcache, chunk_to_evict->bucket_id), chunk_to_evict, next_chunk);
    __wt_spin_unlock(session, &chunkcache->bucket_locks[chunk_to_evict->bucket_id]);
    __chunkcache_free_chunk(session, chunk_to_evict);

    WT_STAT_CONN_INCR(session, chunkcache_chunks_evicted);
    return (true);
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
        if ((chunkcache->bytes_used + chunkcache->chunk_size) >
          chunkcache->evict_watermark * chunkcache->capacity / 100)
            __chunkcache_evict_one(session);
        __wt_sleep(1, 0); /* a more appropriate frequency must be chosen */
    }
    return (WT_THREAD_RET_VALUE);
}

/*
 * __wt_chunkcache_get --
 *     If the cache has the data at the given size and offset, copy it into the supplied buffer.
 *     Otherwise, read and cache the chunks containing the requested data.
 */
bool
__wt_chunkcache_get(WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t objectid, wt_off_t offset,
  uint32_t size, void *dst)
{
    WT_CHUNKCACHE *chunkcache;
    WT_CHUNKCACHE_CHUNK *chunk;
    WT_CHUNKCACHE_HASHID hash_id;
    bool chunk_cached;
    unsigned int bucket_id, retries;
    size_t already_read, left_to_read, readable_in_chunk, size_copied;

    chunkcache = &S2C(session)->chunkcache;
    already_read = 0;
    left_to_read = size;
    retries = 0;

    if (!chunkcache->configured)
        return (false);

    __wt_verbose(session, WT_VERB_CHUNKCACHE, "get: %s(%u), offset=%" PRId64 ", size=%u",
      (char *)block->name, objectid, offset, size);

    WT_STAT_CONN_INCR(session, chunkcache_lookups);

    /* A block may span two (or more) chunks. Loop until we have read all the data. */
    while (left_to_read > 0) {
        /* Find the bucket for the chunk containing this offset. */
        bucket_id = __chunkcache_makehash(
          chunkcache, &hash_id, block, objectid, offset + (wt_off_t)already_read);
retry:
        chunk_cached = false;
        __wt_spin_lock(session, &chunkcache->bucket_locks[bucket_id]);
        TAILQ_FOREACH (chunk, WT_BUCKET_CHUNKS(chunkcache, bucket_id), next_chunk) {
            if (memcmp(&chunk->hash_id, &hash_id, sizeof(hash_id)) == 0) {
                /* If the chunk is there, but invalid, there is I/O in progress. Retry. */
                if (!chunk->valid) {
                    __wt_spin_unlock(session, &chunkcache->bucket_locks[bucket_id]);
                    if (retries++ < WT_CHUNKCACHE_MAX_RETRIES) {
                        WT_STAT_CONN_INCR(session, chunkcache_retries);
                        goto retry;
                    } else {
                        __wt_verbose(session, WT_VERB_CHUNKCACHE,
                          "lookup timed out after %u retries", retries);
                        WT_STAT_CONN_INCR(session, chunkcache_toomany_retries);
                        return (false);
                    }
                }
                /* Found the needed chunk. */
                WT_ASSERT(session,
                  WT_BLOCK_PART_IN_CHUNK(chunk->chunk_offset, offset + (wt_off_t)already_read,
                    chunk->chunk_size, left_to_read));

                /* We can't read beyond the chunk's boundary */
                readable_in_chunk =
                  (size_t)chunk->chunk_offset + chunk->chunk_size - (size_t)offset;
                size_copied = WT_MIN(readable_in_chunk, left_to_read);
                memcpy((void *)((uint64_t)dst + already_read),
                  chunk->chunk_location + (offset + (wt_off_t)already_read - chunk->chunk_offset),
                  size_copied);

                if (already_read > 0)
                    WT_STAT_CONN_INCR(session, chunkcache_spans_chunks_read);

                already_read += size_copied;
                left_to_read -= size_copied;

                chunk_cached = true;
                __wt_spin_unlock(session, &chunkcache->bucket_locks[bucket_id]);
                WT_STAT_CONN_INCR(session, chunkcache_hits);
                break;
            }
        }
        /* The chunk is not cached. Read it from storage and insert into the cache. */
        if (!chunk_cached) {
            if (__chunkcache_alloc_chunk(
                  session, &chunk, offset + (wt_off_t)already_read, block, &hash_id) != 0) {
                __wt_spin_unlock(session, &chunkcache->bucket_locks[bucket_id]);
                return (false);
            }
            /*
             * Insert the chunk into the bucket before releasing the lock and doing I/O. This way we
             * avoid two threads trying to cache the same chunk.
             */
            TAILQ_INSERT_HEAD(WT_BUCKET_CHUNKS(chunkcache, bucket_id), chunk, next_chunk);
            __wt_spin_unlock(session, &chunkcache->bucket_locks[bucket_id]);

            /* Read the chunk and mark it as valid */
            if (__wt_read(session, block->fh, chunk->chunk_offset, chunk->chunk_size,
                  chunk->chunk_location) != 0) {
                __wt_spin_lock(session, &chunkcache->bucket_locks[bucket_id]);
                TAILQ_REMOVE(WT_BUCKET_CHUNKS(chunkcache, bucket_id), chunk, next_chunk);
                __wt_spin_unlock(session, &chunkcache->bucket_locks[bucket_id]);
                __chunkcache_free_chunk(session, chunk);
                WT_STAT_CONN_INCR(session, chunkcache_io_failed);
                return (false);
            }
            WT_CHUNK_MARK_VALID(session, chunkcache, chunk);

            __wt_verbose(session, WT_VERB_CHUNKCACHE,
              "insert: %s(%u), offset=%" PRId64 ", size=%lu", (char *)block->name, objectid,
              chunk->chunk_offset, chunk->chunk_size);
            goto retry;
        }
    }
    return (true);
}

/*
 * __wt_chunkcache_remove --
 *     Remove the chunk containing an outdated block.
 */
void
__wt_chunkcache_remove(
  WT_SESSION_IMPL *session, WT_BLOCK *block, uint32_t objectid, wt_off_t offset, uint32_t size)
{
    WT_CHUNKCACHE *chunkcache;
    WT_CHUNKCACHE_CHUNK *chunk;
    WT_CHUNKCACHE_HASHID hash_id;
    bool done;
    unsigned int bucket_id;
    size_t already_removed, left_to_remove, removable_in_chunk, size_removed;

    chunkcache = &S2C(session)->chunkcache;
    already_removed = 0;
    left_to_remove = size;

    if (!chunkcache->configured)
        return;

    __wt_verbose(session, WT_VERB_CHUNKCACHE, "remove block: %s(%u), offset=%" PRId64 ", size=%u",
      (char *)block->name, objectid, offset, size);

    /* A block may span many chunks. Loop until we have removed all the chunks. */
    while (left_to_remove > 0) {
        /* Find the bucket for the containing chunk. */
        bucket_id = __chunkcache_makehash(
          chunkcache, &hash_id, block, objectid, offset + (wt_off_t)already_removed);
        done = false;
        removable_in_chunk = (size_t)WT_CHUNK_OFFSET(chunkcache, offset + already_removed) +
          chunkcache->chunk_size - ((size_t)offset + already_removed);
        __wt_spin_lock(session, &chunkcache->bucket_locks[bucket_id]);
        TAILQ_FOREACH (chunk, WT_BUCKET_CHUNKS(chunkcache, bucket_id), next_chunk) {
            if (memcmp(&chunk->hash_id, &hash_id, sizeof(hash_id)) == 0) {
                if (chunk->valid) {
                    WT_ASSERT(session,
                      WT_BLOCK_PART_IN_CHUNK(chunk->chunk_offset, offset, chunk->chunk_size, size));

                    WT_STAT_CONN_INCR(session, chunkcache_chunks_invalidated);
                    /*
                     * If the chunk is being evicted, the eviction code will remove it and free it,
                     * so we are done.
                     */
                    __wt_spin_lock(session, &chunkcache->chunkcache_lru_lock);
                    if (chunk->being_evicted)
                        done = true;
                    else
                        TAILQ_REMOVE(&chunkcache->chunkcache_lru_list, chunk, next_lru_item);
                    __wt_spin_unlock(session, &chunkcache->chunkcache_lru_lock);

                    if (done)
                        break;

                    TAILQ_REMOVE(WT_BUCKET_CHUNKS(chunkcache, bucket_id), chunk, next_chunk);
                    __chunkcache_free_chunk(session, chunk);
                    __wt_verbose(session, WT_VERB_CHUNKCACHE,
                      "removed chunk: %s(%u), offset=%" PRId64 ", size=%" PRIu64,
                      (char *)&hash_id.objectname, hash_id.objectid, chunk->chunk_offset,
                      (uint64_t)chunk->chunk_size);
                    done = true;
                    break;
                }
            }
        }
        /*
         * If we found the chunk, we removed the data and we update the variables so that we can
         * find the next chunk that might contain the block's data. If we did not find the cached
         * chunk, we still update the variable, so that we can look for the next chunk that might
         * have part of the block. If we don't update these variables, we will be stuck forever
         * looking for a chunk that's not cached.
         */
        size_removed = WT_MIN(removable_in_chunk, left_to_remove);
        already_removed += size_removed;
        left_to_remove -= size_removed;

        if (left_to_remove > 0)
            WT_STAT_CONN_INCR(session, chunkcache_spans_chunks_remove);

        __wt_spin_unlock(session, &chunkcache->bucket_locks[bucket_id]);
    }
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
    unsigned int i;
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
    if ((chunkcache->chunk_size = (uint64_t)cval.val) <= 0)
        chunkcache->chunk_size = WT_CHUNKCACHE_DEFAULT_CHUNKSIZE;

    WT_RET(__wt_config_gets(session, cfg, "chunk_cache.hashsize", &cval));
    if ((chunkcache->hashtable_size = (u_int)cval.val) == 0)
        chunkcache->hashtable_size = WT_CHUNKCACHE_DEFAULT_HASHSIZE;
    else if (chunkcache->hashtable_size < WT_CHUNKCACHE_MINHASHSIZE ||
      chunkcache->hashtable_size > WT_CHUNKCACHE_MAXHASHSIZE)
        WT_RET_MSG(session, EINVAL,
          "chunk cache hashtable size must be between %d and %d entries and we have %u",
          WT_CHUNKCACHE_MINHASHSIZE, WT_CHUNKCACHE_MAXHASHSIZE, chunkcache->hashtable_size);

    WT_RET(__wt_config_gets(session, cfg, "chunk_cache.evict_trigger", &cval));
    if (((chunkcache->evict_watermark = (u_int)cval.val) == 0) ||
      (chunkcache->evict_watermark > 100))
        WT_RET_MSG(session, EINVAL, "evict trigger must be between 0 and 100");

    WT_RET(__wt_config_gets(session, cfg, "chunk_cache.type", &cval));
    if (cval.len == 0 || WT_STRING_MATCH("dram", cval.str, cval.len) ||
      WT_STRING_MATCH("DRAM", cval.str, cval.len))
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
        TAILQ_INIT(&(chunkcache->hashtable[i].colliding_chunks));
        WT_RET(__wt_spin_init(session, &chunkcache->bucket_locks[i], "chunk cache bucket locks"));
    }
    TAILQ_INIT(&chunkcache->chunkcache_lru_list);

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
      (chunkcache->type == WT_CHUNKCACHE_DRAM) ? "DRAM" : "FILE", chunkcache->capacity);
    return (0);
}
