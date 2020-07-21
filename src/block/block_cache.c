/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */


#include "wt_internal.h"

/*
 * __wt_blkcache_get --
 *     Get a block from the cache. If the data pointer is null, this function
 *     checks if a block exists, returning zero if so, without attempting
 *     to copy the data.
 */
int
__wt_blkcache_get_or_check(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset,
		  wt_off_t size, void *data_ptr)
{
    WT_BLKCACHE *blkcache;
    WT_BLKCACHE_ID id;
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    if (data_ptr != NULL)
	WT_STAT_CONN_INCR(session, block_cache_data_refs);

    id.fh = fh;
    id.offset = offset;
    id.size = size;
    hash = __wt_hash_city64(&id, sizeof(id));

    bucket = hash % WT_HASH_ARRAY_SIZE;
    __wt_spin_lock(session, &blkcache->hash_locks[bucket]);
    TAILQ_FOREACH(blkcache_item, &blkcache->hash[bucket], hashq) {
	if ((ret = memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) == 0) {
	    if (data_ptr != NULL)
		memcpy(data_ptr, blkcache_item->data, size);
	    __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
	    WT_STAT_CONN_INCR(session, block_cache_hits);
	    return (0);
	}
    }

    /* Block not found */
     __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
     WT_STAT_CONN_INCR(session, block_cache_misses);
     return (-1);
}

/*
 * __wt_blkcache_put --
 *     Put a block into the cache.
 */
int
__wt_blkcache_put(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset,
		  wt_off_t size, void *data)
{
    WT_BLKCACHE *blkcache;
    WT_BLKCACHE_ID id;
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash;
    void *data_ptr = NULL;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    /* Are we within cache size limits? */
    if (blkcache->bytes_used >= blkcache->max_bytes)
	return -1;

    /*
     * Allocate space in the cache outside of the critical section.
     * In the unlikely event that we fail to allocate metadata, or if
     * the item exists and the caller did not check for that prior to
     * calling this function, we will free the space.
     */
    WT_RET(__wt_blkcache_alloc(session, size, &data_ptr));

    id.fh = fh;
    id.offset = offset;
    id.size = size;
    hash = __wt_hash_city64(&id, sizeof(id));

    bucket = hash % WT_HASH_ARRAY_SIZE;
    __wt_spin_lock(session, &blkcache->hash_locks[bucket]);
    TAILQ_FOREACH(blkcache_item, &blkcache->hash[bucket], hashq) {
	if ((ret = memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) == 0)
	    goto item_exists;
    }

    WT_ERR(__wt_calloc_one(session, &blkcache_item));
    blkcache_item->data = data_ptr;
    memcpy(blkcache_item->data, data, size);
    TAILQ_INSERT_HEAD(&blkcache->hash[bucket], blkcache_item, hashq);

    blkcache->num_data_blocks++;
    blkcache->bytes_used += size;

    __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
    WT_STAT_CONN_INCRV(session, block_cache_bytes, size);
    WT_STAT_CONN_INCR(session, block_cache_blocks);
    return (0);
  item_exists:
  err:
    __wt_blkcache_free(session, data_ptr);
    __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
    return (ret);
}

/*
 * __wt_blkcache_remove --
 *     Remove a block from the cache.
 */
void
__wt_blkcache_remove(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset,
		  wt_off_t size, void *data)
{
    WT_BLKCACHE *blkcache;
    WT_BLKCACHE_ID id;
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    id.fh = fh;
    id.offset = offset;
    id.size = size;
    hash = __wt_hash_city64(&id, sizeof(id));

    bucket = hash % WT_HASH_ARRAY_SIZE;
    __wt_spin_lock(session, &blkcache->hash_locks[bucket]);
    TAILQ_FOREACH(blkcache_item, &blkcache->hash[bucket], hashq) {
	if ((ret = memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) == 0) {
	    TAILQ_REMOVE(&blkcache->hash[bucket], blkcache_item, hashq);
	    blkcache->num_data_blocks--;
	    blkcache->bytes_used -= size;
	    __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
	    __wt_blkcache_free(session, blkcache_item->data);
	    __wt_free(session, blkcache_item);
	    WT_STAT_CONN_DECRV(session, block_cache_bytes, size);
	    WT_STAT_CONN_DECR(session, block_cache_blocks);
	    return;
	}
    }
    __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
}

int
__wt_blkcache_init(WT_SESSION_IMPL *session, wt_off_t size)
{
    WT_BLKCACHE *blkcache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    int i;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    for (i = 0; i < WT_HASH_ARRAY_SIZE; i++) {
	TAILQ_INIT(&blkcache->hash[i]); /* Block cache hash lists */
	WT_RET(__wt_spin_init(session, &blkcache->hash_locks[i], "block cache bucket locks"));
    }

#if FSDAX
    if ((ret = memkind_create_pmem(DEFAULT_MEMKIND_PATH, 0, &pmem_kind)) != 0)
	WT_RET_MSG(session, ret, "block cache failed to initialize");
#endif
    return (0);
}
