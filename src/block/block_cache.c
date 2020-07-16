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
 *     Get a block from the cache.
 */
int
__wt_blkcache_get(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset,
		  wt_off_t size, void *data)
{
    WT_BLKCACHE *blkcache;
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_BLKCACHE_ID id;
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
    __wt_spin_lock(session, &blkcache->hash_lock[bucket]);
    TAILQ_FOREACH(blkcache_item, &blkcache->hash[bucket], hashq) {
	if (memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) {
	    memcpy(data, blkcache_item->data, size);
	    __wt_spin_unlock(session, &blkcache->hash_lock[bucket]);
	    return (0);
	}
    }

    /* Block not found */
     __wt_spin_unlock(session, &blkcache->hash_lock[bucket]);
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
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_BLKCACHE_ID id;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash;
    void *data_ptr = NULL;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    /* Allocate space in the cache outside of the critical section */
    WT_RET(__wt_blkcache_alloc(session, &data_ptr, size));

    id.fh = fh;
    id.offset = offset;
    id.size = size;
    hash = __wt_hash_city64(&id, sizeof(id));

    bucket = hash % WT_HASH_ARRAY_SIZE;
    __wt_spin_lock(session, &blkcache->hash_lock[bucket]);
    TAILQ_FOREACH(blkcache_item, &blkcache->hash[bucket], hashq) {
	if (memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) {
	    __wt_spin_unlock(session, &blkcache->hash_lock[bucket]);
	    return (0);
	}
    }

    WT_ERR(__wt_calloc_one(session, &blkcache_item));
    blkcache_item->data = data_ptr;
    memcpy(blkcache_item->data, data, size);
    TAILQ_INSERT_HEAD(&blkcache->hash[bucket], blkcache_item, hashq);

    blkcache->num_data_blocks++;
    blkcache->space_occupied += size;

    __wt_spin_unlock(session, &blkcache->hash_lock[bucket]);
    return (0);
  err:
    __wt_blkcache_free(data_ptr);
    __wt_spin_unlock(session, &blkcache->hash_lock[bucket]);
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
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_BLKCACHE_ID id;
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
    __wt_spin_lock(session, &blkcache->hash_lock[bucket]);
    TAILQ_FOREACH(blkcache_item, &blkcache->hash[bucket], hashq) {
	if (memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) {
	    TAILQ_REMOVE(&blkcache->hash[bucket], blkcache_item, hashq);
	    blkcache->num_data_blocks--;
	    blkcache->space_occupied -= size;
	    __wt_spin_unlock(session, &blkcache->hash_lock[bucket]);
	    __wt_blkcache_free(session, blkcache_item->data);
	    __wt_free(session, blkcache_item);
	    return;
	}
    }
    __wt_spin_unlock(session, &blkcache->hash_lock[bucket]);
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
