/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_blkcache_alloc --
 *     Allocate a block of memory in the cache.
 */
static int
__wt_blkcache_alloc(WT_SESSION_IMPL *session, size_t size, void **retp)
{
    WT_BLKCACHE *blkcache;
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    if (blkcache->type == BLKCACHE_DRAM)
        return __wt_malloc(session, size, retp);
    else if (blkcache->type == BLKCACHE_NVRAM) {
#ifdef HAVE_LIBMEMKIND
        *retp = memkind_malloc(blkcache->pmem_kind, size);
        if (retp == NULL)
            return __wt_errno();
#else
        WT_RET_MSG(session, EINVAL, "NVRAM block cache type requires libmemkind.");
#endif
    }
    return (0);
}

/*
 * __wt_blkcache_free --
 *     Free the block in the cache.
 */
static void
__wt_blkcache_free(WT_SESSION_IMPL *session, void *ptr)
{

    WT_BLKCACHE *blkcache;
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    if (blkcache->type == BLKCACHE_DRAM)
        __wt_free_int(session, ptr);
    else if (blkcache->type == BLKCACHE_NVRAM) {
#ifdef HAVE_LIBMEMKIND
        memkind_free(blkcache->pmem_kind, ptr);
#else
        __wt_err(session, EINVAL, "NVRAM block cache type requires libmemkind.");
#endif
    }
}

/*
 * __wt_blkcache_get_or_check --
 *     Get a block from the cache or check if one exists.
 */
int
__wt_blkcache_get_or_check(
  WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset, size_t size, void *data_ptr)
{
    WT_BLKCACHE *blkcache;
    WT_BLKCACHE_ID id;
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash;

    conn = S2C(session);
    blkcache = &conn->blkcache;
    blkcache_item = NULL;

    if (blkcache->type == BLKCACHE_UNCONFIGURED)
        return -1;
    if (data_ptr != NULL)
        WT_STAT_CONN_INCR(session, block_cache_data_refs);

    if (fh->file_type != WT_FS_OPEN_FILE_TYPE_DATA)
        printf("Non-data file\n");

    id.fh = fh;
    id.offset = offset;
    id.size = size;
    hash = __wt_hash_city64(&id, sizeof(id));

    bucket = hash % WT_HASH_ARRAY_SIZE;
    __wt_spin_lock(session, &blkcache->hash_locks[bucket]);
    TAILQ_FOREACH (blkcache_item, &blkcache->hash[bucket], hashq) {
        if ((ret = memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) == 0) {
            if (data_ptr != NULL)
                memcpy(data_ptr, blkcache_item->data, size);
            __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
            WT_STAT_CONN_INCR(session, block_cache_hits);
            __wt_verbose(session, WT_VERB_BLKCACHE, "block in cache: "
			 "off %" PRIuMAX ", size %" PRIu32,
			 (uintmax_t)offset, (uint32_t)size);
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
__wt_blkcache_put(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset, size_t size, void *data)
{
    WT_BLKCACHE *blkcache;
    WT_BLKCACHE_ID id;
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash;
    void *data_ptr;

    conn = S2C(session);
    blkcache = &conn->blkcache;
    blkcache_item = NULL;
    data_ptr = NULL;

    if (blkcache->type == BLKCACHE_UNCONFIGURED)
        return -1;

    /* Are we within cache size limits? */
    if (blkcache->bytes_used >= blkcache->max_bytes)
        return -1;

    /*
     * Allocate space in the cache outside of the critical section. In the unlikely event that we
     * fail to allocate metadata, or if the item exists and the caller did not check for that prior
     * to calling this function, we will free the space.
     */
    WT_RET(__wt_blkcache_alloc(session, size, &data_ptr));

    id.fh = fh;
    id.offset = offset;
    id.size = size;
    hash = __wt_hash_city64(&id, sizeof(id));

    bucket = hash % WT_HASH_ARRAY_SIZE;
    __wt_spin_lock(session, &blkcache->hash_locks[bucket]);
    TAILQ_FOREACH (blkcache_item, &blkcache->hash[bucket], hashq) {
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
    __wt_verbose(session, WT_VERB_BLKCACHE, "block inserted in cache: "
		 "off %" PRIuMAX ", size %" PRIu32,
		 (uintmax_t)offset, (uint32_t)size);
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
__wt_blkcache_remove(WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset, size_t size)
{
    WT_BLKCACHE *blkcache;
    WT_BLKCACHE_ID id;
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t bucket, hash;

    conn = S2C(session);
    blkcache = &conn->blkcache;
    blkcache_item = NULL;

    if (blkcache->type == BLKCACHE_UNCONFIGURED)
        return;

    id.fh = fh;
    id.offset = offset;
    id.size = size;
    hash = __wt_hash_city64(&id, sizeof(id));

    bucket = hash % WT_HASH_ARRAY_SIZE;
    __wt_spin_lock(session, &blkcache->hash_locks[bucket]);
    TAILQ_FOREACH (blkcache_item, &blkcache->hash[bucket], hashq) {
        if ((ret = memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) == 0) {
            TAILQ_REMOVE(&blkcache->hash[bucket], blkcache_item, hashq);
            blkcache->num_data_blocks--;
            blkcache->bytes_used -= size;
            __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
            __wt_blkcache_free(session, blkcache_item->data);
            __wt_overwrite_and_free(session, blkcache_item);
            WT_STAT_CONN_DECRV(session, block_cache_bytes, size);
            WT_STAT_CONN_DECR(session, block_cache_blocks);
            return;
        }
    }
    __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
}

/*
 * __wt_blkcache_init --
 *     Initialize the block cache.
 */
static int
__wt_blkcache_init(WT_SESSION_IMPL *session, size_t size, int type, char *nvram_device_path)
{
    WT_BLKCACHE *blkcache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    int i;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    if (type == BLKCACHE_NVRAM) {
#ifdef HAVE_LIBMEMKIND
        if ((ret = memkind_create_pmem(nvram_device_path, 0, &blkcache->pmem_kind)) != 0) {
            WT_RET_MSG(session, ret, "block cache failed to initialize");
            WT_RET(__wt_strndup(
              session, nvram_device_path, strlen(nvram_device_path), &blkcache->nvram_device_path));
            __wt_free(session, nvram_device_path);
        }
#else
        (void)nvram_device_path;
        WT_RET_MSG(session, EINVAL, "NVRAM block cache type requires libmemkind.");
#endif
    }

    for (i = 0; i < WT_HASH_ARRAY_SIZE; i++) {
        TAILQ_INIT(&blkcache->hash[i]); /* Block cache hash lists */
        WT_RET(__wt_spin_init(session, &blkcache->hash_locks[i], "block cache bucket locks"));
    }

    blkcache->type = type;
    blkcache->max_bytes = size;

    return (ret);
}

/*
 * __wt_block_cache_destroy --
 *     Destroy the block cache and free all memory.
 */
void
__wt_block_cache_destroy(WT_SESSION_IMPL *session)
{
    WT_BLKCACHE *blkcache;
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_CONNECTION_IMPL *conn;
    int i;

    conn = S2C(session);
    blkcache = &conn->blkcache;
    blkcache_item = NULL;

    if (blkcache->bytes_used == 0)
        goto done;

    for (i = 0; i < WT_HASH_ARRAY_SIZE; i++) {
        __wt_spin_lock(session, &blkcache->hash_locks[i]);
        while (!TAILQ_EMPTY(&blkcache->hash[i])) {
            blkcache_item = TAILQ_FIRST(&blkcache->hash[i]);
            TAILQ_REMOVE(&blkcache->hash[i], blkcache_item, hashq);
            __wt_blkcache_free(session, blkcache_item->data);
            blkcache->num_data_blocks--;
            blkcache->bytes_used -= blkcache_item->id.size;
            __wt_free(session, blkcache_item);
        }
        __wt_spin_unlock(session, &blkcache->hash_locks[i]);
    }

    WT_ASSERT(session, blkcache->bytes_used == blkcache->num_data_blocks == 0);

done:
#ifdef HAVE_LIBMEMKIND
    if (blkcache->type == BLKCACHE_NVRAM) {
        memkind_destroy_kind(blkcache->pmem_kind);
        __wt_free(session, blkcache->nvram_device_path);
    }
#endif
    /*
     * Zeroing the structure has the effect of setting the block cache type to unconfigured.
     */
    memset((void *)blkcache, 0, sizeof(WT_BLKCACHE));
    printf("block cache destroyed\n");
}

/*
 * __wt_block_cache_setup --
 *     Set up the block cache.
 */
int
__wt_block_cache_setup(WT_SESSION_IMPL *session, const char *cfg[], bool reconfig)
{
    WT_BLKCACHE *blkcache;
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;

    char *nvram_device_path = NULL;
    int cache_type = BLKCACHE_UNCONFIGURED;
    size_t cache_size = 0;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    if (reconfig)
        __wt_block_cache_destroy(session);

    if (blkcache->type != BLKCACHE_UNCONFIGURED)
        WT_RET_MSG(session, -1, "block cache setup requested for a configured cache");

    WT_RET(__wt_config_gets(session, cfg, "block_cache.enabled", &cval));
    if (cval.val == 0)
        return 0;

    WT_RET(__wt_config_gets(session, cfg, "block_cache.size", &cval));
    if ((cache_size = (uint64_t)cval.val) <= 0)
        WT_RET_MSG(session, EINVAL, "block cache size must be greater than zero");

    WT_RET(__wt_config_gets(session, cfg, "block_cache.type", &cval));
    if (WT_STRING_MATCH("dram", cval.str, cval.len) || WT_STRING_MATCH("DRAM", cval.str, cval.len))
        cache_type = BLKCACHE_DRAM;
    else if (WT_STRING_MATCH("nvram", cval.str, cval.len) ||
      WT_STRING_MATCH("NVRAM", cval.str, cval.len)) {
#ifdef HAVE_LIBMEMKIND
        cache_type = BLKCACHE_NVRAM;
        WT_RET(__wt_config_gets(session, cfg, "block_cache.path", &cval));
        WT_RET(__wt_strndup(session, cval.str, cval.len, &nvram_device_path));
#else
        WT_RET_MSG(session, EINVAL, "NVRAM block cache type requires libmemkind");
#endif
    } else
        WT_RET_MSG(session, EINVAL, "Invalid block cache type.");

    printf("initializing block cache of type %d, size %lu, device path: %s\n", cache_type,
      cache_size, (nvram_device_path == NULL) ? "-" : nvram_device_path);
    return __wt_blkcache_init(session, cache_size, cache_type, nvram_device_path);
}
