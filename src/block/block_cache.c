/*-
 * Copyright (c) 2014-2020 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __blkcache_alloc --
 *     Allocate a block of memory in the cache.
 */
static int
__blkcache_alloc(WT_SESSION_IMPL *session, size_t size, void **retp)
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
        if (*retp == NULL)
            return WT_CACHE_FULL;
#else
        WT_RET_MSG(session, EINVAL, "NVRAM block cache type requires libmemkind.");
#endif
    }
    return (0);
}

/*
 * Every so often, compute the total size of the files open
 * in the block manager.
 */
#define BLKCACHE_FILESIZE_EST_FREQ 1000

static size_t
__blkcache_estimate_filesize(WT_SESSION_IMPL *session)
{
    WT_BLOCK *block;
    WT_BLKCACHE *blkcache;
    WT_CONNECTION_IMPL *conn;
    size_t size;
    uint64_t bucket;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    /* This is a deliberate race condition */
    if (blkcache->refs_since_filesize_estimated++ < BLKCACHE_FILESIZE_EST_FREQ)
	return blkcache->estimated_file_size;

    blkcache->refs_since_filesize_estimated = 0;

    size = 0;
    __wt_spin_lock(session, &conn->block_lock);
    for (bucket = 0; bucket < conn->hash_size; bucket++) {
	TAILQ_FOREACH (block, &conn->blockhash[bucket], hashq) {
	    size += (size_t)block->size;
	}
    }
    blkcache->estimated_file_size = size;
    __wt_spin_unlock(session, &conn->block_lock);

    return blkcache->estimated_file_size ;
}

/*
 * __blkcache_free --
 *     Free a chunk of memory.
 */
static void
__blkcache_free(WT_SESSION_IMPL *session, void *ptr)
{

    WT_BLKCACHE *blkcache;
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    if (blkcache->type == BLKCACHE_DRAM)
        __wt_free(session, ptr);
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
    WT_SESSION_IMPL *session, wt_off_t offset, size_t size, uint32_t checksum,
    void *data_ptr)
{
    WT_BLKCACHE *blkcache;
    WT_BLKCACHE_ID id;
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    size_t filesize;
    uint64_t bucket, hash;
#if BLKCACHE_TRACE == 1
    uint64_t time_start, time_stop;

    time_start = time_stop = 0;
#endif

    conn = S2C(session);
    blkcache = &conn->blkcache;
    blkcache_item = NULL;

    if (blkcache->type == BLKCACHE_UNCONFIGURED)
        return -1;

    WT_STAT_CONN_INCR(session, block_cache_data_refs);

    /* If more than half the file is likely to be in the buffer cache,
     * don't use the cache.
     */
    filesize = 	__blkcache_estimate_filesize(session);
    if (blkcache->system_ram >= filesize * blkcache->fraction_in_dram) {
	WT_STAT_CONN_INCR(session, block_cache_bypass_get);
	WT_STAT_CONN_SET(session, block_cache_bypass_filesize, filesize);
	return -1;
    }

    id.checksum = (uint64_t)checksum;
    id.offset = (uint64_t)offset;
    id.size = (uint64_t)size;
    hash = __wt_hash_city64(&id, sizeof(id));

    bucket = hash % blkcache->hash_size;
    __wt_spin_lock(session, &blkcache->hash_locks[bucket]);
    TAILQ_FOREACH (blkcache_item, &blkcache->hash[bucket], hashq) {
        if ((ret = memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) == 0) {
#if BLKCACHE_TRACE == 1
	    time_start = __wt_clock(session);
#endif
            if (data_ptr != NULL)
                memcpy(data_ptr, blkcache_item->data, size);

#if BLKCACHE_TRACE == 1
	    time_stop = __wt_clock(session);
#endif
            __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
#if BLKCACHE_TRACE == 1
	    __wt_verbose(session, WT_VERB_BLKCACHE, "memory read latency: "
			 "offset=%" PRIuMAX ", size=%" PRIu32 ", hash=%" PRIu64 ", "
			 "latency=%" PRIu64 " ns.",
			 (uintmax_t)offset, (uint32_t)size, hash,
			 WT_CLOCKDIFF_NS(time_stop, time_start));
#endif
            WT_STAT_CONN_INCR(session, block_cache_hits);
            __wt_verbose(session, WT_VERB_BLKCACHE, "block found in cache: "
			 "offset=%" PRIuMAX ", size=%" PRIu32 ", "
			 "checksum=%" PRIu32 ", hash=%" PRIu64,
			 (uintmax_t)offset, (uint32_t)size, checksum, hash);
            return (0);
        }
    }

    /* Block not found */
    __wt_verbose(session, WT_VERB_BLKCACHE, "block not found in cache: "
		 "offset=%" PRIuMAX ", size=%" PRIu32 ", "
		 "checksum=%" PRIu32 ", hash=%" PRIu64,
		 (uintmax_t)offset, (uint32_t)size, checksum, hash);

    __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
    WT_STAT_CONN_INCR(session, block_cache_misses);
    return (-1);
}

/*
 * __wt_blkcache_put --
 *     Put a block into the cache.
 */
int
__wt_blkcache_put(WT_SESSION_IMPL *session, wt_off_t offset, size_t size,
		  uint32_t checksum, void *data, bool write)
{
    WT_BLKCACHE *blkcache;
    WT_BLKCACHE_ID id;
    WT_BLKCACHE_ITEM *blkcache_item;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    size_t filesize;
    uint64_t bucket, hash;
    void *data_ptr;
#if BLKCACHE_TRACE == 1
    uint64_t time_start, time_stop;

    time_start = time_stop = 0;
#endif

    conn = S2C(session);
    blkcache = &conn->blkcache;
    blkcache_item = NULL;
    data_ptr = NULL;

    if (blkcache->type == BLKCACHE_UNCONFIGURED)
        return -1;

    /*
     * If DRAM is large enough to cache the desired fraction of the file, don't
     * populate the cache on the read path.
     */
    if (write == false) {
	filesize = __blkcache_estimate_filesize(session);
	if (blkcache->system_ram >= filesize * blkcache->fraction_in_dram)
	    return -1;
    }

    /* Are we within cache size limits? */
    if (blkcache->bytes_used >= blkcache->max_bytes)
        return WT_CACHE_FULL;

    /*
     * Allocate space in the cache outside of the critical section. In the unlikely event that we
     * fail to allocate metadata, or if the item exists and the caller did not check for that prior
     * to calling this function, we will free the space.
     */
    WT_RET(__blkcache_alloc(session, size, &data_ptr));

    id.checksum = (uint64_t)checksum;
    id.offset = (uint64_t)offset;
    id.size = (uint64_t)size;
    hash = __wt_hash_city64(&id, sizeof(id));

    bucket = hash % blkcache->hash_size;
    __wt_spin_lock(session, &blkcache->hash_locks[bucket]);
    TAILQ_FOREACH (blkcache_item, &blkcache->hash[bucket], hashq) {
        if ((ret = memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) == 0)
            goto item_exists;
    }

    WT_ERR(__wt_calloc_one(session, &blkcache_item));
    blkcache_item->id = id;
    blkcache_item->data = data_ptr;

#if BLKCACHE_TRACE == 1
    time_start = __wt_clock(session);
#endif
    memcpy(blkcache_item->data, data, size);
#if BLKCACHE_TRACE == 1
    time_stop = __wt_clock(session);
#endif

    TAILQ_INSERT_HEAD(&blkcache->hash[bucket], blkcache_item, hashq);

    blkcache->num_data_blocks++;
    blkcache->bytes_used += size;

    __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);

#if BLKCACHE_TRACE == 1
    __wt_verbose(session, WT_VERB_BLKCACHE, "memory write latency: "
		 "offset=%" PRIuMAX ", size=%" PRIu32 ", hash=%" PRIu64 ", "
		 "latency=%" PRIu64 " ns.",
		 (uintmax_t)offset, (uint32_t)size, hash,
		 WT_CLOCKDIFF_NS(time_stop, time_start));
#endif

    WT_STAT_CONN_INCRV(session, block_cache_bytes, size);
    WT_STAT_CONN_INCR(session, block_cache_blocks);
    if (write) {
	WT_STAT_CONN_INCRV(session, block_cache_bytes_insert_write, size);
	WT_STAT_CONN_INCR(session, block_cache_blocks_insert_write);
    }
    else {
	WT_STAT_CONN_INCRV(session, block_cache_bytes_insert_read, size);
	WT_STAT_CONN_INCR(session, block_cache_blocks_insert_read);
    }

    __wt_verbose(session, WT_VERB_BLKCACHE, "block inserted in cache: "
		 "offset=%" PRIuMAX ", size=%" PRIu32 ", "
		 "checksum=%" PRIu32 ", hash=%" PRIu64,
		 (uintmax_t)offset, (uint32_t)size, checksum, hash);
    return (0);
item_exists:
    if (write) {
	memcpy(blkcache_item->data, data, size);
	WT_STAT_CONN_INCRV(session, block_cache_bytes_update, size);
	WT_STAT_CONN_INCR(session, block_cache_blocks_update);
    }
    __wt_verbose(session, WT_VERB_BLKCACHE, "block exists during put: "
		 "offset=%" PRIuMAX ", size=%" PRIu32 ", "
		 "checksum=%" PRIu32 ", hash=%" PRIu64,
		 (uintmax_t)offset, (uint32_t)size, checksum, hash);

err:
    __blkcache_free(session, data_ptr);
    __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
    return (ret);
}

/*
 * __wt_blkcache_remove --
 *     Remove a block from the cache.
 */
void
__wt_blkcache_remove(WT_SESSION_IMPL *session, wt_off_t offset, size_t size, uint32_t checksum)
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

    id.checksum = (uint64_t)checksum;
    id.offset = (uint64_t)offset;
    id.size = (uint64_t)size;
    hash = __wt_hash_city64(&id, sizeof(id));

    bucket = hash % blkcache->hash_size;
    __wt_spin_lock(session, &blkcache->hash_locks[bucket]);
    TAILQ_FOREACH (blkcache_item, &blkcache->hash[bucket], hashq) {
        if ((ret = memcmp(&blkcache_item->id, &id, sizeof(WT_BLKCACHE_ID))) == 0) {
            TAILQ_REMOVE(&blkcache->hash[bucket], blkcache_item, hashq);
            blkcache->num_data_blocks--;
            blkcache->bytes_used -= size;
            __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
            __blkcache_free(session, blkcache_item->data);
            __wt_overwrite_and_free(session, blkcache_item);
            WT_STAT_CONN_DECRV(session, block_cache_bytes, size);
            WT_STAT_CONN_DECR(session, block_cache_blocks);
	    WT_STAT_CONN_INCR(session, block_cache_blocks_removed);
	    __wt_verbose(session, WT_VERB_BLKCACHE, "block removed from cache: "
			 "offset=%" PRIuMAX ", size=%" PRIu32 ", "
			 "checksum=%" PRIu32 ", hash=%" PRIu64,
			 (uintmax_t)offset, (uint32_t)size, checksum, hash);
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
__blkcache_init(WT_SESSION_IMPL *session, size_t size, size_t hash_size,
		   int type, char *nvram_device_path, size_t system_ram,
		   int percent_file_in_dram)
{
    WT_BLKCACHE *blkcache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t i;

    conn = S2C(session);
    blkcache = &conn->blkcache;
    blkcache->hash_size = hash_size;
    blkcache->system_ram = system_ram;
    blkcache->fraction_in_dram = (float)percent_file_in_dram / 100;

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

    WT_RET(__wt_calloc_def(session, blkcache->hash_size, &blkcache->hash));
    WT_RET(__wt_calloc_def(session, blkcache->hash_size, &blkcache->hash_locks));

    for (i = 0; i < blkcache->hash_size; i++) {
        TAILQ_INIT(&blkcache->hash[i]); /* Block cache hash lists */
        WT_RET(__wt_spin_init(session, &blkcache->hash_locks[i], "block cache bucket locks"));
    }

    blkcache->type = type;
    blkcache->max_bytes = size;

    __wt_verbose(session, WT_VERB_BLKCACHE, "block cache initialized: "
		 "type=%s, size=%" PRIu32 " path=%s",
		 (type == BLKCACHE_NVRAM)?"nvram":(type == BLKCACHE_DRAM)?"dram":"unconfigured",
		 (uint32_t)size, (nvram_device_path == NULL)?"--":nvram_device_path);

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
    uint64_t i;

    conn = S2C(session);
    blkcache = &conn->blkcache;
    blkcache_item = NULL;

    __wt_verbose(session, WT_VERB_BLKCACHE, "block cache with %" PRIu32
		 " bytes used to be destroyed", (uint32_t)blkcache->bytes_used);

    if (blkcache->bytes_used == 0)
        goto done;

    for (i = 0; i < blkcache->hash_size; i++) {
        __wt_spin_lock(session, &blkcache->hash_locks[i]);
        while (!TAILQ_EMPTY(&blkcache->hash[i])) {
            blkcache_item = TAILQ_FIRST(&blkcache->hash[i]);
            TAILQ_REMOVE(&blkcache->hash[i], blkcache_item, hashq);
            __blkcache_free(session, blkcache_item->data);
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
    __wt_free(session, blkcache->hash);
    __wt_free(session, blkcache->hash_locks);
    /*
     * Zeroing the structure has the effect of setting the block cache type to unconfigured.
     */
    memset((void *)blkcache, 0, sizeof(WT_BLKCACHE));
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
    int cache_type = BLKCACHE_UNCONFIGURED, percent_file_in_dram;
    size_t cache_size, hash_size, system_ram;

    conn = S2C(session);
    blkcache = &conn->blkcache;
    cache_size = hash_size = 0;

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

    WT_RET(__wt_config_gets(session, cfg, "block_cache.hashsize", &cval));
    if ((hash_size = (uint64_t)cval.val) == 0)
	hash_size = BLKCACHE_HASHSIZE_DEFAULT;
    else if (hash_size < BLKCACHE_HASHSIZE_MIN || hash_size > BLKCACHE_HASHSIZE_MAX)
        WT_RET_MSG(session, EINVAL, "block cache hash size must be between %d and %d entries",
		   BLKCACHE_HASHSIZE_MIN, BLKCACHE_HASHSIZE_MAX);

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

    WT_RET(__wt_config_gets(session, cfg, "block_cache.system_ram", &cval));
    system_ram = (uint64_t)cval.val;

    WT_RET(__wt_config_gets(session, cfg, "block_cache.percent_file_in_dram", &cval));
    if ((percent_file_in_dram = (int)cval.val) == 0)
	percent_file_in_dram =  BLKCACHE_PERCENT_FILE_IN_DRAM;

    return __blkcache_init(session, cache_size, hash_size, cache_type,
			      nvram_device_path, system_ram, percent_file_in_dram);
}
