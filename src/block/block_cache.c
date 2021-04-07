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
            return WT_BLKCACHE_FULL;
#else
        WT_RET_MSG(session, EINVAL, "NVRAM block cache type requires libmemkind.");
#endif
    }
    return (0);
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
 * __blkcache_update_ref_histogram --
 *      Update the histogram of block accesses when the block is freed or on exit.
 */
static void
__blkcache_update_ref_histogram(
    WT_SESSION_IMPL *session, WT_BLKCACHE_ITEM *blkcache_item, int type)
{
    WT_BLKCACHE *blkcache;
    WT_CONNECTION_IMPL *conn;
    unsigned int bucket;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    bucket = blkcache_item->num_references / BLKCACHE_HIST_BOUNDARY;
    if (bucket > BLKCACHE_HIST_BUCKETS - 1)
	bucket =  BLKCACHE_HIST_BUCKETS - 1;

    blkcache->cache_references[bucket]++;

    if (type == BLKCACHE_RM_FREE)
	blkcache->cache_references_removed_blocks[bucket]++;
    else if (type == BLKCACHE_RM_EVICTION)
	blkcache->cache_references_evicted_blocks[bucket]++;
}

static void
__blkcache_print_reference_hist(const char *header, uint32_t *hist)
{
    int j;

    printf("%s:\n", header);
    printf("Reuses \t Number of blocks \n");
    printf("-----------------------------\n");
    for (j = 0; j < BLKCACHE_HIST_BUCKETS; j++) {
	printf("[%d - %d] \t %d \n", j * BLKCACHE_HIST_BOUNDARY,
	       (j + 1) * BLKCACHE_HIST_BOUNDARY, hist[j]);
    }
    printf("\n");
}

static inline bool
__blkcache_high_overhead(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    WT_BLKCACHE *blkcache;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    if ((double)(blkcache->inserts + blkcache->removals)/
	(double)(blkcache->lookups) > blkcache->overhead_pct)
	return true;

    return false;
}

static bool
__blkcache_should_evict(WT_SESSION_IMPL *session, WT_BLKCACHE_ITEM *blkcache_item)
{
    WT_CONNECTION_IMPL *conn;
    WT_BLKCACHE *blkcache;

    conn = S2C(session);
    blkcache = &conn->blkcache;

    /* Don't evict if there is plenty of free space */
    if ((double)blkcache->bytes_used/(double)blkcache->max_bytes < blkcache->full_target)
	return false;
    /* Don't evict if there is high overhead due to blocks being
     * inserted/removed. Churn kills performance and evicting
     * when churn is high will exacerbate the overhead.
     */
    if (__blkcache_high_overhead(session)) {
	WT_STAT_CONN_INCR(session, block_cache_not_evicted_overhead);
	return false;
    }
    if (blkcache_item->freq_rec_counter < blkcache->evict_aggressive)
	return true;
    else
	return false;
}

/*
 * __blkcache_eviction_thread
 *     Periodically sweep the cache and evict unused blocks.
 */
static WT_THREAD_RET
__blkcache_eviction_thread(void *arg)
{
    WT_BLKCACHE *blkcache;
    WT_CONNECTION_IMPL *conn;
    WT_BLKCACHE_ITEM *blkcache_item, *blkcache_item_tmp;
    WT_SESSION_IMPL *session;
    int i;

    session = (WT_SESSION_IMPL *)arg;
    conn = S2C(session);
    blkcache = &conn->blkcache;

    printf("Block cache eviction thread starting... Aggressive target = %d, "
	   "full target = %f.\n", blkcache->evict_aggressive,
	   blkcache->full_target);

    while (!blkcache->blkcache_exiting) {

	/* Sweep the cache every second to ensure time-based decay of
	 * frequency/recency counters of resident blocks.
	 */
	__wt_cond_wait(session, blkcache->blkcache_cond, WT_MILLION, NULL);

	/* Check if we were awaken because the cache is being destroyed */
	if (blkcache->blkcache_exiting)
	    return (0);

	/*
	 * Walk the cache, gathering statistics and evicting blocks
	 * that are within our target. We sweep the cache every second,
	 * decrementing the frequency/recency counter of each block.
	 * Blocks whose counter goes below the threshold will get
	 * evicted. The threshold is set according to how soon
	 * we expect the blocks to become irrelevant. For example,
	 * if the threshold is set to 1800 seconds (=30 minutes),
	 * blocks that were used once but then weren't referenced for
	 * 30 minutes will be evicted. Blocks that were referenced
	 * a lot in the past but weren't referenced in the past 30
	 * minutes will stay in the cache a bit longer, until their
	 * frequency/recency counter drops below the threshold.
	 */
	for (i = 0; i < (int)blkcache->hash_size; i++) {
	    __wt_spin_lock(session, &blkcache->hash_locks[i]);
	    TAILQ_FOREACH_SAFE(blkcache_item, &blkcache->hash[i], hashq, blkcache_item_tmp) {
		if (__blkcache_should_evict(session, blkcache_item)) {
		    TAILQ_REMOVE(&blkcache->hash[i], blkcache_item, hashq);
		    __blkcache_free(session, blkcache_item->data);
		    __blkcache_update_ref_histogram(session, blkcache_item, BLKCACHE_RM_EVICTION);
		    blkcache->num_data_blocks--;
		    blkcache->bytes_used -= blkcache_item->id.size;

                    /* Update the number of removals because it is used to
		     * estimate the overhead, and we want the overhead
		     * contributed by eviction to be part of that calculation.
		     */
		    blkcache->removals++;

		    WT_STAT_CONN_INCR(session, block_cache_blocks_evicted);
		    WT_STAT_CONN_DECRV(session, block_cache_bytes, blkcache_item->id.size);
		    WT_STAT_CONN_DECR(session, block_cache_blocks);
		    __wt_free(session, blkcache_item);
		}
		else
		    blkcache_item->freq_rec_counter--;
	    }
	    __wt_spin_unlock(session, &blkcache->hash_locks[i]);
	    if (blkcache->blkcache_exiting)
		return (0);
	}
	WT_STAT_CONN_INCR(session, block_cache_eviction_passes);
    }
    return (0);
}

/*
 * Every so often, compute the total size of the files open
 * in the block manager.
 */
#define BLKCACHE_FILESIZE_EST_FREQ 5000

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

    WT_STAT_CONN_SET(session, block_cache_bypass_filesize,
		     blkcache->estimated_file_size);

    return blkcache->estimated_file_size;
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
    blkcache->lookups++;

    /* If more than the configured fraction of the file is likely
     * to fit in the buffer cache, don't use the cache.
     */
    if (blkcache->system_ram >=
	__blkcache_estimate_filesize(session) * blkcache->fraction_in_dram) {
	WT_STAT_CONN_INCR(session, block_cache_bypass_get);
	return WT_BLKCACHE_BYPASS;
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

	    blkcache_item->num_references++;
	    if (blkcache_item->freq_rec_counter < 0)
		blkcache_item->freq_rec_counter = 0;
	    blkcache_item->freq_rec_counter++;

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

    /* Bypass on write if the no-write-allocate setting is on */
    if (write && blkcache->write_allocate == false) {
	WT_STAT_CONN_INCR(session, block_cache_bypass_writealloc);
	return -1;
    }

    /* Are we within cache size limits? */
    if (blkcache->bytes_used >= blkcache->max_bytes)
        return WT_BLKCACHE_FULL;

    /* If more than the configured fraction of the file is likely
     * to fit in the buffer cache, don't use the cache.
     */
    if (blkcache->system_ram >=
	__blkcache_estimate_filesize(session) * blkcache->fraction_in_dram) {
	WT_STAT_CONN_INCR(session, block_cache_bypass_put);
	return WT_BLKCACHE_BYPASS;
    }

    /* Bypass on high overhead */
    if (__blkcache_high_overhead(session) == true) {
	WT_STAT_CONN_INCR(session, block_cache_bypass_overhead_put);
	return WT_BLKCACHE_BYPASS;
    }
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

    /*
     * Set the recency timestamp on newly inserted blocks to the
     * maximum value to reduce the chance of them being evicted before they
     * are reused.
     */
    blkcache_item->freq_rec_counter = 1;

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
    blkcache->inserts++;

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
    if (write == true) {
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
	    __blkcache_update_ref_histogram(session, blkcache_item, BLKCACHE_RM_FREE);
            __wt_spin_unlock(session, &blkcache->hash_locks[bucket]);
            __blkcache_free(session, blkcache_item->data);
            __wt_overwrite_and_free(session, blkcache_item);
            WT_STAT_CONN_DECRV(session, block_cache_bytes, size);
            WT_STAT_CONN_DECR(session, block_cache_blocks);
	    WT_STAT_CONN_INCR(session, block_cache_blocks_removed);
	    blkcache->removals++;
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
__blkcache_init(WT_SESSION_IMPL *session, size_t cache_size,
		size_t hash_size, uint type, char *nvram_device_path,
		size_t system_ram, uint percent_file_in_dram,
		bool write_allocate, double overhead_pct,
		bool eviction_on, uint evict_aggressive, double full_target)
{
    WT_BLKCACHE *blkcache;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    uint64_t i;

    conn = S2C(session);
    blkcache = &conn->blkcache;
    blkcache->hash_size = hash_size;
    blkcache->fraction_in_dram = (float)percent_file_in_dram / 100;
    blkcache->full_target = full_target;
    blkcache->max_bytes = cache_size;
    blkcache->overhead_pct = overhead_pct;
    blkcache->system_ram = system_ram;
    blkcache->write_allocate = write_allocate;

    printf("Block cache: fraction in dram: %f, full_target %f,"
	   "overhead percent: %f, evict_aggressive: %d\n",
	   (double)blkcache->fraction_in_dram, blkcache->full_target,
	   blkcache->overhead_pct, -evict_aggressive);

    if (type == BLKCACHE_NVRAM) {
#ifdef HAVE_LIBMEMKIND
        if ((ret = memkind_create_pmem(nvram_device_path, 0, &blkcache->pmem_kind)) != 0)
            WT_RET_MSG(session, ret, "block cache failed to initialize: memkind_create_pmem");

	WT_RET(__wt_strndup(
		   session, nvram_device_path, strlen(nvram_device_path), &blkcache->nvram_device_path));
	__wt_free(session, nvram_device_path);
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

    /* Create the eviction thread */
    if (eviction_on) {
	WT_RET(__wt_cond_alloc(session, "Block cache eviction", &blkcache->blkcache_cond));
	WT_RET(__wt_thread_create(session, &blkcache->evict_thread_tid,
				  __blkcache_eviction_thread, (void*)session));
	blkcache->eviction_on = true;
	blkcache->evict_aggressive = -((int)evict_aggressive);
    }

    blkcache->type = type;

    __wt_verbose(session, WT_VERB_BLKCACHE, "block cache initialized: "
		 "type=%s, size=%" PRIu32 " path=%s",
		 (type == BLKCACHE_NVRAM)?"nvram":(type == BLKCACHE_DRAM)?"dram":"unconfigured",
		 (uint32_t)cache_size, (nvram_device_path == NULL)?"--":nvram_device_path);

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
    WT_DECL_RET;
    uint64_t i;

    conn = S2C(session);
    blkcache = &conn->blkcache;
    blkcache_item = NULL;

    __wt_verbose(session, WT_VERB_BLKCACHE, "block cache with %" PRIu32
		 " bytes used to be destroyed", (uint32_t)blkcache->bytes_used);

    if (blkcache->type == BLKCACHE_UNCONFIGURED)
	return;

    if (blkcache->eviction_on) {
	blkcache->blkcache_exiting = true;
	__wt_cond_signal(session, blkcache->blkcache_cond);
	WT_TRET(__wt_thread_join(session, &blkcache->evict_thread_tid));
	printf("Block cache eviction thread exited...\n");
	__wt_cond_destroy(session, &blkcache->blkcache_cond);
    }

    if (blkcache->bytes_used == 0)
        goto done;

    for (i = 0; i < blkcache->hash_size; i++) {
        __wt_spin_lock(session, &blkcache->hash_locks[i]);
        while (!TAILQ_EMPTY(&blkcache->hash[i])) {
            blkcache_item = TAILQ_FIRST(&blkcache->hash[i]);
            TAILQ_REMOVE(&blkcache->hash[i], blkcache_item, hashq);
#if 0 /* Todo: remove this later. Some workloads crash on freeing arenas */
            __blkcache_free(session, blkcache_item->data);
#endif
	    __blkcache_update_ref_histogram(session, blkcache_item, BLKCACHE_RM_EXIT);
            blkcache->num_data_blocks--;
            blkcache->bytes_used -= blkcache_item->id.size;
            __wt_free(session, blkcache_item);
        }
        __wt_spin_unlock(session, &blkcache->hash_locks[i]);
    }
    WT_ASSERT(session, blkcache->bytes_used == blkcache->num_data_blocks == 0);

  done:
    /* Print reference histograms */
    __blkcache_print_reference_hist("All blocks", blkcache->cache_references);
    __blkcache_print_reference_hist("Removed blocks", blkcache->cache_references_removed_blocks);
    __blkcache_print_reference_hist("Evicted blocks", blkcache->cache_references_evicted_blocks);

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

    bool write_allocate, eviction_on;
    char *nvram_device_path;
    double overhead_pct, full_target;
    uint cache_type, evict_aggressive, percent_file_in_dram;
    size_t cache_size, hash_size, system_ram;

    conn = S2C(session);
    blkcache = &conn->blkcache;
    cache_type = BLKCACHE_UNCONFIGURED;
    cache_size = hash_size = 0;
    eviction_on = write_allocate = true;
    nvram_device_path = NULL;

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
    percent_file_in_dram = cval.val;

    WT_RET(__wt_config_gets(session, cfg, "block_cache.eviction_on", &cval));
    if (cval.val == 0)
	eviction_on = false;

    WT_RET(__wt_config_gets(session, cfg, "block_cache.eviction_aggression", &cval));
    evict_aggressive = cval.val;

    WT_RET(__wt_config_gets(session, cfg, "block_cache.full_target", &cval));
    full_target = (double)cval.val/(double)100;

    WT_RET(__wt_config_gets(session, cfg, "block_cache.write_allocate", &cval));
    if (cval.val == 0)
	write_allocate = false;

    WT_RET(__wt_config_gets(session, cfg, "block_cache.max_percent_overhead", &cval));
    overhead_pct = (double)cval.val/(double)100;

    return __blkcache_init(session, cache_size, hash_size, cache_type,
			   nvram_device_path, system_ram, percent_file_in_dram, write_allocate,
			   overhead_pct, eviction_on, evict_aggressive, full_target);
}
