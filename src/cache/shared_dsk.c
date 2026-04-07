/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wti_shared_dsk_cache_init --
 *     Initialize the shared disk cache.
 */
int
__wti_shared_dsk_cache_init(WT_SESSION_IMPL *session, u_int hash_size)
{
    WT_DECL_RET;
    WT_SHARED_DSK_CACHE *shared_dsk_cache;
    uint64_t i;

    shared_dsk_cache = &S2C(session)->cache->shared_dsk_cache;
    shared_dsk_cache->hash_size = hash_size;
    /* FIXME-WT-17066: We should pick a WT_SHARED_DSK_CACHE_MAX_LOCKS wisely. */
    shared_dsk_cache->hash_lock_size = WT_MIN(hash_size, WT_SHARED_DSK_CACHE_MAX_LOCKS);
    shared_dsk_cache->max_bucket_size = 0;
    shared_dsk_cache->max_ref_count = 0;

    WT_ERR(__wt_calloc_def(session, shared_dsk_cache->hash_size, &shared_dsk_cache->hash));
    WT_ERR(
      __wt_calloc_def(session, shared_dsk_cache->hash_lock_size, &shared_dsk_cache->hash_locks));

    for (i = 0; i < shared_dsk_cache->hash_size; i++)
        TAILQ_INIT(&shared_dsk_cache->hash[i]);
    for (i = 0; i < shared_dsk_cache->hash_lock_size; i++)
        WT_ERR(__wt_spin_init(
          session, &shared_dsk_cache->hash_locks[i], "shared disk cache bucket locks"));

    shared_dsk_cache->enabled = true;
    return (0);

err:
    __wti_shared_dsk_cache_destroy(session);
    return (ret);
}

/*
 * __wti_shared_dsk_cache_destroy --
 *     Destroy the shared disk cache and free all memory.
 */
void
__wti_shared_dsk_cache_destroy(WT_SESSION_IMPL *session)
{
    WT_SHARED_DSK_CACHE *shared_dsk_cache;
    WT_SHARED_DSK_ITEM *shared_dsk_item;
    uint64_t i;

    shared_dsk_cache = &S2C(session)->cache->shared_dsk_cache;

    if (shared_dsk_cache->hash == NULL || shared_dsk_cache->hash_locks == NULL)
        goto done;

    /* If the shared disk cache was initialized, we should be a disaggregated standby node. */
    WT_ASSERT(session, __wt_conn_is_disagg(session) && !S2C(session)->layered_table_manager.leader);

    for (i = 0; i < shared_dsk_cache->hash_size; i++) {
        while (!TAILQ_EMPTY(&shared_dsk_cache->hash[i])) {
            shared_dsk_item = TAILQ_FIRST(&shared_dsk_cache->hash[i]);
            TAILQ_REMOVE(&shared_dsk_cache->hash[i], shared_dsk_item, hashq);
            __wt_free(session, shared_dsk_item->data);
            __wt_free(session, shared_dsk_item);
        }
    }
    for (i = 0; i < shared_dsk_cache->hash_lock_size; i++)
        __wt_spin_destroy(session, &shared_dsk_cache->hash_locks[i]);

done:
    __wt_free(session, shared_dsk_cache->hash);
    __wt_free(session, shared_dsk_cache->hash_locks);
}
