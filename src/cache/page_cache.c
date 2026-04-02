/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wti_page_cache_init --
 *     Initialize the page cache.
 */
int
__wti_page_cache_init(WT_SESSION_IMPL *session, u_int hash_size)
{
    WT_DECL_RET;
    WT_PAGE_CACHE *page_cache;
    uint64_t i;

    page_cache = &S2C(session)->cache->page_cache;
    page_cache->hash_size = hash_size;
    page_cache->hash_lock_size = WT_MIN(hash_size, WT_PAGE_CACHE_MAX_LOCKS);
    page_cache->max_bucket_size = 0;
    page_cache->max_ref_count = 0;

    WT_ERR(__wt_calloc_def(session, page_cache->hash_size, &page_cache->hash));
    WT_ERR(__wt_calloc_def(session, page_cache->hash_lock_size, &page_cache->hash_locks));

    for (i = 0; i < page_cache->hash_size; i++)
        TAILQ_INIT(&page_cache->hash[i]);
    for (i = 0; i < page_cache->hash_lock_size; i++)
        WT_ERR(__wt_spin_init(session, &page_cache->hash_locks[i], "page cache bucket locks"));

    return (0);

err:
    __wti_page_cache_destroy(session);
    return (ret);
}

/*
 * __wti_page_cache_destroy --
 *     Destroy the page cache and free all memory.
 */
void
__wti_page_cache_destroy(WT_SESSION_IMPL *session)
{
    WT_PAGE_CACHE *page_cache;
    WT_PAGE_CACHE_ITEM *page_cache_item;
    uint64_t i;

    page_cache = &S2C(session)->cache->page_cache;

    if (page_cache->hash == NULL || page_cache->hash_locks == NULL)
        goto done;

    /* If the page cache was initialized, we should be a disaggregated standby node. */
    WT_ASSERT(session, __wt_conn_is_disagg(session) && !S2C(session)->layered_table_manager.leader);

    for (i = 0; i < page_cache->hash_size; i++) {
        while (!TAILQ_EMPTY(&page_cache->hash[i])) {
            page_cache_item = TAILQ_FIRST(&page_cache->hash[i]);
            TAILQ_REMOVE(&page_cache->hash[i], page_cache_item, hashq);
            __wt_free(session, page_cache_item->data);
            __wt_free(session, page_cache_item);
        }
    }
    for (i = 0; i < page_cache->hash_lock_size; i++)
        __wt_spin_destroy(session, &page_cache->hash_locks[i]);

done:
    __wt_free(session, page_cache->hash);
    __wt_free(session, page_cache->hash_locks);
}
