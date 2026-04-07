/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wti_shared_dsk_init --
 *     Initialize the shared disk.
 */
int
__wti_shared_dsk_init(WT_SESSION_IMPL *session, u_int hash_size)
{
    WT_DECL_RET;
    WT_SHARED_DSK *shared_dsk;
    uint64_t i;

    shared_dsk = &S2C(session)->cache->shared_dsk;
    shared_dsk->hash_size = hash_size;
    /* FIXME-WT-17066: We should pick a right WT_SHARED_DSK_MAX_LOCKS. */
    shared_dsk->hash_lock_size = WT_MIN(hash_size, WT_SHARED_DSK_MAX_LOCKS);
    shared_dsk->max_bucket_size = 0;
    shared_dsk->max_ref_count = 0;

    WT_ERR(__wt_calloc_def(session, shared_dsk->hash_size, &shared_dsk->hash));
    WT_ERR(__wt_calloc_def(session, shared_dsk->hash_lock_size, &shared_dsk->hash_locks));

    for (i = 0; i < shared_dsk->hash_size; i++)
        TAILQ_INIT(&shared_dsk->hash[i]);
    for (i = 0; i < shared_dsk->hash_lock_size; i++)
        WT_ERR(__wt_spin_init(session, &shared_dsk->hash_locks[i], "shared disk bucket locks"));

    return (0);

err:
    __wti_shared_dsk_destroy(session);
    return (ret);
}

/*
 * __wti_shared_dsk_destroy --
 *     Destroy the shared disk and free all memory.
 */
void
__wti_shared_dsk_destroy(WT_SESSION_IMPL *session)
{
    WT_SHARED_DSK *shared_dsk;
    WT_SHARED_DSK_ITEM *shared_dsk_item;
    uint64_t i;

    shared_dsk = &S2C(session)->cache->shared_dsk;

    if (shared_dsk->hash == NULL || shared_dsk->hash_locks == NULL)
        goto done;

    /* If the shared disk was initialized, we should be a disaggregated standby node. */
    WT_ASSERT(session, __wt_conn_is_disagg(session) && !S2C(session)->layered_table_manager.leader);

    for (i = 0; i < shared_dsk->hash_size; i++) {
        while (!TAILQ_EMPTY(&shared_dsk->hash[i])) {
            shared_dsk_item = TAILQ_FIRST(&shared_dsk->hash[i]);
            TAILQ_REMOVE(&shared_dsk->hash[i], shared_dsk_item, hashq);
            __wt_free(session, shared_dsk_item->data);
            __wt_free(session, shared_dsk_item);
        }
    }
    for (i = 0; i < shared_dsk->hash_lock_size; i++)
        __wt_spin_destroy(session, &shared_dsk->hash_locks[i]);

done:
    __wt_free(session, shared_dsk->hash);
    __wt_free(session, shared_dsk->hash_locks);
}
