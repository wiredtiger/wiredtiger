/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_btree_read_ahead --
 *     Pre-load a set of pages into the cache. This session holds a hazard pointer on the ref passed
 *     in, so there must be a valid page and a valid parent page (though that parent could change if
 *     a split happens).
 */
int
__wt_btree_read_ahead(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_ADDR_COPY addr;
    WT_BTREE *btree;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_REF *next_ref;
    uint64_t block_preload;

    btree = S2BT(session);
    block_preload = 0;

    /*
     * TODO: Support read-ahead for column stores. Hopefully this is free, but I don't want to think
     * about it just yet.
     */
    if (btree->type != BTREE_ROW)
        return (0);

    /*
     * TODO: Does the actual reading need to be out-of-band (i.e done in another thread?). I'd
     * rather not need to queue/pop, and have a utility thread. OTOH: We don't really need an
     * asynchronous mechanism - there is already a mechanism to ensure only a single thread reads
     * a page into cache.
     */
    WT_RET(__wt_scr_alloc(session, 0, &tmp));
    /*
     * This requires a split generation be held. It usually seems to already have one.
     * __wt_session_gen_enter(session, WT_GEN_SPLIT);
     */

    /* Load and decompress a set of pages into the block cache. */
    WT_INTL_FOREACH_BEGIN (session, ref->home, next_ref)
        /*
         * Only pre-fetch pages that aren't already in the cache, this is imprecise (the state could
         * change), but it doesn't matter. It would just fetch the same block twice.
         *
         * TODO we can probably get rid of the ref_addr_copy - will we push too much to the queue?
         */
        if (ref->state == WT_REF_DISK && __wt_ref_addr_copy(session, next_ref, &addr)) {
            ++ref->home->refcount;
            struct __wt_readahead *ra = malloc(sizeof(struct __wt_readahead));
            ra->ref = ref;
            ra->session = session;
            TAILQ_INSERT_TAIL(&S2C(session)->raqh, ra, q);
            ++block_preload;
        }
    WT_INTL_FOREACH_END;

    /*__wt_session_gen_leave(session, WT_GEN_SPLIT);*/
    __wt_scr_free(session, &tmp);

    WT_STAT_CONN_INCRV(session, block_readahead_pages, block_preload);
    return (ret);
}
