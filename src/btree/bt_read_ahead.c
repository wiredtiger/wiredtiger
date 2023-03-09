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
    WT_BTREE *btree;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_READAHEAD *ra;
    WT_REF *next_ref;
    uint64_t block_preload;

    btree = S2BT(session);
    conn = S2C(session);
    block_preload = 0;

    /*
     * TODO: Support read-ahead for column stores. Hopefully this is free, but I don't want to think
     * about it just yet.
     */
    if (btree->type != BTREE_ROW)
        return (0);

    WT_ASSERT_ALWAYS(session, F_ISSET(ref, WT_REF_FLAG_LEAF),
            "Read ahead starts with a leaf page and reviews the parent");

    /*
    fprintf(stderr, "Doing read ahead from %p in parent page %p\n", ref, ref);
    */
    WT_ASSERT_ALWAYS(session, __wt_session_gen(session, WT_GEN_SPLIT) != 0,
            "Read ahead requires a split generation to traverse internal page(s)");

    session->readahead_prev_ref = ref;
    /* Load and decompress a set of pages into the block cache. */
    WT_INTL_FOREACH_BEGIN (session, ref->home, next_ref)
        /*
        fprintf(stderr, "\tref %p, state %s, type %s\n", next_ref,
          __wt_debug_ref_state(next_ref->state),
          F_ISSET(next_ref, WT_REF_FLAG_INTERNAL) ? "internal" : "leaf");
          */
        /* Don't let the read ahead queue get overwhelmed. */
        if (conn->read_ahead_queue_count > WT_MAX_READ_AHEAD_QUEUE)
            break;

        /*
         * Skip queuing pages that are already in cache or are internal. They aren't the pages
         * we are looking for.
         */ 
        if (next_ref->state == WT_REF_DISK && F_ISSET(next_ref, WT_REF_FLAG_LEAF)) {
            WT_RET(__wt_calloc_one(session, &ra));
            WT_ASSERT(session, next_ref->home == ref->home);
            ++next_ref->home->refcount;
            ra->ref = next_ref;
            ra->first_home = next_ref->home;
            ra->dhandle = session->dhandle;
            __wt_spin_lock(session, &conn->readahead_lock);
            TAILQ_INSERT_TAIL(&conn->raqh, ra, q);
            ++conn->read_ahead_queue_count;
            __wt_spin_unlock(session, &conn->readahead_lock);
            ++block_preload;
        }
    WT_INTL_FOREACH_END;

    WT_STAT_CONN_INCRV(session, block_readahead_pages_queued, block_preload);
    return (ret);
}
