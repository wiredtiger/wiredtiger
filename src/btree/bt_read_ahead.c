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
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_REF *next_ref;
    uint64_t block_preload;

    conn = S2C(session);
    block_preload = 0;

    WT_ASSERT_ALWAYS(session, F_ISSET(ref, WT_REF_FLAG_LEAF),
      "Read ahead starts with a leaf page and reviews the parent");

    WT_ASSERT_ALWAYS(session, __wt_session_gen(session, WT_GEN_SPLIT) != 0,
      "Read ahead requires a split generation to traverse internal page(s)");

    session->read_ahead_prev_ref = ref;
    /* Load and decompress a set of pages into the block cache. */
    WT_INTL_FOREACH_BEGIN (session, ref->home, next_ref) {
        /* Don't let the read ahead queue get overwhelmed. */
        if (conn->read_ahead_queue_count > WT_MAX_READ_AHEAD_QUEUE ||
          block_preload > WT_READ_AHEAD_QUEUE_PER_TRIGGER)
            break;

        /*
         * Skip queuing pages that are already in cache or are internal. They aren't the pages we
         * are looking for. This pretty much assumes that all children of an internal page remain in
         * cache during the scan. If a previous read-ahead of this internal page read a page in,
         * then that page was evicted and now a future page wants to be read ahead, this algorithm
         * needs a tweak. It would need to remember which child was last queued and start again from
         * there, rather than this approximation which assumes recently read ahead pages are still
         * in cache.
         */
        if (next_ref->state == WT_REF_DISK && F_ISSET(next_ref, WT_REF_FLAG_LEAF)) {
            ret = __wt_conn_read_ahead_queue_push(session, next_ref);
            if (ret == 0)
                ++block_preload;
            else if (ret != EBUSY)
                WT_RET(ret);
        }
    }
    WT_INTL_FOREACH_END;

    WT_STAT_CONN_INCRV(session, block_read_ahead_pages_queued, block_preload);
    return (0);
}

/*
 * __wt_read_ahead_page_in --
 *     Does the heavy lifting of reading a page into the cache. Immediately releases the page since
 *     reading it in is the useful side effect here. Must be called while holding a dhandle.
 */
int
__wt_read_ahead_page_in(WT_SESSION_IMPL *session, WT_READ_AHEAD *ra)
{
    WT_ADDR_COPY addr;

    WT_ASSERT_ALWAYS(
      session, ra->ref->home == ra->first_home, "The home changed while queued for read ahead");
    WT_ASSERT_ALWAYS(session, ra->dhandle != NULL, "Read ahead needs to save a valid dhandle");
    WT_ASSERT_ALWAYS(
      session, !F_ISSET(ra->ref, WT_REF_FLAG_INTERNAL), "Read ahead should only see leaf pages");

    if (ra->ref->state != WT_REF_DISK) {
        WT_STAT_CONN_INCR(session, block_read_ahead_pages_fail);
        return (0);
    }

    WT_STAT_CONN_INCR(session, block_read_ahead_pages_read);

    if (__wt_ref_addr_copy(session, ra->ref, &addr)) {
        WT_RET(__wt_page_in(session, ra->ref, WT_READ_PREFETCH));
        WT_RET(__wt_page_release(session, ra->ref, 0));
    }

    return (0);
}
