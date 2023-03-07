/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_readahead_create --
 *     Start the readahead server.
 */
int
__wt_readahead_create(WT_SESSION_IMPL *session)
{
    F_SET(session, WT_SESSION_READAHEAD_RUN);

    WT_RET(__wt_thread_group_create(session, &session->readahead_threads, "readahead-server",
        1, 1, 0, __wt_readahead_thread_chk, __wt_readahead_thread_run,
        __wt_readahead_thread_stop));

    session->readahead_running = true;

    return (0);
}

/*
 * __wt_readahead_thread_chk --
 *     Check to decide if the readahead thread should continue running.
 */
bool
__wt_readahead_thread_chk(WT_SESSION_IMPL *session)
{
    return (F_ISSET(session, WT_SESSION_READAHEAD_RUN));
}

/*
 * __wt_readahead_thread_run --
 *     Entry function for a readahead thread. This is called repeatedly from the thread group code
 *     so it does not need to loop itself.
 */
int
__wt_readahead_thread_run(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_REF *ref;

    WT_UNUSED(thread);

    WT_ORDERED_READ(ref, session->readahead_prev_ref);

    if (__wt_session_readahead_check(session, ref))
        WT_RET(__wt_btree_read_ahead(session, ref));

    return (0);
}

int
__wt_readahead_thread_stop(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_UNUSED(thread);
    F_CLR(session, WT_SESSION_READAHEAD_RUN);
    return (0);
}

/*
 * __wt_session_readahead_check --
 *     Check to see whether cursors owned by this session might benefit from doing read ahead
 */
bool
__wt_session_readahead_check(WT_SESSION_IMPL *session, WT_REF *ref)
{
    if (session->readahead_disk_read_count == 1)
        WT_STAT_CONN_INCR(session, block_readahead_disk_one);

    /* A single read from disk is common - don't use it to guide read ahead behavior. */
    if (session->readahead_disk_read_count < 2) {
        WT_STAT_CONN_INCR(session, block_readahead_skipped);
        return (false);
    }

    if (session->readahead_prev_ref == NULL) {
        WT_STAT_CONN_INCR(session, block_readahead_attempts);
        return (true);
    }

    WT_ASSERT_ALWAYS(session, session->readahead_prev_ref->state == WT_REF_MEM,
      "Any ref being used for read-ahead better already be in cache.");

    WT_ASSERT_ALWAYS(session, F_ISSET(session->readahead_prev_ref, WT_REF_FLAG_INTERNAL),
      "Any ref being used for read-ahead better reference an internal page");

    /*
     * If the previous read ahead was using the same home ref, it's already been pre-loaded. Note
     * that this heuristic probably needs to get more sophisticated - ideally it would preload a
     * number of pages, not necessarily all children of the current internal page.
     */
    if (session->readahead_prev_ref->page == ref->home) {
        WT_STAT_CONN_INCR(session, block_readahead_skipped);
        return (false);
    }

    WT_STAT_CONN_INCR(session, block_readahead_attempts);
    return (true);
}
