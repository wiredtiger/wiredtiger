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
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    F_SET(conn, WT_CONN_READAHEAD_RUN);

    WT_RET(__wt_thread_group_create(session, &conn->readahead_threads, "readahead-server", 1, 5, 0,
      __wt_readahead_thread_chk, __wt_readahead_thread_run, __wt_readahead_thread_stop));

    return (0);
}

/*
 * __wt_readahead_thread_chk --
 *     Check to decide if the readahead thread should continue running.
 */
bool
__wt_readahead_thread_chk(WT_SESSION_IMPL *session)
{
    return (F_ISSET(S2C(session), WT_CONN_READAHEAD_RUN));
}

/*
 * __readahead_page_in --
 *     Does the heavy lifting of reading a page into the cache. Immediately releases the page since
 *     reading it in is the useful side effect here. Must be called while holding a dhandle.
 */
static int
__readahead_page_in(WT_SESSION_IMPL *session, WT_READAHEAD *ra)
{
    WT_ADDR_COPY addr;

    WT_ASSERT_ALWAYS(
      session, ra->ref->home == ra->first_home, "The home changed while queued for read ahead");
    WT_ASSERT_ALWAYS(session, ra->ref->home->refcount > 0, "uh oh, ref count tracking is borked");
    WT_ASSERT_ALWAYS(session, ra->dhandle != NULL, "Read ahead needs to save a valid dhandle");

    if (__wt_ref_addr_copy(session, ra->ref, &addr)) {
        WT_RET(__wt_page_in(session, ra->ref, 0));
        WT_RET(__wt_page_release(session, ra->ref, 0));
    }

    return (0);
}

/*
 * __wt_readahead_thread_run --
 *     Entry function for a readahead thread. This is called repeatedly from the thread group code
 *     so it does not need to loop itself.
 */
int
__wt_readahead_thread_run(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_READAHEAD *ra;

    WT_UNUSED(thread);

    WT_ASSERT(session, session->id != 0);

    conn = S2C(session);
    WT_RET(__wt_scr_alloc(session, 0, &tmp));

    while (F_ISSET(conn, WT_CONN_READAHEAD_RUN)) {
        __wt_spin_lock(session, &conn->readahead_lock);
        ra = TAILQ_FIRST(&conn->raqh);
        if (ra == NULL) {
            __wt_spin_unlock(session, &conn->readahead_lock);
            break;
        }
        TAILQ_REMOVE(&conn->raqh, ra, q);
        __wt_spin_unlock(session, &conn->readahead_lock);

        WT_WITH_DHANDLE(session, ra->dhandle, ret = __readahead_page_in(session, ra));
        WT_ERR(ret);

        --ra->ref->home->refcount;
        __wt_free(session, ra);
    }

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __wt_readahead_destroy --
 *     Destroy the readahead threads.
 */
int
__wt_readahead_destroy(WT_SESSION_IMPL *session)
{
    F_CLR(S2C(session), WT_CONN_READAHEAD_RUN);

    __wt_writelock(session, &S2C(session)->readahead_threads.lock);

    WT_RET(__wt_thread_group_destroy(session, &S2C(session)->readahead_threads));

    return (0);
}

/*
 * __wt_readahead_thread_stop --
 *     Shutdown function for a readahead thread. TODO can we remove this?
 */
int
__wt_readahead_thread_stop(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_UNUSED(thread);
    WT_UNUSED(session);

    return (0);
}

/*
 * __wt_session_readahead_check --
 *     Check to see whether cursors owned by this session might benefit from doing read ahead
 */
bool
__wt_session_readahead_check(WT_SESSION_IMPL *session, WT_REF *ref)
{
    if (!S2C(session)->read_ahead_auto_on)
        return (false);

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

    /*
     * This seemed reasonable, but it's OK for a page to have been evicted after
     * it was used for read ahead (and I saw that)
     * WT_ASSERT_ALWAYS(session, session->readahead_prev_ref->state == WT_REF_MEM,
     *  "Any ref being used for read-ahead better already be in cache.");
     */

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
