/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __wt_read_ahead_create --
 *     Start the read_ahead server.
 */
int
__wt_read_ahead_create(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    F_SET(conn, WT_CONN_READ_AHEAD_RUN);

    WT_RET(__wt_thread_group_create(session, &conn->read_ahead_threads, "read-ahead-server", 9, 9,
      0, __wt_read_ahead_thread_chk, __wt_read_ahead_thread_run, NULL));

    return (0);
}

/*
 * __wt_read_ahead_thread_chk --
 *     Check to decide if the read_ahead thread should continue running.
 */
bool
__wt_read_ahead_thread_chk(WT_SESSION_IMPL *session)
{
    return (F_ISSET(S2C(session), WT_CONN_READ_AHEAD_RUN));
}

/*
 * __read_ahead_page_in --
 *     Does the heavy lifting of reading a page into the cache. Immediately releases the page since
 *     reading it in is the useful side effect here. Must be called while holding a dhandle.
 */
static int
__read_ahead_page_in(WT_SESSION_IMPL *session, WT_READ_AHEAD *ra)
{
    WT_ADDR_COPY addr;

    WT_ASSERT_ALWAYS(
      session, ra->ref->home == ra->first_home, "The home changed while queued for read ahead");
    WT_ASSERT_ALWAYS(session, ra->dhandle != NULL, "Read ahead needs to save a valid dhandle");
    WT_ASSERT_ALWAYS(
      session, !F_ISSET(ra->ref, WT_REF_FLAG_INTERNAL), "Read ahead should only see leaf pages");

    if (ra->ref->state == WT_REF_DISK)
        WT_STAT_CONN_INCR(session, block_read_ahead_pages_read);

    if (__wt_ref_addr_copy(session, ra->ref, &addr)) {
        WT_RET(__wt_page_in(session, ra->ref, WT_READ_PREFETCH));
        WT_RET(__wt_page_release(session, ra->ref, 0));
    }

    return (0);
}

/*
 * __wt_read_ahead_thread_run --
 *     Entry function for a read_ahead thread. This is called repeatedly from the thread group code
 *     so it does not need to loop itself.
 */
int
__wt_read_ahead_thread_run(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_ITEM(tmp);
    WT_DECL_RET;
    WT_READ_AHEAD *ra;

    WT_UNUSED(thread);

    WT_ASSERT(session, session->id != 0);

    conn = S2C(session);
    WT_RET(__wt_scr_alloc(session, 0, &tmp));

    while (F_ISSET(conn, WT_CONN_READ_AHEAD_RUN)) {
        __wt_spin_lock(session, &conn->read_ahead_lock);
        ra = TAILQ_FIRST(&conn->raqh);
        if (ra == NULL) {
            __wt_spin_unlock(session, &conn->read_ahead_lock);
            break;
        }
        TAILQ_REMOVE(&conn->raqh, ra, q);
        --conn->read_ahead_queue_count;
        __wt_spin_unlock(session, &conn->read_ahead_lock);

        WT_WITH_DHANDLE(session, ra->dhandle, ret = __read_ahead_page_in(session, ra));
        WT_ERR(ret);

        --ra->ref->home->refcount;
        __wt_free(session, ra);
    }

err:
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __wt_read_ahead_destroy --
 *     Destroy the read_ahead threads.
 */
int
__wt_read_ahead_destroy(WT_SESSION_IMPL *session)
{
    F_CLR(S2C(session), WT_CONN_READ_AHEAD_RUN);

    __wt_writelock(session, &S2C(session)->read_ahead_threads.lock);

    WT_RET(__wt_thread_group_destroy(session, &S2C(session)->read_ahead_threads));

    return (0);
}
