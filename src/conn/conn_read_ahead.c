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

    WT_RET(__wt_thread_group_create(session, &conn->read_ahead_threads, "read-ahead-server", 20, 20,
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
    bool locked;

    WT_UNUSED(thread);

    WT_ASSERT(session, session->id != 0);

    conn = S2C(session);
    locked = false;
    WT_RET(__wt_scr_alloc(session, 0, &tmp));

    while (F_ISSET(conn, WT_CONN_READ_AHEAD_RUN)) {
        __wt_spin_lock(session, &conn->read_ahead_lock);
        locked = true;
        ra = TAILQ_FIRST(&conn->raqh);

        /* If there is no work for the thread to do - return back to the thread pool */
        if (ra == NULL)
            break;

        TAILQ_REMOVE(&conn->raqh, ra, q);
        --conn->read_ahead_queue_count;
        WT_ASSERT_ALWAYS(session, F_ISSET(ra->ref, WT_REF_FLAG_READ_AHEAD),
          "Any ref on the read ahead queue needs to have the read ahead flag set");
        __wt_spin_unlock(session, &conn->read_ahead_lock);
        locked = false;

        WT_WITH_DHANDLE(session, ra->dhandle, ret = __wt_read_ahead_page_in(session, ra));
        /*
         * It probably isn't strictly necessary to re-acquire the lock to reset the flag, but other
         * flag accesses do need to lock, so it's better to be consistent.
         */
        __wt_spin_lock(session, &conn->read_ahead_lock);
        F_CLR(ra->ref, WT_REF_FLAG_READ_AHEAD);
        __wt_spin_unlock(session, &conn->read_ahead_lock);
        WT_ERR(ret);

        __wt_free(session, ra);
    }

err:
    if (locked)
        __wt_spin_unlock(session, &conn->read_ahead_lock);
    __wt_scr_free(session, &tmp);
    return (ret);
}

/*
 * __wt_conn_read_ahead_queue_push --
 *     Push a ref onto the read ahead queue.
 */
int
__wt_conn_read_ahead_queue_push(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;
    WT_READ_AHEAD *ra;

    conn = S2C(session);

    WT_RET(__wt_calloc_one(session, &ra));
    ra->ref = ref;
    ra->first_home = ref->home;
    ra->dhandle = session->dhandle;
    __wt_spin_lock(session, &conn->read_ahead_lock);
    if (F_ISSET(ref, WT_REF_FLAG_READ_AHEAD))
        ret = EBUSY;
    else {
        F_SET(ref, WT_REF_FLAG_READ_AHEAD);
        TAILQ_INSERT_TAIL(&conn->raqh, ra, q);
        ++conn->read_ahead_queue_count;
    }
    __wt_spin_unlock(session, &conn->read_ahead_lock);

    if (ret != 0)
        __wt_free(session, ra);
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
