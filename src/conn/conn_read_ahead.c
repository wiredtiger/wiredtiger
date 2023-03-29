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
#if 0 /*                                                                                    \
       * We can get to the parent page, but not the parent ref, so it isn't simple to get a \
       * hazard pointer on the parent as we'd like.                                         \
       */
        /* Copy the parent ref to ensure the same hazard pointer is set and cleared. */
        WT_ORDERED_READ(parent_ref, ra->ref->home);
        /*
         * Acquire a hazard pointer on the parent of the leaf page we are about to read in. The
         * hazard pointer stops that internal page being evicted while the child is being read.
         */
        WT_ERR(__wt_hazard_set(session, parent_ref, &parent_hazard));
        __wt_spin_unlock(session, &conn->read_ahead_lock);
        locked = false;

        if (parent_hazard) {
            WT_WITH_DHANDLE(session, ra->dhandle, ret = __wt_read_ahead_page_in(session, ra));
            WT_ASSERT_ALWAYS(session, ra->ref->home == parent_ref,
                    "It isn't safe for the parent page to change while doing read ahead. If the "
                    "parent change, the new parent could have been evicted before this child was "
                    "read in.");
            WT_ERR(__wt_hazard_clear(session, parent_ref));
            WT_ERR(ret);
        }
#else
        __wt_spin_unlock(session, &conn->read_ahead_lock);
        locked = false;

        WT_WITH_DHANDLE(session, ra->dhandle, ret = __wt_read_ahead_page_in(session, ra));
        WT_ERR(ret);
#endif

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
    WT_READ_AHEAD *ra;

    conn = S2C(session);

    WT_RET(__wt_calloc_one(session, &ra));
    ra->ref = ref;
    ra->first_home = ref->home;
    ra->dhandle = session->dhandle;
    __wt_spin_lock(session, &conn->read_ahead_lock);
    TAILQ_INSERT_TAIL(&conn->raqh, ra, q);
    ++conn->read_ahead_queue_count;
    __wt_spin_unlock(session, &conn->read_ahead_lock);

    return (0);
}

/*
 * __wt_conn_read_ahead_queue_check --
 *     Check to see if a ref is present in the read ahead queue.
 */
bool
__wt_conn_read_ahead_queue_check(WT_SESSION_IMPL *session, WT_REF *check_ref)
{
    WT_CONNECTION_IMPL *conn;
    WT_READ_AHEAD *ra;

    conn = S2C(session);
    ra = NULL;

    __wt_spin_lock(session, &conn->read_ahead_lock);
    TAILQ_FOREACH (ra, &conn->raqh, q) {
        if (ra->ref == check_ref)
            break;
    }
    __wt_spin_unlock(session, &conn->read_ahead_lock);
    if (ra != NULL && ra->ref == check_ref)
        return (true);
    return (false);
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
