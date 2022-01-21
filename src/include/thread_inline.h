/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * __wt_thread_cleanup --
 *     Cleanup up a stopped thread. Free its condition mutex and close its attached session.
 */
static inline int
__wt_thread_cleanup(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    /* The system thread must have joined at this point */
    WT_ASSERT(session, !thread->tid.created);

    __wt_cond_destroy(session, &thread->cond);
    thread->cond = NULL;

    if (thread->session != NULL) {
        WT_RET(__wt_session_close_internal(thread->session));
        thread->session = NULL;
    }

    return (0);
}

/*
 * __wt_thread_running --
 *     Return true if the thread is running.
 */
static inline bool
__wt_thread_running(WT_THREAD *thread)
{
    return thread->session != NULL;
}

/*
 * __wt_thread_start --
 *     Initialize and start up a thread.
 */
static inline int
__wt_thread_start(WT_CONNECTION_IMPL *conn, const char *session_name, bool open_metadata,
  uint32_t session_flags, const char *cond_name, uint32_t min, uint32_t max,
  WT_THREAD_RET (*thread_runner)(void *arg), WT_THREAD *threadp)
{
    if (__wt_thread_running(threadp))
        return (0);

    // FIXME WT-8696 - This usage of WT_THREAD is a bit leaky, we're not using any of the below
    // fields
    threadp->id = 0;
    threadp->flags = 0;
    threadp->chk_func = NULL;
    threadp->run_func = NULL;
    threadp->stop_func = NULL;

    WT_RET(__wt_open_internal_session(
      conn, session_name, open_metadata, session_flags, 0, &threadp->session));

    if (min != 0 && max != 0)
        WT_RET(__wt_cond_auto_alloc(threadp->session, cond_name, min, max, &threadp->cond));
    else
        WT_RET(__wt_cond_alloc(threadp->session, cond_name, &threadp->cond));

    WT_RET(__wt_thread_create(threadp->session, &threadp->tid, thread_runner, threadp->session));

    return (0);
}

/*
 * __wt_thread_stop --
 *     Stop a running thread.
 */
static inline int
__wt_thread_stop(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    if (__wt_thread_running(thread))
        if (thread->tid.created) {
            __wt_cond_signal(session, thread->cond);
            WT_RET(__wt_thread_join(session, &thread->tid));
        }
    return (0);
}

/*
 * __wt_thread_stop_and_cleanup --
 *     Stop and cleanup up a thread.
 */
static inline int
__wt_thread_stop_and_cleanup(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_DECL_RET;
    WT_TRET(__wt_thread_stop(session, thread));
    WT_TRET(__wt_thread_cleanup(session, thread));
    return ret;
}
