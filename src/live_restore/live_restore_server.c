/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"
#include "live_restore_private.h"

/*
 * __live_restore_worker_check --
 *     Thread groups cannot exist without a check function but in our case we don't use it due to it
 *     not meshing well with how we terminate threads. Given that, this function simply returns
 *     true.
 */
static bool
__live_restore_worker_check(WT_SESSION_IMPL *session)
{
    WT_UNUSED(session);
    return (true);
}

/*
 * __live_restore_work_queue_drain --
 *     Drain the work queue of any remaining items. This function does not need to take the queue
 *     lock but we do anyway because the semantic is if you touch the queue after it is initialized,
 *     you hold the lock.
 */
static void
__live_restore_work_queue_drain(WT_SESSION_IMPL *session)
{
    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;
    __wt_spin_lock(session, &server->queue_lock);
    if (!TAILQ_EMPTY(&server->work_queue)) {
        WT_LIVE_RESTORE_WORK_ITEM *work_item = NULL, *work_item_tmp = NULL;
        TAILQ_FOREACH_SAFE(work_item, &server->work_queue, q, work_item_tmp)
        {
            TAILQ_REMOVE(&server->work_queue, work_item, q);
            __wt_free(session, work_item->uri);
            __wt_free(session, work_item);
        }
    }
    WT_ASSERT(session, TAILQ_EMPTY(&server->work_queue));
    __wt_spin_unlock(session, &server->queue_lock);
}

/*
 * __live_restore_worker_run --
 *     Entry function for a live restore thread. This is called repeatedly from the thread group
 *     code so it does not need to loop itself.
 */
static int
__live_restore_worker_run(WT_SESSION_IMPL *session, WT_THREAD *ctx)
{
    WT_UNUSED(ctx);
    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;

    __wt_spin_lock(session, &server->queue_lock);
    if (TAILQ_EMPTY(&server->work_queue)) {
        /*
         * Because we depend on stopping threads to avoid decrementing the counter indefinitely this
         * section of code will both decrement the counter and terminate the thread. This is an
         * unusual usage of thread groups but avoids refactoring a LOT of other code for the time
         * being. The obvious fix would be to pass a WT_THREAD to the check function and then clear
         * the run flag, however this then modifies the definition of __wt_cond_wait_signal which is
         * used _everywhere_.
         */
        server->threads_working--;
        if (server->threads_working == 0) {
            /* All threads have finished their work, signal that source can be dropped. */
            WT_STAT_CONN_SET(session, live_restore_state, WT_LIVE_RESTORE_COMPLETE);
        }
        /* Kill ourselves. */
        F_CLR(ctx, WT_THREAD_RUN);
        __wt_verbose_debug2(session, WT_VERB_FILEOPS, "Live restore worker terminating%s", "!");
        __wt_spin_unlock(session, &server->queue_lock);
        return (0);
    }

    WT_LIVE_RESTORE_WORK_ITEM *work_item = NULL;

    work_item = TAILQ_FIRST(&server->work_queue);
    if (work_item == NULL) {
        __wt_spin_unlock(session, &server->queue_lock);
        return (0);
    }
    TAILQ_REMOVE(&server->work_queue, work_item, q);
    __wt_verbose_debug2(
      session, WT_VERB_FILEOPS, "Live restore worker taking queue item: %s", work_item->uri);
    __wt_spin_unlock(session, &server->queue_lock);

    WT_CURSOR *cursor;
    WT_SESSION *wt_session = (WT_SESSION *)session;

    /*
     * TODO: Add logic to differentiate log files, we can't open cursors on them. Can they be
     * deleted concurrently? It may be easier to have two separate queues.
     */
    /*
     * Open a cursor so no one can get exclusive access on the object. This prevents concurrent
     * schema operations like drop and rename.
     */
    WT_RET(wt_session->open_cursor(wt_session, work_item->uri, NULL, NULL, &cursor));

    /*
     * We need to get access to the WiredTiger file handle. Given we've opened the cursor we should
     * be able to access the WT_FH by first getting to its block manager and then the WT_FH.
     */
    WT_ASSERT(session, (CUR2BT(cursor)->bm)->is_multi_handle == false);
    /* This is a bit more than a little gross. */
    WT_FILE_HANDLE *fh = CUR2BT(cursor)->bm->block->fh->handle;
    /*
     * Call the fill holes function. Right now no other reads or writes can be occurring
     * concurrently.
     */
    __wt_verbose_debug2(
      session, WT_VERB_FILEOPS, "Live restore worker filling holes for: %s", work_item->uri);

    WT_DECL_RET;
    ret = __wti_live_restore_fs_fill_holes(fh, wt_session);
    WT_TRET(cursor->close(cursor));
    return (ret);
}

/*
 * __wt_live_restore_server_init --
 *     Start the worker threads and build the work queue.
 */
int
__wt_live_restore_server_init(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONFIG_ITEM cval;
    WT_RET(__wt_config_gets(session, cfg, "live_restore.threads_max", &cval));

    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;
    /*TODO: Add work queue population. */
    TAILQ_INIT(&server->work_queue);
    /*
     * Open a metadata cursor to gather the list of objects. The metadata file is built from the
     * WiredTiger.backup file, during __wt_turtle_init. Thus this function must be run after that
     * function. I don't know if we have a way of enforcing that.
     */
    WT_CURSOR *cursor;
    WT_RET(__wt_metadata_cursor(session, &cursor));
    WT_DECL_RET;
    WT_LIVE_RESTORE_WORK_ITEM *work_item = NULL;
    __wt_verbose_debug1(session, WT_VERB_FILEOPS, "Initializing the live restore work %s", "queue");
    while ((ret = cursor->next(cursor)) == 0) {
        const char *uri = NULL;
        WT_RET(cursor->get_key(cursor, &uri));
        if (WT_PREFIX_MATCH(uri, "file:")) {
            __wt_verbose_debug2(
              session, WT_VERB_FILEOPS, "Adding an item to the work queue %s", uri);
            WT_RET(__wt_calloc_one(session, &work_item));
            WT_ERR(__wt_strdup(session, uri, &work_item->uri));
            TAILQ_INSERT_HEAD(&server->work_queue, work_item, q);
        }
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    /* Set this value before the threads start up in case they immediately decrement it.*/
    server->threads_working = (uint32_t)cval.val;

    /* Create the thread group. */
    WT_ERR(__wt_thread_group_create(session, &server->threads, "live_restore_workers", 0, (uint32_t)cval.val,
      0, __live_restore_worker_check, __live_restore_worker_run, NULL));

    WT_STAT_CONN_SET(session, live_restore_state, WT_LIVE_RESTORE_IN_PROGRESS);

    if (0) {
err:
        if (work_item != NULL)
            __wt_free(session, work_item->uri);
        __wt_free(session, work_item);
        /* Should we unwind the full queue and free everything here? */
        server->threads_working = 0;
    }
    return (ret);
}

/*
 * __wt_live_restore_server_destroy --
 *     Destroy the live restore threads.
 */
int
__wt_live_restore_server_destroy(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;
    conn = S2C(session);

    if (!F_ISSET(conn, WT_CONN_LIVE_RESTORE))
        return (0);

    F_CLR(conn, WT_CONN_LIVE_RESTORE);
    /* Let any running threads finish up. */
    WT_LIVE_RESTORE_SERVER *server = &conn->live_restore_server;
    __wt_cond_signal(session, server->threads.wait_cond);

    __wt_writelock(session, &server->threads.lock);
    /* This call destroys the thread group lock. */
    WT_RET(__wt_thread_group_destroy(session, &server->threads));

    __live_restore_work_queue_drain(session);
    return (0);
}
