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
 *     Check whether a worker thread should still be running. Return true if so.
 */
static bool
__live_restore_worker_check(WT_SESSION_IMPL *session)
{
    return (F_ISSET(S2C(session), WT_CONN_LIVE_RESTORE)) &&
      !TAILQ_EMPTY(&S2C(session)->live_restore_server.work_queue);
}
/*
 * __live_restore_work_queue_drain --
 *     Drain the work queue of any remaining items.
 */
static void
__live_restore_work_queue_drain(WT_SESSION_IMPL *session)
{
    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;
    if (TAILQ_EMPTY(&server->work_queue))
        return;

    WT_LIVE_RESTORE_WORK_ITEM *work_item = NULL, *work_item_tmp = NULL;
    TAILQ_FOREACH_SAFE(work_item, &server->work_queue, q, work_item_tmp)
    {
        TAILQ_REMOVE(&server->work_queue, work_item, q);
        __wt_free(session, work_item->uri);
        __wt_free(session, work_item);
    }
    WT_ASSERT(session, TAILQ_EMPTY(&server->work_queue));
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

    if (TAILQ_EMPTY(&server->work_queue))
        return (0);

    WT_LIVE_RESTORE_WORK_ITEM *work_item = NULL;
    WT_WITH_LOCK_WAIT(session, &server->queue_lock, WT_SESSION_LOCKED_LIVE_RESTORE_QUEUE,
      TAILQ_REMOVE(&server->work_queue, work_item, q));

    WT_CURSOR *cursor;
    WT_SESSION *wt_session = (WT_SESSION *)session;

    /* TODO: Add logic to differentiate log files. it may be easier to have two separate queues.*/
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
    WT_FILE_HANDLE *fh = ((CUR2BT(cursor)->bm)->handle_array[0]->fh->handle);

    /*
     * Call the fill holes function. Right now no other reads or writes can be occurring
     * concurrently.
     */
    return (__wti_live_restore_fs_fill_holes(fh, wt_session));
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

    /* Create the thread group. */
    WT_ERR(__wt_thread_group_create(session, &server->threads, "live_restore_workers", 0, cval.val,
      0, __live_restore_worker_check, __live_restore_worker_run, NULL));

    if (0) {
err:
        if (work_item != NULL)
            __wt_free(session, work_item->uri);
        __wt_free(session, work_item);
        /* Should we unwind the full queue and free everything here? */
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
    WT_DECL_RET;

    conn = S2C(session);

    /*
     * TODO: If we indicate to the connection that source can be dropped we'll clear this flag. At
     * the same time we should destroy.
     */
    if (!F_ISSET(conn, WT_CONN_LIVE_RESTORE))
        return (0);

    F_CLR(conn, WT_CONN_LIVE_RESTORE);
    /* Let any running threads finish up. */
    WT_LIVE_RESTORE_SERVER *server = &conn->live_restore_server;
    __wt_cond_signal(session, server->threads.wait_cond);

    __wt_writelock(session, &server->threads.lock);
    /* This call destroys the lock. */
    WT_RET(__wt_thread_group_destroy(session, &server->threads));

    /* Empty the work queue. */
    __live_restore_work_queue_drain(session);
    return (ret);
}
