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
      !TAILQ_EMPTY(S2C(session)->live_restore_server.work_queue);
}

/*
 * __live_restore_worker_run --
 *     Entry function for a live restore thread. This is called repeatedly from the thread group
 *     code so it does not need to loop itself.
 */
static int
__live_restore_worker_run(WT_SESSION_IMPL *session, WT_THREAD *ctx)
{
    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;

    if (TAILQ_EMPTY(server->work_queue))
        return;

    WT_LIVE_RESTORE_WORK_ITEM *work_item;
    WT_WITH_LOCK_WAIT(session, &server->queue_lock, WT_SESSION_LOCKED_LIVE_RESTORE_QUEUE,
      TAILQ_REMOVE(server->work_queue, work_item, q));

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
    WT_LIVE_RESTORE_FILE_HANDLE *fh =
      (WT_LIVE_RESTORE_FILE_HANDLE *)((CUR2BT(cursor)->bm)->handle_array[0]->fh->handle);

    /* Call the fill holes function. */
    __wti_live_restore_fs_fill_holes(fh, wt_session);
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

    return (__wt_thread_group_create(session, &server->threads, "live_restore_workers", 0, cval.val,
      0, __live_restore_worker_check, __live_restore_worker_run, NULL));
}
