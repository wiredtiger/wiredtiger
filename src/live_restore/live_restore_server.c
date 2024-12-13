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
 * __live_restore_worker_stop --
 *     When a live restore worker stops we need to manage some state. If all workers stop and the
 *     queue is empty then update the state statistic to track that.
 */
static int
__live_restore_worker_stop(WT_SESSION_IMPL *session, WT_THREAD *ctx)
{
    WT_UNUSED(ctx);
    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;

    __wt_spin_lock(session, &server->queue_lock);
    server->threads_working--;

    if (server->threads_working == 0 && TAILQ_EMPTY(&server->work_queue)) {
        /*
         * If all the threads have stopped and the queue is empty signal that the live restore is
         * complete.
         */
        WT_STAT_CONN_SET(session, live_restore_state, WT_LIVE_RESTORE_COMPLETE);
        __wt_verbose_debug1(session, WT_VERB_FILEOPS, "%s", "Live restore finished");
    }
    __wt_spin_unlock(session, &server->queue_lock);

    return (0);
}

/*
 * __live_restore_work_queue_drain --
 *     Drain the work queue of any remaining items.This is called either on connection close - and
 *     the work will be continued after a restart - or for error handling cleanup in which case
 *     we're about to crash.
 */
static void
__live_restore_work_queue_drain(WT_SESSION_IMPL *session)
{
    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;

    /*
     * All contexts that call this function are singly threaded however we take the lock as that is
     * the correct semantic and will future proof the code.
     */
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
    WT_ASSERT_ALWAYS(
      session, TAILQ_EMPTY(&server->work_queue), "Live restore work queue failed to drain");
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
    WT_DECL_RET;
    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;

    __wt_spin_lock(session, &server->queue_lock);
    if (TAILQ_EMPTY(&server->work_queue)) {
        /* Stop our thread from running. This will call the stop_func and trigger state cleanup. */
        F_CLR(ctx, WT_THREAD_RUN);
        __wt_verbose_debug2(session, WT_VERB_FILEOPS, "%s", "Live restore worker terminating");
        __wt_spin_unlock(session, &server->queue_lock);
        return (0);
    }

    WT_LIVE_RESTORE_WORK_ITEM *work_item = NULL;

    work_item = TAILQ_FIRST(&server->work_queue);
    WT_ASSERT(session, work_item != NULL);
    TAILQ_REMOVE(&server->work_queue, work_item, q);
    WT_STAT_CONN_SET(session, live_restore_queue_length, --server->queue_size);
    __wt_verbose_debug2(
      session, WT_VERB_FILEOPS, "Live restore worker taking queue item: %s", work_item->uri);
    __wt_spin_unlock(session, &server->queue_lock);

    WT_CURSOR *cursor;
    WT_SESSION *wt_session = (WT_SESSION *)session;

    /*
     * Open a cursor so no one can get exclusive access on the object. This prevents concurrent
     * schema operations like drop and rename. Even if this object is a log file it can have a
     * cursor opened it. Opening a cursor on a log will prevent it from getting archived.
     *
     * If the file no longer exists, which for logs means they could have been archived and for
     * regular files dropped, don't error out.
     */
    ret = wt_session->open_cursor(wt_session, work_item->uri, NULL, NULL, &cursor);
    if (ret == ENOENT)
        return (0);
    WT_RET(ret);

    /*
     * We need to get access to the WiredTiger file handle. Given we've opened the cursor we should
     * be able to access the WT_FH by first getting to its block manager and then the WT_FH.
     */
    WT_BM *bm = CUR2BT(cursor)->bm;
    WT_ASSERT(session, bm->is_multi_handle == false);

    /* This will be replaced with an API call in the future for now it is what we have. */
    WT_FILE_HANDLE *fh = bm->block->fh->handle;

    /*
     * Call the fill holes function. Right now no other reads or writes should be occurring
     * concurrently or else things will eventually break.
     *
     * FIXME-WT-13825: Update this comment.
     */
    __wt_verbose_debug2(
      session, WT_VERB_FILEOPS, "Live restore worker filling holes for: %s", work_item->uri);
    ret = __wti_live_restore_fs_fill_holes(fh, wt_session);
    WT_TRET(cursor->close(cursor));

    return (ret);
}

/*
 * __live_restore_populate_queue --
 *     Populate the live restore work queue. Free all objects on failure.
 */
static int
__live_restore_populate_queue(WT_SESSION_IMPL *session, uint64_t *work_count)
{
    WT_DECL_RET;
    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;

    TAILQ_INIT(&server->work_queue);
    /*
     * Open a metadata cursor to gather the list of objects. The metadata file is built from the
     * WiredTiger.backup file, during __wt_turtle_init. Thus this function must be run after that
     * function. I don't know if we have a way of enforcing that.
     */

    /*
     * FIXME-WT-13888: Add logic to queue log files first, then the oplog then the history store.
     * This will use a directory list call.
     */
    WT_CURSOR *cursor;
    WT_RET(__wt_metadata_cursor(session, &cursor));
    WT_LIVE_RESTORE_WORK_ITEM *work_item = NULL;
    __wt_verbose_debug1(session, WT_VERB_FILEOPS, "Initializing the live restore work %s", "queue");

    *work_count = 0;
    while ((ret = cursor->next(cursor)) == 0) {
        const char *uri = NULL;
        WT_ERR(cursor->get_key(cursor, &uri));
        if (WT_PREFIX_MATCH(uri, "file:")) {
            __wt_verbose_debug2(
              session, WT_VERB_FILEOPS, "Adding an item to the work queue %s", uri);
            WT_ERR(__wt_calloc_one(session, &work_item));
            WT_ERR(__wt_strdup(session, uri, &work_item->uri));
            TAILQ_INSERT_HEAD(&server->work_queue, work_item, q);
            (*work_count)++;
        }
    }
    WT_ERR_NOTFOUND_OK(ret, false);

    if (0) {
err:
        /* The queue insert cannot fail so whatever we've put into it is guaranteed to be there. */
        __live_restore_work_queue_drain(session);

        /* Free the various items. */
        __wt_free(session, work_item->uri);
        __wt_free(session, work_item);
    }
    return (ret);
}

/*
 * __wt_live_restore_server_create --
 *     Start the worker threads and build the work queue.
 */
int
__wt_live_restore_server_create(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CONFIG_ITEM cval;

    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;

    /* Check that we have a live restore file system before starting the threads. */
    if (!F_ISSET(S2C(session), WT_CONN_LIVE_RESTORE_FS))
        return (0);

    /*
     * Set this state before we run the threads, if we do it after there's a chance we'll context
     * switch and then this state will happen after the finish state. This also means we transition
     * through all valid states.
     */
    WT_STAT_CONN_SET(session, live_restore_state, WT_LIVE_RESTORE_IN_PROGRESS);

    /* Read the threads_max config, 0 threads is valid in which case we don't do anything. */
    WT_RET(__wt_config_gets(session, cfg, "live_restore.threads_max", &cval));
    if (cval.val == 0)
        return (0);

    /*
     * Even if we start from an empty database the history store file will exist before we get here
     * which means there will always be at least one item in the queue.
     */
    uint64_t work_count;
    WT_RET(__live_restore_populate_queue(session, &work_count));
    WT_STAT_CONN_SET(session, live_restore_queue_length, work_count);
    server->queue_size = work_count;

    /* Set this value before the threads start up in case they immediately decrement it. */
    server->threads_working = (uint32_t)cval.val;
    /*
     * Create the thread group.
     *
     * WiredTiger thread groups are very weird, all threads will enter the run loop but unless
     * WT_THREAD_ACTIVE is set on a given thread it will wait 10 seconds before actually executing
     * the run func. Then on the next iteration the thread will wait another 10 seconds and then
     * execute run func. So WT_THREAD_ACTIVE does not mean the thread won't do work. To have
     * WT_THREAD_ACTIVE set on a thread __wt_thread_group_start_one needs to be called but that is
     * expected to be called externally. Calling __wt_thread_group_start_one can be thought of as
     * "starting" the thread. On thread group creation __wt_thread_group_start_one will be called
     * for min_thread_count number of threads. So to get them all "started" we specify a
     * min_thread_count equal to our max_thread_count. Alternatively we could loop and "start" them
     * all ourselves but we cannot guarantee that by the time we call start, after creating the
     * thread group, the threads haven't terminated themselves.
     *
     * So in summary there are 3 things of note here:
     *   - Threads can be active but not started despite this they are always running and calling
     *     into the run_func, but only every 10 seconds.
     *   - We terminate threads which is not expected by the thread group. So we can't call
     *     __wt_thread_group_start_one yet.
     *   - The thread group code expects whatever subsystem that is using it to scale the number of
     *     active threads but only eviction actually does this. We plan on doing this in some form
     *     in the future but for now are short circuiting this weirdness by specifying min_threads
     *     to be the same as max_threads.
     */
    return (__wt_thread_group_create(session, &server->threads, "live_restore_workers",
      (uint32_t)cval.val, (uint32_t)cval.val, 0, __live_restore_worker_check,
      __live_restore_worker_run, __live_restore_worker_stop));
}

/*
 * __wt_live_restore_server_destroy --
 *     Destroy the live restore threads.
 */
int
__wt_live_restore_server_destroy(WT_SESSION_IMPL *session)
{
    WT_LIVE_RESTORE_SERVER *server = &S2C(session)->live_restore_server;

    /* If we didn't create a live restore file system then we also didn't start any threads. */
    if (!F_ISSET(S2C(session), WT_CONN_LIVE_RESTORE_FS))
        return (0);

    /*
     * It is possible to get here without ever starting the thread group. Ensure that it has been
     * created before destroying it. One such case would be if we configure the live restore file
     * system. But then an error occurs and we never initialize the server before destroying it.
     */
    if (server->threads.wait_cond == NULL)
        return (0);

    /* Let any running threads finish up. */
    __wt_cond_signal(session, server->threads.wait_cond);
    __wt_writelock(session, &server->threads.lock);
    /* This call destroys the thread group lock. */
    WT_RET(__wt_thread_group_destroy(session, &server->threads));

    __live_restore_work_queue_drain(session);
    return (0);
}
