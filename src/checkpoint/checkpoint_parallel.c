/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

/*
 * __checkpoint_parallel_thread_chk --
 *     Check to decide if the checkpoint page reconciliation thread should continue running.
 */
static bool
__checkpoint_parallel_thread_chk(WT_SESSION_IMPL *session)
{
    return (FLD_ISSET(S2C(session)->server_flags, WT_CONN_SERVER_CHECKPOINT_RECONCILE_THREADS));
}

/*
 * __checkpoint_parallel_take_work --
 *     A worker attempts to take the current work item. On success the worker holds its own hazard
 *     pointer on the ref and the per-item work fields have been copied to the output parameters.
 */
static int
__checkpoint_parallel_take_work(
  WT_SESSION_IMPL *session, WT_REF **refp, uint32_t *reconcile_flagsp)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;
    WT_DECL_RET;
    WT_REF *ref;
    bool busy;

    ckpt_threads = S2C(session)->ckpt_reconcile_threads;
    *refp = NULL;

    __wt_spin_lock(session, &ckpt_threads->work_lock);

    ref = (WT_REF *)__wt_atomic_load_ptr_acquire(&ckpt_threads->work_ref);
    if (ref == NULL) {
        __wt_spin_unlock(session, &ckpt_threads->work_lock);
        return (0);
    }

    /*
     * Set the session's dhandle so __wt_hazard_set can find the btree. The acquire-load of
     * work_ref above guarantees we see the checkpoint's store to work_dhandle.
     */
    session->dhandle = ckpt_threads->work_dhandle;

    /*
     * Take our own hazard pointer on the ref before clearing the shared pointer. The checkpoint
     * session also holds a hazard pointer; once we clear work_ref it will release its copy, so we
     * must establish ours first. We already know the page is valid (checkpoint holds it), so a
     * busy return is transient.
     */
    for (;;) {
        ret = __wt_hazard_set(session, ref, &busy);
        if (ret != 0) {
            (void)__wt_atomic_cas_int32(&ckpt_threads->error, 0, ret);
            __wt_spin_unlock(session, &ckpt_threads->work_lock);
            return (ret);
        }
        if (!busy)
            break;
        __wt_yield();
    }

    *refp = ref;
    *reconcile_flagsp = ckpt_threads->work_reconcile_flags;

    /*
     * Increment workers_active before clearing work_ref. The checkpoint session uses
     * workers_active in drain to know when all reconciliations are complete; if we cleared
     * work_ref first, the checkpoint could observe the clear and call drain before we increment,
     * seeing zero active workers while we're about to start reconciling.
     */
    (void)__wt_atomic_add_uint64(&ckpt_threads->workers_active, 1);

    /* Clear the pointer so the checkpoint session knows we took it. */
    __wt_atomic_store_ptr_release(&ckpt_threads->work_ref, NULL);

    __wt_spin_unlock(session, &ckpt_threads->work_lock);
    return (0);
}

/*
 * __checkpoint_parallel_thread_run --
 *     Entry function for a checkpoint page reconciliation thread. This is called repeatedly from the
 *     thread group code so we loop internally while work is available.
 */
static int
__checkpoint_parallel_thread_run(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;
    WT_DECL_RET;
    WT_REF *ref;
    uint32_t reconcile_flags;
    bool signalled;

    WT_UNUSED(thread);

    ckpt_threads = S2C(session)->ckpt_reconcile_threads;

    /* Wait until the next event. */
    __wt_cond_wait_signal(
      session, ckpt_threads->work_cond, WT_MILLION, __checkpoint_parallel_thread_chk, &signalled);

    for (;;) {
        /* Stop taking work if a sibling already failed. */
        if (__wt_atomic_load_int32_relaxed(&ckpt_threads->error) != 0)
            break;

        WT_ERR(__checkpoint_parallel_take_work(session, &ref, &reconcile_flags));
        if (ref == NULL)
            break;

        /* Begin a transaction and import the checkpoint's snapshot once. */
        if (!F_ISSET(session->txn, WT_TXN_RUNNING)) {
            WT_ERR(__wt_txn_begin(session, NULL));
            F_SET(session, WT_SESSION_CHECKPOINT);
            F_SET(session, WT_SESSION_CHECKPOINT_WORKER);
            __wt_txn_import_snapshot(session, ckpt_threads->checkpoint_snapshot);
            session->isolation = session->txn->isolation = ckpt_threads->checkpoint_isolation;
        }

        /* session->dhandle was set by take_work for the hazard pointer. */
        ret = __wt_reconcile(session, ref, NULL, reconcile_flags);
        WT_STAT_CONN_INCR(session, checkpoint_parallel_pages_reconciled);

        /* Release our hazard pointer; we're done with this page. */
        WT_TRET(__wt_page_release(session, ref, 0));

        /*
         * Set the error before decrementing workers_active. Drain waits for workers_active to
         * reach zero; if we decremented first, drain could see zero active workers and return
         * success before the error is visible.
         */
        if (ret != 0)
            (void)__wt_atomic_cas_int32(&ckpt_threads->error, 0, ret);

        (void)__wt_atomic_sub_uint64(&ckpt_threads->workers_active, 1);

        if (ret != 0)
            WT_ERR(ret);
    }

    if (0) {
err:
        WT_RET_PANIC(session, ret, "Checkpoint page reconciliation thread error");
    }

    return (0);
}

/*
 * __wt_checkpoint_parallel_push_work --
 *     Post a leaf page for parallel reconciliation. This function blocks until a worker takes the
 *     item (acquires its own hazard pointer). The caller retains its hazard pointer on the ref;
 *     the walk code releases it when advancing to the next page.
 */
int
__wt_checkpoint_parallel_push_work(
  WT_SESSION_IMPL *session, WT_REF *ref, uint32_t reconcile_flags)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;
    int32_t err;

    ckpt_threads = S2C(session)->ckpt_reconcile_threads;

    /* Bail out early if a worker already failed. */
    if ((err = __wt_atomic_load_int32_relaxed(&ckpt_threads->error)) != 0)
        return (err);

    /*
     * Set the per-checkpoint isolation and snapshot on the first push. These are constant for the
     * entire checkpoint so workers read them directly from the struct.
     */
    if (ckpt_threads->checkpoint_snapshot == NULL) {
        ckpt_threads->checkpoint_isolation = session->txn->isolation;
        ckpt_threads->checkpoint_snapshot = &session->txn->snapshot_data;
    }

    /*
     * Fill in the per-item work metadata. These are ordinary stores; the release-store of work_ref
     * below makes them visible to any thread that acquire-loads work_ref.
     */
    ckpt_threads->work_dhandle = session->dhandle;
    ckpt_threads->work_reconcile_flags = reconcile_flags;

    /* Publish the work item. */
    __wt_atomic_store_ptr_release(&ckpt_threads->work_ref, ref);
    __wt_cond_signal(session, ckpt_threads->work_cond);

    /*
     * Wait for a worker to take the item. A worker taking it means it has established its own
     * hazard pointer on the ref.
     */
    while (__wt_atomic_load_ptr_relaxed(&ckpt_threads->work_ref) != NULL) {
        if ((err = __wt_atomic_load_int32_relaxed(&ckpt_threads->error)) != 0) {
            /*
             * A worker failed. Clear the work pointer under the lock so we don't race with a
             * worker mid-handoff.
             */
            __wt_spin_lock(session, &ckpt_threads->work_lock);
            __wt_atomic_store_ptr_release(&ckpt_threads->work_ref, NULL);
            __wt_spin_unlock(session, &ckpt_threads->work_lock);
            return (err);
        }
        __wt_yield();
    }

    return (0);
}

/*
 * __wt_checkpoint_parallel_drain --
 *     Wait for all in-flight parallel reconciliations to complete. Called at internal page boundaries
 *     and at the end of the tree walk.
 */
int
__wt_checkpoint_parallel_drain(WT_SESSION_IMPL *session)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;
    int32_t err;

    ckpt_threads = S2C(session)->ckpt_reconcile_threads;

    /*
     * Acquire-load workers_active to synchronize with worker stores. Workers set the error (CAS
     * release) before decrementing workers_active, so observing zero here with acquire semantics
     * guarantees visibility of any prior error store.
     */
    while (__wt_atomic_load_uint64_acquire(&ckpt_threads->workers_active) > 0) {
        if ((err = __wt_atomic_load_int32_relaxed(&ckpt_threads->error)) != 0)
            return (err);
        __wt_yield();
    }

    return (__wt_atomic_load_int32_relaxed(&ckpt_threads->error));
}

/*
 * __wt_checkpoint_parallel_thread_create --
 *     Start the checkpoint page reconciliation threads.
 */
int
__wt_checkpoint_parallel_thread_create(WT_SESSION_IMPL *session, const char *cfg[])
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;
    WT_CONFIG_ITEM cval;
    WT_CONNECTION_IMPL *conn;
    uint32_t session_flags;
    int checkpoint_threads;

    conn = S2C(session);

    conn->ckpt_reconcile_threads = ckpt_threads = &conn->_ckpt_reconcile_threads;

    /* Get the number of checkpoint threads from the configuration. */
    WT_RET(__wt_config_gets(session, cfg, "checkpoint_threads", &cval));
    checkpoint_threads = (int)cval.val;
    if (checkpoint_threads < 1)
        checkpoint_threads = 1;

    ckpt_threads->num_threads = (uint32_t)checkpoint_threads;

    /* If the number of checkpoint threads is 1, parallel checkpoints are disabled. */
    if (checkpoint_threads == 1)
        return (0);

    /* Set first, the thread might run before we finish up. */
    FLD_SET(conn->server_flags, WT_CONN_SERVER_CHECKPOINT_RECONCILE_THREADS);

    WT_RET(__wt_spin_init(
      session, &ckpt_threads->work_lock, "checkpoint page reconciliation threads - work"));
    WT_RET(
      __wt_cond_auto_alloc(session, "checkpoint page reconciliation threads - work (signal)",
        10 * WT_THOUSAND, WT_MILLION, &ckpt_threads->work_cond));

    /* Create the checkpoint thread group. */
    session_flags = WT_THREAD_CAN_WAIT | WT_THREAD_PANIC_FAIL;
    WT_RET(__wt_thread_group_create(session, &ckpt_threads->thread_group,
      "checkpoint-page-reconciliation-threads", ckpt_threads->num_threads,
      ckpt_threads->num_threads, session_flags, __checkpoint_parallel_thread_chk,
      __checkpoint_parallel_thread_run, NULL));

    return (0);
}

/*
 * __wt_checkpoint_parallel_thread_destroy --
 *     Destroy the checkpoint page reconciliation threads.
 */
int
__wt_checkpoint_parallel_thread_destroy(WT_SESSION_IMPL *session)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;
    WT_CONNECTION_IMPL *conn;
    WT_DECL_RET;

    conn = S2C(session);
    ckpt_threads = conn->ckpt_reconcile_threads;

    /* Check whether we have initialized the threads to begin with. */
    if (ckpt_threads == NULL)
        return (0);
    if (!FLD_ISSET(conn->server_flags, WT_CONN_SERVER_CHECKPOINT_RECONCILE_THREADS))
        return (0);

    /* Wait for any checkpoint thread group changes to stabilize. */
    __wt_writelock(session, &ckpt_threads->thread_group.lock);

    FLD_CLR(conn->server_flags, WT_CONN_SERVER_CHECKPOINT_RECONCILE_THREADS);
    __wt_cond_signal(session, ckpt_threads->work_cond);

    __wt_verbose(session, WT_VERB_CHECKPOINT, "%s", "Waiting for helper threads");

    WT_TRET(__wt_thread_group_destroy(session, &ckpt_threads->thread_group));
    __wt_spin_destroy(session, &ckpt_threads->work_lock);
    __wt_cond_destroy(session, &ckpt_threads->work_cond);

    return (ret);
}

/*
 * __checkpoint_parallel_thread_release_snapshot --
 *     Release the snapshot associated with a worker thread.
 */
static int
__checkpoint_parallel_thread_release_snapshot(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_UNUSED(thread);

    if (!F_ISSET(session->txn, WT_TXN_HAS_SNAPSHOT))
        return (0);

    __wt_verbose(session, WT_VERB_CHECKPOINT,
      "Checkpoint page reconciliation thread %u releasing the snapshot", thread->id);
    __wt_txn_release_snapshot(session);

    return (0);
}

/*
 * __wti_checkpoint_parallel_release_snapshot --
 *     Release all snapshots for the checkpoint page reconciliation workers.
 */
int
__wti_checkpoint_parallel_release_snapshot(WT_SESSION_IMPL *session)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;

    if (!WT_PARALLEL_CHECKPOINTS_ENABLED(session))
        return (0);

    ckpt_threads = S2C(session)->ckpt_reconcile_threads;
    WT_RET(__wt_thread_group_foreach(
      session, &ckpt_threads->thread_group, __checkpoint_parallel_thread_release_snapshot));
    return (0);
}

/*
 * __checkpoint_parallel_thread_commit --
 *     Commit the transaction associated with a worker thread.
 */
static int
__checkpoint_parallel_thread_commit(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_UNUSED(thread);

    if (!F_ISSET(session->txn, WT_TXN_RUNNING))
        return (0);

    __wt_verbose(session, WT_VERB_CHECKPOINT,
      "Checkpoint page reconciliation thread %u committing the transaction", thread->id);
    WT_RET(__wt_txn_commit(session, NULL));

    WT_ASSERT(session, !F_ISSET(session->txn, WT_TXN_RUNNING));
    return (0);
}

/*
 * __wti_checkpoint_parallel_commit --
 *     Commit all transactions for the checkpoint page reconciliation workers.
 */
int
__wti_checkpoint_parallel_commit(WT_SESSION_IMPL *session)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;

    if (!WT_PARALLEL_CHECKPOINTS_ENABLED(session))
        return (0);

    ckpt_threads = S2C(session)->ckpt_reconcile_threads;
    WT_RET(__wt_thread_group_foreach(
      session, &ckpt_threads->thread_group, __checkpoint_parallel_thread_commit));

    /* Reset per-checkpoint state for the next checkpoint. */
    ckpt_threads->checkpoint_snapshot = NULL;
    __wt_atomic_store_int32_relaxed(&ckpt_threads->error, 0);

    return (0);
}
