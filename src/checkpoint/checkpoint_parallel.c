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
 *     pointer on the ref. Handoff is load_acquire(ref), hazard_set, CAS(ref, NULL). Isolation,
 *     snapshot, work_dhandle and work_reconcile_flags are set once per file in begin_file and are
 *     stable for the duration of the file, so we read them from ckpt_threads after claiming.
 */
static int
__checkpoint_parallel_take_work(WT_SESSION_IMPL *session, WT_REF **refp, uint32_t *reconcile_flagsp)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;
    WT_DECL_RET;
    WT_REF *ref;
    bool busy;

    ckpt_threads = S2C(session)->ckpt_reconcile_threads;
    *refp = NULL;

    ref = (WT_REF *)__wt_atomic_load_ptr_acquire(&ckpt_threads->work_ref);
    if (ref == NULL)
        return (0);

    /* work_dhandle is stable for the file; set for hazard_set. */
    session->dhandle = ckpt_threads->work_dhandle;

    /*
     * Take our own hazard pointer on the ref before claiming it. The checkpoint session also holds
     * a hazard pointer; once we CAS work_ref to NULL it will release its copy, so we must establish
     * ours first. We already know the page is valid (checkpoint holds it), so a busy return is
     * transient.
     */
    for (;;) {
        ret = __wt_hazard_set(session, ref, &busy);
        if (ret != 0) {
            (void)__wt_atomic_cas_int32(&ckpt_threads->error, 0, ret);
            return (ret);
        }
        if (!busy)
            break;
        __wt_sleep(0, 1);
    }

    /*
     * Claim the work with CAS. Only one worker can succeed; losers release their hazard and return.
     * work_pending is incremented by the checkpoint when it posts (push_work), so drain cannot
     * see zero until the winning worker decrements after reconciling.
     */
    if (!__wt_atomic_cas_ptr(&ckpt_threads->work_ref, ref, NULL)) {
        WT_IGNORE_RET(__wt_page_release(session, ref, 0));
        return (0);
    }

    *refp = ref;
    *reconcile_flagsp = ckpt_threads->work_reconcile_flags;

    return (0);
}

/*
 * __checkpoint_parallel_thread_run --
 *     Entry function for a checkpoint page reconciliation thread. This is called repeatedly from
 *     the thread group code so we loop internally while work is available.
 */
static int
__checkpoint_parallel_thread_run(WT_SESSION_IMPL *session, WT_THREAD *thread)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;
    WT_DECL_RET;
    WT_REF *ref;
    uint32_t reconcile_flags;
    int spins;
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
        if (ref == NULL) {
            /*
             * Spin briefly waiting for new work before falling back to the condvar. Each
             * sched_yield is ~50-100ns, so 100 iterations is ~5-10us — enough to cover the gap
             * between the checkpoint thread walking to the next dirty leaf and posting work, without
             * burning significant CPU.
             */
            for (spins = 0;
                 spins < 100 && __wt_atomic_load_ptr_acquire(&ckpt_threads->work_ref) == NULL;
                 spins++)
                __wt_yield();
            if (__wt_atomic_load_ptr_acquire(&ckpt_threads->work_ref) != NULL)
                continue;
            break;
        }

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
         * Set the error before decrementing work_pending. Drain waits for work_pending to reach
         * zero; if we decremented first, drain could see zero and return before the error is visible.
         */
        if (ret != 0)
            (void)__wt_atomic_cas_int32(&ckpt_threads->error, 0, ret);

        (void)__wt_atomic_sub_uint64(&ckpt_threads->work_pending, 1);

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
 * __wt_checkpoint_parallel_begin_file --
 *     Set the four file-scoped fields for parallel reconciliation. Called once at the start of
 *     __wt_sync_file(WT_SYNC_CHECKPOINT); isolation, snapshot, dhandle and reconcile_flags do not
 *     change during a single file sync.
 */
void
__wt_checkpoint_parallel_begin_file(WT_SESSION_IMPL *session, uint32_t reconcile_flags)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;

    if (!WT_PARALLEL_CHECKPOINTS_ENABLED(session))
        return;

    ckpt_threads = S2C(session)->ckpt_reconcile_threads;
    ckpt_threads->checkpoint_isolation = session->txn->isolation;
    ckpt_threads->checkpoint_snapshot = &session->txn->snapshot_data;
    ckpt_threads->work_dhandle = session->dhandle;
    ckpt_threads->work_reconcile_flags = reconcile_flags;
}

/*
 * __wt_checkpoint_parallel_push_work --
 *     Post a leaf page for parallel reconciliation. The four file-scoped fields were set in
 *     begin_file; handoff is store_release(ref) then wait for a worker to CAS it to NULL.
 */
int
__wt_checkpoint_parallel_push_work(WT_SESSION_IMPL *session, WT_REF *ref)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;
    int32_t err;

    ckpt_threads = S2C(session)->ckpt_reconcile_threads;

    /* Bail out early if a worker already failed. */
    if ((err = __wt_atomic_load_int32_relaxed(&ckpt_threads->error)) != 0)
        return (err);

    /*
     * Increment work_pending before publishing so that when we see work_ref == NULL and return,
     * drain cannot see zero until the worker that took the item decrements (success or error path).
     */
    (void)__wt_atomic_add_uint64(&ckpt_threads->work_pending, 1);

    /* Publish the work item; workers read the stable file-scoped fields from ckpt_threads. */
    __wt_atomic_store_ptr_release(&ckpt_threads->work_ref, ref);
    __wt_cond_signal(session, ckpt_threads->work_cond);

    /*
     * Wait for a worker to take the item. If a worker failed (error set), do not revoke: keep
     * waiting for work_ref == NULL so some worker takes it and decrements work_pending. Revoking
     * (clearing work_ref and decrementing) would race with a worker CASing to take the item.
     */
    while (__wt_atomic_load_ptr_acquire(&ckpt_threads->work_ref) != NULL) {
        
    }
    return (__wt_atomic_load_int32_relaxed(&ckpt_threads->error));
}

/*
 * __wt_checkpoint_parallel_drain --
 *     Wait for all in-flight parallel reconciliations to complete. Called at internal page
 *     boundaries and at the end of the tree walk.
 */
int
__wt_checkpoint_parallel_drain(WT_SESSION_IMPL *session)
{
    WT_CHECKPOINT_RECONCILE_THREADS *ckpt_threads;
    int32_t err;

    ckpt_threads = S2C(session)->ckpt_reconcile_threads;

    /*
     * Acquire-load work_pending to synchronize with worker stores. Workers set the error (CAS
     * release) before decrementing work_pending, so observing zero here guarantees visibility of
     * any prior error store.
     */
    while (__wt_atomic_load_uint64_acquire(&ckpt_threads->work_pending) > 0) {
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

    WT_RET(__wt_cond_auto_alloc(session, "checkpoint page reconciliation threads - work (signal)",
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
