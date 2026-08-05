/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * The checkpoint stage: the node's checkpoint duty, paced independently of the workload - produced
 * while leading, adopted from the page log while following. Off the event stream so a checkpoint
 * can land between a schema operation and its publish, and can still be taken while a worker is
 * blocked waiting for one; on its own thread so a slow checkpoint cannot freeze the frontier.
 */

#include "schema_disagg_abort.h"

/*
 * node_current_role --
 *     Return the role this phase runs. The engine tracks the role as a single bool, so the role
 *     instance is derived from it rather than stored: one source of truth, no invariant to keep.
 */
static const NODE_ROLE *
node_current_role(const WORKLOAD_STATE *state)
{
    return (state->leads ? &node_role_leader : &node_role_follower);
}

/*
 * thread_ckpt_run --
 *     Take one checkpoint per random interval for as long as the phase runs. The interval runs from
 *     the previous checkpoint's completion, so slow checkpoints do not chain back to back.
 */
static WT_THREAD_RET
thread_ckpt_run(void *arg)
{
    WORKLOAD_STATE *state = arg;

    WT_SESSION *session;
    testutil_check(state->conn->open_session(state->conn, NULL, NULL, &session));

    /* The role's checkpoint bookkeeping for this phase; only a follower adopts checkpoints. */
    CKPT_CTX ckpt = {0};
    __wt_epoch(NULL, &ckpt.phase_start);
    if (!state->leads)
        testutil_check(state->conn->get_page_log(state->conn, "palite", &ckpt.page_log));

    /* The cadence draws from the stream one past the generator's per-worker streams. */
    WT_RAND_STATE *rnd = &state->gen_rnd[state->nth_workers];
    struct timespec last = ckpt.phase_start;
    uint64_t wait = __wt_random(rnd) % MAX_CKPT_INVL;

    while (workload_active(state, STAGE_CKPT)) {
        struct timespec now;
        __wt_epoch(NULL, &now);
        if ((uint64_t)WT_TIMEDIFF_SEC(now, last) < wait) {
            __wt_sleep(0, 100 * WT_THOUSAND);
            continue;
        }

        node_current_role(state)->checkpoint(state, session, &ckpt);

        __wt_epoch(NULL, &last);
        wait = __wt_random(rnd) % MAX_CKPT_INVL;
    }

    if (ckpt.page_log != NULL)
        testutil_check(ckpt.page_log->terminate(ckpt.page_log, NULL));
    testutil_check(session->close(session, NULL));
    return (WT_THREAD_RET_VALUE);
}

/* The checkpoint thread's handle; its bookkeeping lives on the thread's own stack. */
static wt_thread_t ckpt_thr;

/*
 * node_ckpt_start --
 *     Start the checkpoint thread for a phase. Every phase has one, in either role.
 */
void
node_ckpt_start(WORKLOAD_STATE *state)
{
    testutil_check(__wt_thread_create(NULL, &ckpt_thr, thread_ckpt_run, state));
}

/*
 * node_ckpt_join --
 *     Join the checkpoint thread. The stage it exits on is the caller's to set.
 */
void
node_ckpt_join(void)
{
    testutil_check(__wt_thread_join(NULL, &ckpt_thr));
}
