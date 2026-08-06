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
WT_THREAD_RET
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

/*
 * workers_min --
 *     Return the minimum completed timestamp across all worker threads: the frontier with no
 *     unfinished publish or commit at or below it. Returns 0 if any worker has not yet completed an
 *     operation this phase.
 */
static uint64_t
workers_min(WORKLOAD_STATE *state)
{
    uint64_t min_val = UINT64_MAX;
    for (uint32_t i = 0; i < state->nth_workers; i++) {
        const uint64_t val = __wt_atomic_load_uint64(&state->workers[i].completed_ts);
        if (val == 0)
            return (0);
        if (val < min_val)
            min_val = val;
    }
    return (min_val);
}

/*
 * thread_ts_run --
 *     Advances the oldest and stable timestamps and the stable schema epoch to the workers'
 *     completed frontier, keeping stable data on published tables only.
 */
WT_THREAD_RET
thread_ts_run(void *arg)
{
    WORKLOAD_STATE *state = arg;

    while (workload_active(state, STAGE_TS)) {
        /*
         * The single frontier serves both schema and data operations: everything at or below it is
         * published and committed.
         */
        const uint64_t frontier = workers_min(state);
        if (frontier != 0) {
            const uint64_t cur_stable = query_ts(state->conn, "stable_timestamp");
            if (frontier >= cur_stable)
                set_frontier(state->conn, frontier);
        }

        __wt_sleep(0, 100 * WT_THOUSAND);
    }
    return (WT_THREAD_RET_VALUE);
}
