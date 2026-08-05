/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Leader role: the two ends of a leadership term, both in-place reconfigures. Stepping down makes
 * the term's tail durable, releases the page log's single writer slot, and hands the term over to
 * the peer; stepping up continues the previous term's counter.
 *
 * Everything a running leader does - generating, executing and relaying events, and checkpointing -
 * is the generic engine, node.c and its stages, with role->leads set.
 */

#include "schema_disagg_abort.h"

/*
 * leader_checkpoint --
 *     Produce one checkpoint. The first one reports leader readiness. Nothing is checkpointed while
 *     no stable timestamp exists yet.
 */
static void
leader_checkpoint(WORKLOAD_STATE *state, WT_SESSION *session, CKPT_CTX *ckpt)
{
    /* The stable value this checkpoint is bound to cover; it can only advance mid-checkpoint. */
    const uint64_t covered = query_ts(state->conn, "stable_timestamp");
    if (covered == 0) {
        struct timespec now;
        __wt_epoch(NULL, &now);
        if (WT_TIMEDIFF_SEC(now, ckpt->phase_start) > MAX_OP_WAIT)
            testutil_die(ETIMEDOUT, "stable timestamp not set after %d seconds", MAX_OP_WAIT);
        return;
    }

    /* The timestamp thread owns the stable epoch and timestamps; just checkpoint. */
    testutil_check(session->checkpoint(session, "use_timestamp=true"));

    /*
     * The generator's drop gate is not published here: the timestamp thread republishes it from the
     * connection's durable schema epoch, which this checkpoint has just advanced.
     */

    println("Node %" PRIu32 ": checkpoint %d complete", state->cfg->node_id, ++ckpt->produced);

    /* A stable frontier implies every worker published, so this checkpoint has a schema op. */
    if (ckpt->produced == 1)
        testutil_sentinel(NULL, LEADER_READY_FILE);
}

/*
 * leader_leave --
 *     Step down: take the step-down checkpoint so the term's tail is durable, reconfigure into the
 *     follower role before the peer steps up (the page log allows one writer), then hand the term
 *     over with the switch event, carrying the final counter value the next leader must continue
 *     from.
 */
static void
leader_leave(WORKLOAD_STATE *state, uint64_t final_counter)
{
    WT_CONNECTION *conn = state->conn;
    WT_SESSION *session;
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    /*
     * The term is quiesced and drained, so its counter is the step-down boundary: nothing more will
     * be committed or published. The checkpoint has to land on that boundary exactly - WiredTiger
     * asserts it at the role change - and has to carry the epoch with it, since the step-down
     * clears the shared metadata queue and loses any publish left behind.
     */
    if (final_counter != 0) {
        set_ts(conn, "step_down_timestamp", final_counter);
        set_frontier(conn, final_counter);
    }
    testutil_check(session->checkpoint(session, "use_timestamp=true"));
    testutil_check(session->close(session, NULL));
    testutil_check(conn->reconfigure(conn, "disaggregated=(role=follower)"));

    SCHEMA_EVENT ev = {0};
    ev.type = EVENT_SWITCH;
    ev.event_ts = final_counter;
    (void)node_event_send(state->cfg, &ev);
}

/*
 * leader_enter --
 *     Step up: reconfigure into the leader role and continue the previous leader's counter. The
 *     step-up is the transition that completes a swap.
 */
static void
leader_enter(WORKLOAD_STATE *state, uint64_t final_counter)
{
    /* Adopt the latest checkpoint from the page log first. */
    follower_adopt_latest(state);

    testutil_check(state->conn->reconfigure(state->conn, "disaggregated=(role=leader)"));
    workload_seed_counter(state, final_counter);

    /*
     * Restore the stable frontier on the new leader's connection: a follower's reopened connection
     * starts with none, and a checkpoint taken before this term advances it (a short term's closing
     * checkpoint, say) must not regress the shared metadata's epoch.
     */
    if (final_counter != 0)
        set_frontier(state->conn, final_counter);
}

const NODE_ROLE node_role_leader = {
  "leader", NULL, true, leader_leave, leader_enter, leader_checkpoint};
