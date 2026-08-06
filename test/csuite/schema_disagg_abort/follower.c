/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Follower role: adopting the leader's checkpoints from the page log, and the role transitions.
 * Everything a running follower executes goes through the same reader and worker loops as a leader,
 * in node.c; the adoption itself is in ckpt.c, shared with the step-up.
 */

#include "schema_disagg_abort.h"

/*
 * follower_leave --
 *     Step out of following: nothing to do. The reader was already joined when the phase stopped,
 *     and the connection stays open for the step-up's reconfigure.
 */
static void
follower_leave(WORKLOAD_STATE *state, uint64_t final_counter)
{
    WT_UNUSED(state);
    WT_UNUSED(final_counter);
}

/*
 * follower_enter --
 *     This method is called when the node enters the follower role.
 */
static void
follower_enter(WORKLOAD_STATE *state, uint64_t final_counter)
{
    /* This node produced its checkpoints rather than adopting them; nothing to skip. */
    state->adopted_ckpt_lsn = 0;

    /*
     * This node keeps publishing the operations it applies for the new leader, so its frontier must
     * cover the whole term. Everything the new leader relays was allocated above it.
     */
    set_frontier(state->conn, final_counter);
}

/*
 * follower_checkpoint --
 *     Adopt the latest checkpoint the page log holds. The workers keep running through it. The
 *     first adoption reports follower readiness.
 */
static void
follower_checkpoint(WORKLOAD_STATE *state, WT_SESSION *session, CKPT_CTX *ckpt)
{
    if (ckpt_pick_up(state, session, ckpt->page_log) && !ckpt->picked_up) {
        testutil_sentinel(NULL, FOLLOWER_READY_FILE);
        ckpt->picked_up = true;
    }
}

const NODE_ROLE node_role_follower = {"follower", "debug=(skip_checkpoint=true)", false,
  follower_leave, follower_enter, follower_checkpoint};
