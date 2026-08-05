/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * The generic node: the phase loop, the WiredTiger connection, the workload engine's state and
 * per-phase lifecycle, the worker event queues, and the timestamp thread.
 *
 * One pipeline serves both roles, coordinating without locks: a generator produces the node's
 * command stream into a self-pipe, a reader demuxes the source pipe - the self-pipe, or a live
 * peer's - to N workers that apply the events, a timestamp thread advances the frontier, and a
 * checkpoint thread checkpoints on a cadence of its own. Each stage lives in its own file behind a
 * start/stop pair; the role specifics live in leader.c and follower.c behind the NODE_ROLE
 * operations.
 */

#include "schema_disagg_abort.h"

#include <signal.h>

/*
 * workload_state_create --
 *     Return the node's workload state, zeroed and bound to the configuration. The state has
 *     process lifetime; the control loop and the role transitions keep its connection current.
 */
WORKLOAD_STATE *
workload_state_create(TEST_CONFIG *cfg)
{
    static WORKLOAD_STATE state;
    WT_CLEAR(state);
    state.cfg = cfg;
    return (&state);
}

/*
 * workload_seed_counter --
 *     Seed the monotonic allocator from the previous leader's final counter, so a node stepping up
 *     continues the global epoch/timestamp sequence.
 */
void
workload_seed_counter(WORKLOAD_STATE *state, uint64_t ts)
{
    testutil_assert(state->current_ts <= ts);
    state->current_ts = ts;
}

/*
 * workload_counter_advance --
 *     Advance the monotonic allocator to at least the given applied value, so a follower's counter
 *     tracks everything it applied.
 */
void
workload_counter_advance(WORKLOAD_STATE *state, uint64_t v)
{
    uint64_t cur;
    do {
        cur = __wt_atomic_load_uint64(&state->current_ts);
    } while (cur < v && !__wt_atomic_cas_uint64(&state->current_ts, cur, v));
}

/*
 * workload_active --
 *     The condition a phase loop runs on: true until the shutdown has reached the caller's stage.
 */
bool
workload_active(WORKLOAD_STATE *state, uint32_t stage)
{
    return (__wt_atomic_load_uint32(&state->stop_stage) < stage);
}

/*
 * disagg_opts_init --
 *     Point the test options at the shared PALite page log: the single source of truth for the
 *     disaggregated configuration, used by the nodes and by the parent's recovery opens.
 */
void
disagg_opts_init(const TEST_CONFIG *cfg)
{
    cfg->opts->disagg.is_enabled = true;
    cfg->opts->disagg.page_log = "palite";
    cfg->opts->disagg.page_log_home = cfg->page_log_home;
    cfg->opts->disagg.drain_threads = 1;
}

/*
 * node_open --
 *     Open this node's WiredTiger connection in the given disaggregated mode.
 */
void
node_open(TEST_CONFIG *cfg, const char *disagg_mode, WT_CONNECTION **connp)
{
    char node_home[32];
    testutil_snprintf(node_home, sizeof(node_home), NODE_HOME_FMT, cfg->node_id);

    cfg->opts->disagg.mode = disagg_mode;
    testutil_wiredtiger_open(cfg->opts, node_home, ENV_CONFIG_DEF, NULL, connp, false, false);
}

/*
 * node_stop_requested --
 *     Check for the parent's graceful-stop sentinel. Not consumed: every node must see it.
 */
static bool
node_stop_requested(void)
{
    return (testutil_exists(NULL, STOP_FILE));
}

/*
 * node_switch_request_consume --
 *     Check for the parent's switch-request sentinel and consume it, so one request triggers
 *     exactly one switch. Only the acting node (the leader, or a lone node) may call this.
 */
bool
node_switch_request_consume(void)
{
    if (!testutil_exists(NULL, SWITCH_REQUEST_FILE))
        return (false);
    testutil_assert_errno(remove(SWITCH_REQUEST_FILE) == 0);
    return (true);
}

/* What ends a phase of either role. */
typedef enum { TRIGGER_STOP, TRIGGER_SWITCH } NODE_TRIGGER;

/*
 * node_trigger_wait --
 *     The payload wait of a phase, identical in both roles: sleep until something ends the phase.
 *     The stop sentinel ends any phase; a hand-over report ends it with a switch.
 *
 * Whoever owns the phase's stream consumes the switch request. A phase left with no stream - a
 *     follower whose peer died has neither generator nor reader - consumes it here instead.
 */
static NODE_TRIGGER
node_trigger_wait(WORKLOAD_STATE *state)
{
    while (!node_stop_requested()) {
        if (__wt_atomic_load_bool(&state->handover_received))
            return (TRIGGER_SWITCH);

        const bool abandoned_follower = !state->generates && !state->cfg->peer_alive;
        if (abandoned_follower && node_switch_request_consume())
            return (TRIGGER_SWITCH);

        /*
         * Nothing will ever end this phase once the parent is gone: it owns both sentinels. A lone
         * follower would otherwise idle for as long as the machine is up.
         */
        if (getppid() == 1)
            testutil_die(ECHILD, "Node %" PRIu32 ": parent exited", state->cfg->node_id);
        __wt_sleep(1, 0);
    }
    return (TRIGGER_STOP);
}

/*
 * node_transition_done --
 *     Account for a completed role transition; the transition that completes the swap reports it to
 *     the parent through the numbered sentinel.
 */
static void
node_transition_done(const TEST_CONFIG *cfg, WORKLOAD_STATE *state, bool completes_swap)
{
    ++state->switch_gen;
    if (!completes_swap)
        return;

    char name[64];
    testutil_snprintf(name, sizeof(name), SWITCH_DONE_FMT, state->switch_gen);
    testutil_sentinel(NULL, name);
    println("Node %" PRIu32 ": switch %" PRIu32 " complete", cfg->node_id, state->switch_gen);
}

/*
 * evq_push --
 *     Try to append one event to a worker's ring; false when full.
 */
static bool
evq_push(EVENT_QUEUE *q, const SCHEMA_EVENT *ev)
{
    const uint64_t tail = q->tail; /* single producer */
    if (tail - __wt_atomic_load_uint64(&q->head) >= EVQ_SIZE)
        return (false);
    q->ev[tail % EVQ_SIZE] = *ev;
    __wt_atomic_store_uint64(&q->tail, tail + 1);
    return (true);
}

/*
 * evq_pop --
 *     Try to take one event off a worker's ring; false when empty.
 */
static bool
evq_pop(EVENT_QUEUE *q, SCHEMA_EVENT *ev)
{
    const uint64_t head = q->head; /* single consumer */
    if (head == __wt_atomic_load_uint64(&q->tail))
        return (false);
    *ev = q->ev[head % EVQ_SIZE];
    __wt_atomic_store_uint64(&q->head, head + 1);
    return (true);
}

/*
 * evq_empty --
 *     Report whether a worker's ring is empty.
 */
static bool
evq_empty(EVENT_QUEUE *q)
{
    return (__wt_atomic_load_uint64(&q->head) == __wt_atomic_load_uint64(&q->tail));
}

/*
 * workload_enqueue --
 *     Queue one received schema event for its worker thread, blocking while the ring is full: the
 *     stalled reader stops draining the pipe, which backpressures the leader. Gives up when the
 *     phase is stopping.
 */
void
workload_enqueue(WORKLOAD_STATE *state, const SCHEMA_EVENT *ev)
{
    testutil_assert(ev->thread_id < state->nth_workers);

    EVENT_QUEUE *q = &state->workers[ev->thread_id].evq;
    while (!evq_push(q, ev) && workload_active(state, STAGE_WORKERS))
        __wt_sleep(0, WT_THOUSAND);
}

/*
 * workload_dequeue --
 *     Take the next event queued for one worker; false when nothing is queued for it.
 */
bool
workload_dequeue(WORKLOAD_STATE *state, uint32_t thread_index, SCHEMA_EVENT *ev)
{
    return (evq_pop(&state->workers[thread_index].evq, ev));
}

/*
 * workload_queue_empty --
 *     Report whether one worker's queue is empty.
 */
bool
workload_queue_empty(WORKLOAD_STATE *state, uint32_t thread_index)
{
    return (evq_empty(&state->workers[thread_index].evq));
}

/*
 * workload_drain_barrier --
 *     Wait until every worker has applied everything queued so far. Only the reader may call it: it
 *     is the sole producer for the queues, so nothing new can arrive while it waits here. It runs
 *     this before a hand-over, so the counter it asserts against the sender's covers every event of
 *     the term.
 */
void
workload_drain_barrier(WORKLOAD_STATE *state)
{
    for (uint32_t t = 0; t < state->nth_workers; t++)
        while (
          (!evq_empty(&state->workers[t].evq) || __wt_atomic_load_bool(&state->workers[t].busy)) &&
          workload_active(state, STAGE_WORKERS))
            __wt_sleep(0, WT_THOUSAND);
}

/*
 * workers_min --
 *     Return the minimum completed value across all worker threads: the frontier with no unfinished
 *     publish or commit at or below it. Returns 0 if any worker has not yet completed an operation
 *     this phase.
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
 *     completed frontier, keeping stable data on published tables only. Runs in both roles, and
 *     holds nothing slow: the checkpoint duty has a thread of its own so it cannot freeze this one.
 */
static WT_THREAD_RET
thread_ts_run(void *arg)
{
    WORKLOAD_STATE *state = arg;

    while (workload_active(state, STAGE_TS)) {
        /*
         * The single frontier serves both axes: everything at or below it is published and
         * committed, and any commit below it lands in a table created (and published) at a lower
         * value still. It only ever moves forward - a role transition sets it to the term's final
         * counter, which is ahead of anything a fresh phase's workers have completed.
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

/* The timestamp thread's handle; phases join and restart it but never free it. */
static wt_thread_t ts_thr;

/*
 * workload_start --
 *     Start one phase's threads, the same set in either role: N event-processing workers, the
 *     timestamp thread, the checkpoint thread, the reader (when the phase has an event source), and
 *     the generator. Only the event source differs by role: a leader phase consumes its own
 *     generated stream, a follower phase consumes the peer's.
 */
void
workload_start(WORKLOAD_STATE *state, bool as_leader)
{
    TEST_CONFIG *cfg = state->cfg;
    testutil_assert(cfg->nth <= MAX_TH);

    state->nth_workers = cfg->nth;
    state->leads = as_leader;
    /* A leader feeds itself; so does a follower with no peer. Snapshot it: peer_alive can flip. */
    state->generates = as_leader || !cfg->peer_alive;
    state->stop_stage = STAGE_NONE;
    state->handover_received = false;
    state->emitted = state->applied = 0;

    for (uint32_t i = 0; i < cfg->nth; i++) {
        /*
         * The stable frontier must wait for this phase's workers, not trust the previous phase's.
         */
        state->workers[i].completed_ts = 0;
        state->workers[i].busy = false;
        state->workers[i].evq.head = state->workers[i].evq.tail = 0;
    }

    /*
     * Reseed the phase's streams: the generator's worker streams first, then the timestamp thread's
     * checkpoint cadence. Every phase draws, whether it generates or not, so the streams stay in
     * step across role switches.
     */
    for (uint32_t i = 0; i <= cfg->nth; i++)
        testutil_random_from_random(
          &state->gen_rnd[i], i < cfg->nth ? &cfg->opts->data_rnd : &cfg->opts->extra_rnd);

    testutil_check(__wt_thread_create(NULL, &ts_thr, thread_ts_run, state));
    node_workers_start(state);

    /* Every phase checkpoints or adopts checkpoints, independent of the event stream. */
    node_ckpt_start(state);

    /* Every phase has a source: this node's own generator, or a live peer's relay. */
    node_reader_start(state);

    /* Start the generator last, once the machinery consuming its stream is up. */
    if (state->generates)
        node_generator_start(state);
    fflush(stdout);
}

/*
 * workload_stop --
 *     Quiesce and join the phase's threads, walking the shutdown stages in order. The checkpoint
 *     and timestamp threads outlive the workers for a reason: a draining worker blocked on a drop
 *     is waiting for exactly what those two do, a frontier that advances and a checkpoint over it.
 */
void
workload_stop(WORKLOAD_STATE *state)
{
    __wt_atomic_store_uint32(&state->stop_stage, STAGE_GENERATOR);
    node_generator_join();

    __wt_atomic_store_uint32(&state->stop_stage, STAGE_READER);
    node_reader_join();

    __wt_atomic_store_uint32(&state->stop_stage, STAGE_WORKERS);
    node_workers_join(state);

    __wt_atomic_store_uint32(&state->stop_stage, STAGE_CKPT);
    node_ckpt_join();

    __wt_atomic_store_uint32(&state->stop_stage, STAGE_TS);
    testutil_check(__wt_thread_join(NULL, &ts_thr));
}

/*
 * node_step_down --
 *     Leader to follower. The term is quiesced and drained, so its final timestamp is the step-down
 *     boundary: nothing more will be committed or published. The checkpoint has to land on that
 *     boundary exactly - WiredTiger asserts it at the role change - and the boundary is declared in
 *     both ordering spaces, the timestamp and the schema epoch.
 *
 * The reconfigure has to precede the hand-over: the page log allows one writer, so this node must
 *     already be a follower before the peer is told to step up. Without a live peer there is
 *     nothing to send and this node continues both sides itself.
 */
static const NODE_ROLE *
node_switch_role(const NODE_ROLE *role)
{
    WT_CONNECTION *conn = state->conn;
    WT_SESSION *session;
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    if (final_ts != 0) {
        char config[128];
        testutil_snprintf(config, sizeof(config),
          "step_down_timestamp=%" PRIx64 ",step_down_disaggregated_schema_epoch=%" PRIx64, final_ts,
          final_ts);
        testutil_check(conn->set_timestamp(conn, config));
        set_frontier(conn, final_ts);
    }
    testutil_check(session->checkpoint(session, "use_timestamp=true"));
    testutil_check(session->close(session, NULL));
    testutil_check(conn->reconfigure(conn, "disaggregated=(role=follower)"));

    SCHEMA_EVENT ev = {0};
    ev.type = EVENT_SWITCH;
    ev.event_ts = final_ts;
    /* Peer death is the only reason a hand-over may go undelivered; the write itself detects it. */
    if (!pipe_relay_event(state->cfg, &ev)) {
        testutil_assert(!state->cfg->peer_alive);
        println("Node %" PRIu32 ": no peer to hand over to; continuing alone", state->cfg->node_id);
    }

    /* Reset adopted checkpoint tracking. */
    state->adopted_ckpt_lsn = 0;
}

/*
 * node_step_up --
 *     Follower to leader.
 */
static void
node_step_up(WORKLOAD_STATE *state, uint64_t final_ts)
{
    ckpt_adopt_latest(state);

    testutil_check(state->conn->reconfigure(state->conn, "disaggregated=(role=leader)"));
    workload_seed_counter(state, final_ts);

    /* Restore the timestamps on the new leader's connection. */
    if (final_ts != 0)
        set_frontier(state->conn, final_ts);
}

/*
 * node_role --
 *     Return the role based on whether the node leads.
 */
const NODE_ROLE *
node_role(bool leads)
{
    static const NODE_ROLE node_role_leader = {"leader", NULL, true, leader_checkpoint};
    static const NODE_ROLE node_role_follower = {
      "follower", "debug=(skip_checkpoint=true)", false, follower_checkpoint};

    return (leads ? &node_role_leader : &node_role_follower);
}

/*
 * node_run --
 *     The node's control loop, one state machine for both roles. Each iteration runs one phase:
 *     start the workload in the current role, wait for the trigger that ends the phase, quiesce,
 *     then switch roles through the role's leave/enter operations. Returns the process exit status
 *     once the parent directs a graceful stop; a SIGKILL can end the process at any point instead.
 */
static int
node_run(TEST_CONFIG *cfg, WORKLOAD_STATE *state, const NODE_ROLE *role)
{
    NODE_TRIGGER trigger;

    do {
        workload_start(state, role->leads);
        trigger = node_trigger_wait(state);
        workload_stop(state);

        if (trigger == TRIGGER_SWITCH) {
            /*
             * The counter the ending term finished on. The workload is quiesced and drained, so the
             * node's own counter holds every value the term allocated or adopted - on a peered
             * hand-over the reader asserted it equals the sender's final counter.
             */
            const uint64_t final_counter = state->current_ts;

            role->leave(state, final_counter);
            role = node_switch_role(role);
            role->enter(state, final_counter);
            println("Node %" PRIu32 ": now %s", cfg->node_id, role->name);
            /* The swap-completing transition: entering leadership, or a lone node's only one. */
            node_transition_done(cfg, state, role->leads || !cfg->peer_alive);
        }
    } while (trigger != TRIGGER_STOP);

    /* The parent directed a graceful stop; the last phase is already quiesced. */
    testutil_check(state->conn->close(state->conn, role->close_config));
    println("Node %" PRIu32 ": stopped gracefully as %s", cfg->node_id, role->name);
    return (EXIT_SUCCESS);
}

/*
 * node_main --
 *     Node role entry point: set the node up in its parent-assigned starting role, hand control to
 *     the state machine, and report its exit status.
 */
int
node_main(TEST_CONFIG *cfg)
{
    /* A dead peer must not kill this node with a pipe signal. */
    if (cfg->pipe_write_fd >= 0)
        (void)signal(SIGPIPE, SIG_IGN);

    /* The node's own event source; both ends live for the process, across every role switch. */
    int self_pipe[2];
    testutil_assert_errno(pipe(self_pipe) == 0);
    cfg->self_pipe_read_fd = self_pipe[0];
    cfg->self_pipe_write_fd = self_pipe[1];

    if (chdir(cfg->home) != 0)
        testutil_die(errno, "Node %" PRIu32 " chdir: %s", cfg->node_id, cfg->home);

    disagg_opts_init(cfg);
    cfg->peer_alive = cfg->pipe_read_fd >= 0;

    WORKLOAD_STATE *state = workload_state_create(cfg);

    const NODE_ROLE *role = cfg->start_leader ? &node_role_leader : &node_role_follower;
    node_open(cfg, role->name, &state->conn);
    /*
     * Enter the epoch world before the workload can publish anything, on either role: a follower
     * publishes the operations it applies too. The allocator starts at the same value, so the first
     * event's epoch is above the stable one.
     */
    workload_seed_counter(state, SCHEMA_EPOCH_BOOTSTRAP);
    set_frontier(state->conn, SCHEMA_EPOCH_BOOTSTRAP);
    println("Node %" PRIu32 ": starting as %s", cfg->node_id, role->name);

    return (node_run(cfg, state, role));
}
