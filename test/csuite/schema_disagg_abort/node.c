/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * The generic node: everything a database node does regardless of role. This file owns the phase
 * loop, the WiredTiger connection, the event pipes, and the workload engine.
 *
 * One pipeline serves both roles: a generator thread produces the node's command stream (the full
 * workload into the self-pipe when leading, only the lone node's switch trigger when following), a
 * reader thread demuxes the source pipe to N worker threads that apply the events, and a timestamp
 * thread advances the frontier. The threads coordinate without locks.
 *
 * The role specifics live in leader.c and follower.c behind the NODE_ROLE operations.
 */

#include "schema_disagg_abort.h"

#include <signal.h>
#include <sys/select.h>

/* The table configuration every schema table is created with, on either role. */
#define SCHEMA_TABLE_CONFIG "key_format=S,value_format=S,type=layered,block_manager=disagg"

/* Thread argument: the shared workload state plus this thread's identity. */
typedef struct {
    WORKLOAD_STATE *state;
    uint32_t thread_index;
} THREAD_ARG;

/* Per-thread worker state for one phase. */
typedef struct {
    WT_SESSION *session;
    FILE *record_fp; /* schema records (leading) or relay records (following); opens lazily */
} WORKER_CTX;

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
 *     Seed the monotonic allocator from the previous leader's high-water mark, so a node stepping
 *     up continues the global epoch/timestamp sequence.
 */
void
workload_seed_counter(WORKLOAD_STATE *state, uint64_t ts)
{
    testutil_assert(state->current_ts <= ts);
    state->current_ts = ts;
}

/*
 * counter_advance --
 *     Advance the monotonic allocator to at least the given applied value, so a follower's counter
 *     tracks everything it applied.
 */
static void
counter_advance(WORKLOAD_STATE *state, uint64_t v)
{
    uint64_t cur;
    do {
        cur = __wt_atomic_load_uint64_acquire(&state->current_ts);
    } while (cur < v && !__wt_atomic_cas_uint64(&state->current_ts, cur, v));
}

/*
 * workload_running --
 *     The condition every phase loop runs on: true until the phase is directed to quiesce.
 */
static bool
workload_running(WORKLOAD_STATE *state)
{
    return (!__wt_atomic_load_bool_acquire(&state->stop_phase));
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
 * node_event_send --
 *     Relay one event to the peer over the node's out-pipe. Single write() calls of
 *     sizeof(SCHEMA_EVENT) are atomic, so concurrent threads need no extra locking. Returns false
 *     without failing when there is no peer (no pipe, or the peer is gone), leaving the caller to
 *     decide whether delivery is optional (the workload relay) or mandatory (the hand-over).
 */
bool
node_event_send(TEST_CONFIG *cfg, const SCHEMA_EVENT *ev)
{
    if (cfg->pipe_write_fd < 0)
        return (false);

    const ssize_t nw = write(cfg->pipe_write_fd, ev, sizeof(*ev));
    if (nw < 0) {
        if (errno != EPIPE && errno != EBADF)
            testutil_die(errno, "write event pipe");
        close(cfg->pipe_write_fd);
        cfg->pipe_write_fd = -1;
        cfg->peer_alive = false;
        return (false);
    }
    testutil_assert(nw == (ssize_t)sizeof(*ev));
    return (true);
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
static bool
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
 *     The stop sentinel ends any phase; the hand-over report ends it with a switch. All switch
 *     triggering lives in the generator, so the trigger arrives here uniformly, whether it came
 *     from the node's own stream, the peer's hand-over, or a lone follower's sentinel poll.
 */
static NODE_TRIGGER
node_trigger_wait(WORKLOAD_STATE *state)
{
    while (!node_stop_requested()) {
        if (__wt_atomic_load_bool_acquire(&state->handover_received))
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
    if (tail - __wt_atomic_load_uint64_acquire(&q->head) >= EVQ_SIZE)
        return (false);
    q->ev[tail % EVQ_SIZE] = *ev;
    __wt_atomic_store_uint64_release(&q->tail, tail + 1);
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
    if (head == __wt_atomic_load_uint64_acquire(&q->tail))
        return (false);
    *ev = q->ev[head % EVQ_SIZE];
    __wt_atomic_store_uint64_release(&q->head, head + 1);
    return (true);
}

/*
 * evq_empty --
 *     Report whether a worker's ring is empty.
 */
static bool
evq_empty(EVENT_QUEUE *q)
{
    return (__wt_atomic_load_uint64_acquire(&q->head) == __wt_atomic_load_uint64_acquire(&q->tail));
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

    EVENT_QUEUE *q = &state->threads[ev->thread_id].evq;
    while (!evq_push(q, ev) && workload_running(state))
        __wt_sleep(0, WT_THOUSAND);
}

/*
 * workload_drain_barrier --
 *     Wait until every worker has applied everything queued so far. The follower's reader runs this
 *     before a checkpoint pickup and before a hand-over: everything at or below the checkpoint's
 *     stable frontier must be applied locally before its metadata is adopted, so later publishes
 *     and commits stay above the adopted stable values.
 */
void
workload_drain_barrier(WORKLOAD_STATE *state)
{
    for (uint32_t t = 0; t < state->nth_workers; t++)
        while ((!evq_empty(&state->threads[t].evq) ||
                 __wt_atomic_load_bool_acquire(&state->threads[t].busy)) &&
          workload_running(state))
            __wt_sleep(0, WT_THOUSAND);
}

/*
 * record_event_line --
 *     Append one event to a record file; the one place that defines the record format for both the
 *     schema and the relay files.
 */
static void
record_event_line(FILE *fp, const SCHEMA_EVENT *ev)
{
    int ret = 0;

    switch (ev->type) {
    case EVENT_CREATE:
    case EVENT_DROP:
        ret = fprintf(fp, "%s %" PRIu64 " %s\n", ev->type == EVENT_CREATE ? "CREATE" : "DROP",
          ev->event_ts, ev->uri);
        break;
    case EVENT_INSERT:
        ret = fprintf(fp, "INSERT %" PRIu64 " %" PRIu32 " %" PRIu32 " %s\n", ev->event_ts,
          ev->key_min, ev->key_max, ev->uri);
        break;
    case EVENT_CKPT:
    case EVENT_SWITCH:
        testutil_assertfmt(false, "Unexpected record event type: %d", ev->type);
    }
    if (ret < 0)
        testutil_die(EIO, "fprintf event record");
}

/*
 * worker_record_open --
 *     Open a worker's record file: schema records when leading, relay records when following.
 *     Append so a later phase preserves the earlier records for the post-crash verifier.
 */
static FILE *
worker_record_open(const WORKLOAD_STATE *state, uint32_t thread_index)
{
    char fname[128];
    if (state->leads)
        testutil_snprintf(
          fname, sizeof(fname), SCHEMA_RECORDS_FILE, state->cfg->node_id, thread_index);
    else
        testutil_snprintf(
          fname, sizeof(fname), RELAY_RECORDS_FILE, state->cfg->node_id, thread_index);

    FILE *fp;
    testutil_assert_errno((fp = fopen(fname, "a")) != NULL);
    /* Flush the record file per line so entries survive a SIGKILL crash. */
    __wt_stream_set_line_buffer(fp);
    return (fp);
}

/*
 * schema_op_execute --
 *     Execute one schema operation: the single call site for creating and dropping the test's
 *     tables, on either role. EBUSY is retried (the stream cannot be reordered, and when the source
 *     is the peer the operation already succeeded there), with a bound so a wedged operation fails
 *     the test instead of hanging it.
 */
static void
schema_op_execute(WT_SESSION *session, const SCHEMA_EVENT *ev)
{
    const bool is_create = ev->type == EVENT_CREATE;
    testutil_assert(ev->type == EVENT_CREATE || ev->type == EVENT_DROP);

    struct timespec start;
    __wt_epoch(NULL, &start);

    int ret;
    for (;;) {
        ret = is_create ? session->create(session, ev->uri, SCHEMA_TABLE_CONFIG) :
                          session->drop(session, ev->uri, "force=false,lock_wait=false");
        if (ret != EBUSY)
            break;

        struct timespec now;
        __wt_epoch(NULL, &now);
        if (WT_TIMEDIFF_SEC(now, start) > MAX_STARTUP)
            testutil_die(ETIMEDOUT, "%s %s: EBUSY for %d seconds", is_create ? "CREATE" : "DROP",
              ev->uri, MAX_STARTUP);
        __wt_yield();
    }
    testutil_assertfmt(ret == 0, "%s %s (ts %" PRIu64 "): %s", is_create ? "CREATE" : "DROP",
      ev->uri, ev->event_ts, wiredtiger_strerror(ret));
}

/*
 * schema_op_publish --
 *     Publish the schema operation at the given epoch so it becomes visible in shared metadata at
 *     the next checkpoint. Runs on both roles: a follower's applied operations queue up and drain
 *     when it eventually leads.
 */
static void
schema_op_publish(WT_SESSION *session, const char *uri, uint64_t epoch)
{
    char pub_cfg[64];
    testutil_snprintf(pub_cfg, sizeof(pub_cfg), "disaggregated=(schema_epoch=%" PRIx64 ")", epoch);
    testutil_check(session->publish(session, uri, pub_cfg));
}

/*
 * schema_op_insert_data --
 *     Populate a table with rows keyed key_min..key_max at the given commit timestamp; each row is
 *     valued with the commit timestamp, so the verifier can tell which generation of a reused table
 *     name wrote the data.
 */
static void
schema_op_insert_data(
  WT_SESSION *session, const char *uri, uint64_t commit_ts, uint32_t key_min, uint32_t key_max)
{
    char val_buf[32];
    testutil_snprintf(val_buf, sizeof(val_buf), "%" PRIu64, commit_ts);
    testutil_check(session->begin_transaction(session, NULL));

    WT_CURSOR *cursor;
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    for (uint32_t r = key_min; r <= key_max; r++) {
        char key_buf[16];
        testutil_snprintf(key_buf, sizeof(key_buf), "%" PRIu32, r);
        cursor->set_key(cursor, key_buf);
        cursor->set_value(cursor, val_buf);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));

    char commit_cfg[64];
    testutil_snprintf(commit_cfg, sizeof(commit_cfg), "commit_timestamp=%" PRIx64, commit_ts);
    testutil_check(session->commit_transaction(session, commit_cfg));
}

/*
 * worker_complete --
 *     Mark one allocator value fully completed by a worker: track it in the counter (a no-op for
 *     freshly allocated values) and publish it as the thread's completed frontier mark.
 */
static void
worker_complete(WORKLOAD_STATE *state, uint32_t thread_index, uint64_t value)
{
    counter_advance(state, value);
    __wt_atomic_store_uint64_release(&state->threads[thread_index].completed_ts, value);
}

/*
 * apply_event --
 *     Apply one event on this node, identically for both roles and exactly as the source stream
 *     fixed it - same operation, same epoch, same commit timestamp: execute the schema operation or
 *     the insert, record the event, publish a schema operation, relay it to the peer when leading,
 *     and mark it completed.
 *
 * The ordering is load-bearing. A schema operation is recorded before it is published, so the
 *     record reaches the file before a checkpoint can make the epoch durable (a record without a
 *     durable epoch is ignored by the verifier, the reverse would be a hole). The relay precedes
 *     the completion store, which is what lets the stable frontier advance past this operation,
 *     what lets a checkpoint cover it, and what lets the checkpoint thread send that checkpoint's
 *     pipe event: the peer holds every event at or below a checkpoint's stable frontier by the time
 *     it sees that checkpoint's event.
 */
static void
apply_event(WORKLOAD_STATE *state, WORKER_CTX *ctx, uint32_t thread_index, const SCHEMA_EVENT *ev)
{
    const bool relay = state->leads;

    if (ctx->record_fp == NULL)
        ctx->record_fp = worker_record_open(state, thread_index);

    switch (ev->type) {
    case EVENT_INSERT:
        schema_op_insert_data(ctx->session, ev->uri, ev->event_ts, ev->key_min, ev->key_max);
        record_event_line(ctx->record_fp, ev);
        if (relay)
            (void)node_event_send(state->cfg, ev);
        worker_complete(state, thread_index, ev->event_ts);
        break;
    case EVENT_CREATE:
    case EVENT_DROP:
        schema_op_execute(ctx->session, ev);
        record_event_line(ctx->record_fp, ev);
        schema_op_publish(ctx->session, ev->uri, ev->event_ts);
        if (relay)
            (void)node_event_send(state->cfg, ev);
        worker_complete(state, thread_index, ev->event_ts);
        break;
    case EVENT_CKPT:
    case EVENT_SWITCH:
        testutil_assertfmt(false, "Unexpected apply event type: %d", ev->type);
    }
}

/*
 * worker_apply_loop --
 *     A worker's phase, identical in both roles: execute whatever the reader queued while the phase
 *     runs, then drain the queue so a graceful stop loses nothing.
 */
static void
worker_apply_loop(WORKLOAD_STATE *state, WORKER_CTX *ctx, uint32_t thread_index)
{
    EVENT_QUEUE *q = &state->threads[thread_index].evq;
    bool *busyp = &state->threads[thread_index].busy;

    while (workload_running(state) || !evq_empty(q)) {
        /* Publish busy before checking the queue so the drain barrier never races an apply. */
        __wt_atomic_store_bool_release(busyp, true);
        SCHEMA_EVENT ev;
        const bool popped = evq_pop(q, &ev);
        if (popped)
            apply_event(state, ctx, thread_index, &ev);
        __wt_atomic_store_bool_release(busyp, false);
        if (!popped)
            __wt_sleep(0, WT_THOUSAND);
    }
}

/*
 * thread_worker_run --
 *     One worker thread: set up the per-phase context, run the processing loop, tear the context
 *     down.
 */
static WT_THREAD_RET
thread_worker_run(void *arg)
{
    const THREAD_ARG *ta = arg;
    WORKLOAD_STATE *state = ta->state;

    WORKER_CTX ctx;
    WT_CLEAR(ctx);
    testutil_check(state->conn->open_session(state->conn, NULL, NULL, &ctx.session));

    worker_apply_loop(state, &ctx, ta->thread_index);

    if (ctx.record_fp != NULL)
        testutil_check(fclose(ctx.record_fp));
    testutil_check(ctx.session->close(ctx.session, NULL));
    return (WT_THREAD_RET_VALUE);
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
        const uint64_t val = __wt_atomic_load_uint64_acquire(&state->threads[i].completed_ts);
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
 *     completed frontier, keeping stable data on published tables only. Runs in both roles; on a
 *     follower, checkpoint pickups may adopt stable values ahead of the local frontier, so the
 *     thread never moves the stable timestamp backwards.
 *
 * It also republishes the connection's durable schema epoch, the gate the generator drops dirty
 *     tables behind. Taking it from the connection rather than from the checkpoint call site keeps
 *     one owner for the value and keeps it right across role transitions: a follower's pickups
 *     advance it too.
 */
static WT_THREAD_RET
thread_ts_run(void *arg)
{
    const THREAD_ARG *ta = arg;
    WORKLOAD_STATE *state = ta->state;

    while (workload_running(state)) {
        /*
         * The single frontier serves both axes: everything at or below it is published and
         * committed, and any commit below it lands in a table created (and published) at a lower
         * value still.
         */
        const uint64_t frontier = workers_min(state);
        if (frontier != 0) {
            const uint64_t cur_stable = query_ts(state->conn, "stable_timestamp");
            if (frontier >= cur_stable)
                set_frontier(state->conn, frontier);
        }

        const uint64_t durable_epoch = query_ts(state->conn, "last_disaggregated_schema_epoch");
        __wt_atomic_store_uint64_release(&state->ckpt_covered_ts, durable_epoch);

        __wt_sleep(0, 100 * WT_THOUSAND);
    }
    return (WT_THREAD_RET_VALUE);
}

/*
 * generator_running --
 *     The generator's loop condition: true until the engine directs it to exit.
 */
static bool
generator_running(WORKLOAD_STATE *state)
{
    return (!__wt_atomic_load_bool_acquire(&state->generator_stop) && workload_running(state));
}

/*
 * generator_emit --
 *     Write one event to the node's self-pipe, blocking while it is full: the workers' consumption
 *     rate backpressures the generator through the pipe and the queues.
 */
static void
generator_emit(WORKLOAD_STATE *state, const SCHEMA_EVENT *ev)
{
    ssize_t nw;
    while ((nw = write(state->cfg->self_pipe_write_fd, ev, sizeof(*ev))) < 0)
        if (errno != EINTR)
            testutil_die(errno, "write self pipe");
    testutil_assert(nw == (ssize_t)sizeof(*ev));
}

/*
 * generator_op --
 *     Generate one schema operation for the given worker thread: pick a slot, flip it between
 *     create and drop, allocate the epoch, and give one create in INSERT_ODDS an insert at a fresh
 *     commit timestamp, which comes from the same allocator as the epoch and so is above the
 *     table's create epoch by construction. Reports whether an event was emitted.
 *
 * Only a slot holding data is gated: a dirty table cannot be dropped until a completed checkpoint
 *     covers its insert (the drop would wedge in EBUSY), so that pick is simply skipped, the
 *     generation-time analogue of trying another slot. Clean tables churn ungated. The slot model
 *     flips at generation time; the worker's bounded EBUSY retry guarantees the executed state
 *     converges to it.
 */
static bool
generator_op(WORKLOAD_STATE *state, uint32_t t)
{
    WT_RAND_STATE *rnd = &state->threads[t].rnd;

    const uint32_t slot = __wt_random(rnd) % state->cfg->pool_size;
    const bool is_create = !state->table_exists[t][slot];
    /* A clean table (commit timestamp 0) is droppable at once; a dirty one waits for coverage. */
    if (!is_create && state->table_commit_ts[t][slot] != 0 &&
      state->table_commit_ts[t][slot] > __wt_atomic_load_uint64_acquire(&state->ckpt_covered_ts))
        return (false);
    state->table_exists[t][slot] = is_create;

    SCHEMA_EVENT ev = {0};
    ev.type = is_create ? EVENT_CREATE : EVENT_DROP;
    ev.thread_id = t;
    ev.event_ts = __wt_atomic_add_uint64(&state->current_ts, 1);
    testutil_snprintf(ev.uri, sizeof(ev.uri), SCHEMA_TABLE_FMT, state->cfg->node_id, t, slot);
    generator_emit(state, &ev);

    if (is_create) {
        /* Most creates leave the table clean, so the churn is not gated on checkpoints. */
        state->table_commit_ts[t][slot] = 0;
        if (__wt_random(rnd) % INSERT_ODDS == 0) {
            ev.type = EVENT_INSERT;
            ev.event_ts = __wt_atomic_add_uint64(&state->current_ts, 1);
            ev.key_min = DATA_KEY_MIN;
            ev.key_max = DATA_KEY_MAX;
            state->table_commit_ts[t][slot] = ev.event_ts;
            generator_emit(state, &ev);
        }
    }
    return (true);
}

/*
 * generator_round --
 *     Feed every worker thread one generated operation, round-robin. Reports whether anything was
 *     emitted: an empty round means every pick was an uncovered drop, and the caller should wait
 *     out the next checkpoint instead of spinning on the slot model.
 */
static bool
generator_round(WORKLOAD_STATE *state)
{
    bool emitted = false;

    for (uint32_t t = 0; t < state->nth_workers && generator_running(state); t++)
        if (generator_op(state, t))
            emitted = true;
    return (emitted);
}

/* A leading generator's pacing state: the checkpoint cadence and the sentinel poll throttle. */
typedef struct {
    WT_RAND_STATE *rnd; /* the generator's own rnd stream, used for the checkpoint intervals */
    struct timespec last_ckpt;
    struct timespec last_poll;
    uint64_t ckpt_wait; /* seconds until the next checkpoint event is due */
} GENERATOR_PACING;

/*
 * generator_pacing_init --
 *     Initialize the pacing state at the start of a leading phase.
 */
static void
generator_pacing_init(GENERATOR_PACING *pacing, WT_RAND_STATE *rnd)
{
    pacing->rnd = rnd;
    __wt_epoch(NULL, &pacing->last_ckpt);
    pacing->last_poll = pacing->last_ckpt;
    pacing->ckpt_wait = __wt_random(rnd) % MAX_CKPT_INVL;
}

/*
 * generator_ckpt_due --
 *     Pace the stream's checkpoint events: true when the current random interval has elapsed,
 *     starting the next one.
 */
static bool
generator_ckpt_due(GENERATOR_PACING *pacing)
{
    struct timespec now;
    __wt_epoch(NULL, &now);
    if ((uint64_t)WT_TIMEDIFF_SEC(now, pacing->last_ckpt) < pacing->ckpt_wait)
        return (false);
    pacing->last_ckpt = now;
    pacing->ckpt_wait = __wt_random(pacing->rnd) % MAX_CKPT_INVL;
    return (true);
}

/*
 * generator_switch_requested --
 *     Watch for the parent's switch request, polling the sentinel at most once a second (the
 *     cadence the control loop's own waits use).
 */
static bool
generator_switch_requested(GENERATOR_PACING *pacing)
{
    struct timespec now;
    __wt_epoch(NULL, &now);
    if (WT_TIMEDIFF_SEC(now, pacing->last_poll) < 1)
        return (false);
    pacing->last_poll = now;
    return (node_switch_request_consume());
}

/*
 * generator_lead --
 *     A leading generator's phase: produce the term's event stream into the self-pipe - workload
 *     rounds, a checkpoint event whenever one is due, and, once the parent requests a switch, the
 *     hand-over event that ends the stream and the phase.
 */
static void
generator_lead(WORKLOAD_STATE *state, WT_RAND_STATE *rnd)
{
    GENERATOR_PACING pacing;
    generator_pacing_init(&pacing, rnd);

    while (generator_running(state)) {
        if (!generator_round(state))
            __wt_sleep(0, WT_THOUSAND);

        if (generator_ckpt_due(&pacing)) {
            SCHEMA_EVENT ev = {0};
            ev.type = EVENT_CKPT;
            generator_emit(state, &ev);
        }

        if (generator_switch_requested(&pacing)) {
            /* The stream's last event, carrying the counter the next leader continues from. */
            SCHEMA_EVENT ev = {0};
            ev.type = EVENT_SWITCH;
            ev.event_ts = __wt_atomic_load_uint64_acquire(&state->current_ts);
            generator_emit(state, &ev);
            return;
        }
    }
}

/*
 * generator_follow --
 *     A following generator's phase: watch for the parent's switch request on a lone node's behalf
 *     and report the hand-over directly, since a lone follower has no reader or in-flight stream to
 *     order against. A peered follower must not act: its hand-over arrives through the peer's
 *     stream.
 */
static void
generator_follow(WORKLOAD_STATE *state)
{
    while (generator_running(state)) {
        if (!state->cfg->peer_alive && node_switch_request_consume()) {
            __wt_atomic_store_bool_release(&state->handover_received, true);
            return;
        }
        __wt_sleep(1, 0);
    }
}

/*
 * thread_generator_run --
 *     The node's command source, one per phase in either role: the whole event stream when leading,
 *     only the lone node's switch trigger when following. Either way, all switch triggering lives
 *     in the generated stream or here, never in the control loop.
 */
static WT_THREAD_RET
thread_generator_run(void *arg)
{
    const THREAD_ARG *ta = arg;
    WORKLOAD_STATE *state = ta->state;

    if (state->leads)
        generator_lead(state, &state->threads[ta->thread_index].rnd);
    else
        generator_follow(state);
    return (WT_THREAD_RET_VALUE);
}

/* The reader thread's handle; its context and results live in the workload state. */
static wt_thread_t reader_thr;
static bool reader_started = false;

/*
 * pipe_wait_readable --
 *     Wait up to a second for the pipe to become readable, so the reader can notice a stop request
 *     even when the source is silent.
 */
static bool
pipe_wait_readable(int fd)
{
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);

    struct timeval tv = {1, 0};
    const int ret = select(fd + 1, &rfds, NULL, NULL, &tv);
    if (ret < 0) {
        if (errno == EINTR)
            return (false);
        testutil_die(errno, "reader select pipe");
    }
    return (ret > 0);
}

/*
 * pipe_read_event --
 *     Read one complete event from the pipe. Returns false on EOF (the writer died). The writer's
 *     death can truncate the final write, so reassemble the event from partial reads.
 */
static bool
pipe_read_event(int fd, SCHEMA_EVENT *ev)
{
    size_t have = 0;
    while (have < sizeof(*ev)) {
        const ssize_t nr = read(fd, (uint8_t *)ev + have, sizeof(*ev) - have);
        if (nr < 0) {
            if (errno == EINTR)
                continue;
            testutil_die(errno, "reader read pipe");
        }
        if (nr == 0)
            return (false);
        have += (size_t)nr;
    }
    return (true);
}

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
 * thread_reader_run --
 *     Drain the node's event source: the self-pipe when leading, the peer's pipe when following.
 *     Schema and data events are queued for the worker threads; a checkpoint event is produced
 *     (leading) or picked up after a drain barrier (following); the hand-over event ends the phase.
 *     Pipe EOF can only happen on a peer-fed pipe: it marks the peer dead and turns this node into
 *     a lone follower.
 */
static WT_THREAD_RET
thread_reader_run(void *arg)
{
    WORKLOAD_STATE *state = arg;
    TEST_CONFIG *cfg = state->cfg;
    const int src_fd = state->leads ? cfg->self_pipe_read_fd : cfg->pipe_read_fd;

    WT_SESSION *session;
    testutil_check(state->conn->open_session(state->conn, NULL, NULL, &session));

    /* The role's checkpoint bookkeeping for this phase; only a follower picks checkpoints up. */
    CKPT_CTX ckpt = {0};
    __wt_epoch(NULL, &ckpt.phase_start);
    if (!state->leads)
        testutil_check(state->conn->get_page_log(state->conn, "palite", &ckpt.page_log));

    SCHEMA_EVENT ev;
    bool running = true;
    while (running && !__wt_atomic_load_bool_acquire(&state->reader_stop)) {
        if (!pipe_wait_readable(src_fd))
            continue;
        if (!pipe_read_event(src_fd, &ev)) {
            /* EOF: the peer died. Keep the role; the node continues as a lone follower. */
            testutil_assert(!state->leads); /* The self-pipe's writer lives in this process. */
            cfg->peer_alive = false;
            println("Node %" PRIu32 ": peer died; continuing as a lone follower", cfg->node_id);
            running = false;
            continue;
        }

        switch (ev.type) {
        case EVENT_CREATE:
        case EVENT_DROP:
        case EVENT_INSERT:
            workload_enqueue(state, &ev);
            break;
        case EVENT_CKPT:
            node_current_role(state)->checkpoint(state, session, &ckpt, &ev);
            break;
        case EVENT_SWITCH:
            /* The final event of the term's stream: this node must step up. */
            workload_drain_barrier(state);
            /*
             * Relay-integrity check: the drained counter must equal the sender's high-water mark.
             * Every counter value the term allocated rides an event that precedes the switch in the
             * stream, so after the drain nothing may be missing.
             */
            testutil_assertfmt(__wt_atomic_load_uint64_acquire(&state->current_ts) == ev.event_ts,
              "hand-over: drained counter %" PRIu64 " != sender high-water %" PRIu64,
              __wt_atomic_load_uint64_acquire(&state->current_ts), ev.event_ts);
            __wt_atomic_store_bool_release(&state->handover_received, true);
            running = false;
            break;
        }
    }

    if (ckpt.page_log != NULL)
        testutil_check(ckpt.page_log->terminate(ckpt.page_log, NULL));
    testutil_check(session->close(session, NULL));
    return (WT_THREAD_RET_VALUE);
}

/*
 * node_reader_start --
 *     Start the reader thread for a phase with an event source: any leader phase (the self-pipe),
 *     or a follower phase with a live peer. The per-phase hand-over and stop fields were reset by
 *     workload_start.
 */
static void
node_reader_start(WORKLOAD_STATE *state)
{
    testutil_assert(!reader_started);
    testutil_check(__wt_thread_create(NULL, &reader_thr, thread_reader_run, state));
    reader_started = true;
}

/*
 * node_reader_stop --
 *     Stop and join the reader thread, if one is running.
 */
static void
node_reader_stop(WORKLOAD_STATE *state)
{
    if (!reader_started)
        return;
    __wt_atomic_store_bool_release(&state->reader_stop, true);
    testutil_check(__wt_thread_join(NULL, &reader_thr));
    reader_started = false;
}

/* Thread handles have process lifetime; phases join and restart them but never free them. */
static wt_thread_t workload_thr[MAX_TH + 2];
static THREAD_ARG workload_arg[MAX_TH + 2];

/*
 * workload_start --
 *     Start one phase's threads, the same set in either role: N event-processing workers, the
 *     timestamp thread, the reader (when the phase has an event source), and the generator. Only
 *     the event source differs by role: a leader phase consumes its own generated stream, a
 *     follower phase consumes the peer's.
 */
void
workload_start(WORKLOAD_STATE *state, bool as_leader)
{
    TEST_CONFIG *cfg = state->cfg;
    testutil_assert(cfg->nth <= MAX_TH);

    state->nth_workers = cfg->nth;
    state->leads = as_leader;
    state->stop_phase = false;
    state->reader_stop = false;
    state->generator_stop = false;
    state->handover_received = false;
    /* Start gated: the timestamp thread republishes the connection's durable epoch immediately. */
    state->ckpt_covered_ts = 0;

    for (uint32_t i = 0; i < cfg->nth + 2; i++) {
        workload_arg[i].state = state;
        workload_arg[i].thread_index = i;
        testutil_random_from_random(
          &state->threads[i].rnd, i < cfg->nth ? &cfg->opts->data_rnd : &cfg->opts->extra_rnd);
        /*
         * The stable frontier must wait for this phase's workers, not trust the previous phase's.
         */
        state->threads[i].completed_ts = 0;
        state->threads[i].busy = false;
        state->threads[i].evq.head = state->threads[i].evq.tail = 0;
    }

    /* Start timestamp thread. */
    testutil_check(__wt_thread_create(
      NULL, &workload_thr[cfg->nth + 1], thread_ts_run, &workload_arg[cfg->nth + 1]));

    /* Start worker threads. */
    for (uint32_t i = 0; i < cfg->nth; i++)
        testutil_check(
          __wt_thread_create(NULL, &workload_thr[i], thread_worker_run, &workload_arg[i]));

    /* The queue source's producer: the reader drains the self-pipe or a live peer's pipe. */
    if (as_leader || cfg->peer_alive)
        node_reader_start(state);

    /* Start the generator last, once the machinery consuming its stream is up. */
    testutil_check(__wt_thread_create(
      NULL, &workload_thr[cfg->nth], thread_generator_run, &workload_arg[cfg->nth]));
    fflush(stdout);
}

/*
 * workload_stop --
 *     Quiesce and join all of the phase's threads, in dependency order. The generator goes first,
 *     while the reader still drains the self-pipe the generator may be blocked on; the reader next,
 *     while the workers are still consuming; then the workers drain what the reader delivered
 *     before exiting.
 */
void
workload_stop(WORKLOAD_STATE *state)
{
    __wt_atomic_store_bool_release(&state->generator_stop, true);
    testutil_check(__wt_thread_join(NULL, &workload_thr[state->nth_workers]));

    node_reader_stop(state);

    __wt_atomic_store_bool_release(&state->stop_phase, true);
    for (uint32_t i = 0; i < state->nth_workers + 2; ++i)
        if (i != state->nth_workers)
            testutil_check(__wt_thread_join(NULL, &workload_thr[i]));
}

/*
 * node_switch_role --
 *     Return the opposite role instance: follower if the current role is leader, and vice versa.
 */
static const NODE_ROLE *
node_switch_role(const NODE_ROLE *role)
{
    return (role->leads ? &node_role_follower : &node_role_leader);
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
             * The ending term's counter high-water mark. The workload is quiesced and drained, so
             * the node's own counter holds every value the term allocated or adopted - on a peered
             * hand-over the reader asserted it equals the sender's high-water mark.
             */
            const uint64_t counter_hwm = state->current_ts;

            role->leave(state, counter_hwm);
            role = node_switch_role(role);
            role->enter(state, counter_hwm);
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
