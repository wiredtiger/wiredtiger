/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * The generator stage: the node's command stream - workload, the step-down event, and the switch
 * event that ends a term - written into the self-pipe. Started only for a phase that produces its
 * own stream: a leader always does, and so does a follower with no peer.
 */

#include "schema_disagg_abort.h"

/* The generator's state machine. */
typedef enum {
    GEN_NORMAL,         /* the term's workload */
    GEN_BEGIN_STEPDOWN, /* one-shot: the step-down event, reserving epochs for unpublished ops */
    GEN_STEPDOWN,       /* the step-down: limited workload */
    GEN_HANDOVER,       /* one-shot: publish what is still pending, then emit the switch event */
    GEN_STOP            /* terminal: the phase stopped, or the stream ended */
} GENERATOR_PHASE;

/*
 * generator_emit --
 *     Write one event to the node's self-pipe, blocking while it is full: the workers' consumption
 *     rate backpressures the generator through the pipe and the queues.
 */
static void
generator_emit(WORKLOAD_STATE *state, const SCHEMA_EVENT *ev)
{
    testutil_assert(ev->type != EVENT_NONE);
    pipe_event_write(state->cfg->self_pipe_write_fd, ev);
    ++state->emitted;
}

/*
 * generator_emit_slot --
 *     Address and emit an event for one table slot.
 */
static void
generator_emit_slot(WORKLOAD_STATE *state, uint32_t thread_index, uint32_t slot, SCHEMA_EVENT *ev)
{
    ev->thread_id = thread_index;
    ev->slot = slot;
    testutil_snprintf(
      ev->uri, sizeof(ev->uri), SCHEMA_TABLE_FMT, state->cfg->node_id, thread_index, slot);
    generator_emit(state, ev);
}

/*
 * generator_publish --
 *     Emit a slot's pending publish using its scheduled timestamp source.
 */
static bool
generator_publish(WORKLOAD_STATE *state, uint32_t thread_index, uint32_t slot)
{
    TABLE_STATE *slot_state = &state->workers[thread_index].table[slot].state;
    PUBLISH_TIMESTAMP_SOURCE *publish_ts_source =
      &state->workers[thread_index].table[slot].publish_ts_source;
    SCHEMA_EVENT ev = {0};

    switch (*slot_state) {
    case TABLE_CREATED:
        ev.type = EVENT_PUBLISH_CREATE;
        *slot_state = TABLE_PUBLISHED;
        break;
    case TABLE_DROPPED:
        ev.type = EVENT_PUBLISH_DROP;
        *slot_state = TABLE_REMOVED;
        break;
    case TABLE_NONE:
    case TABLE_PUBLISHED:
    case TABLE_REMOVED:
        testutil_assert(*publish_ts_source == PUBLISH_TS_CURRENT);
        return (false);
    }
    testutil_assert(!state->cfg->epoch_less);

    ev.publish_ts_source = *publish_ts_source;
    switch (ev.publish_ts_source) {
    case PUBLISH_TS_CURRENT:
        break;
    case PUBLISH_TS_STEPDOWN:
        testutil_assert(state->stepdown_publish_remaining > 0);
        --state->stepdown_publish_remaining;
        *publish_ts_source = PUBLISH_TS_CURRENT;
        break;
    }
    generator_emit_slot(state, thread_index, slot, &ev);
    return (true);
}

/*
 * generator_slot_droppable --
 *     Whether this slot's table can be dropped now.
 */
static bool
generator_slot_droppable(WORKLOAD_STATE *state, uint32_t t, uint32_t slot, bool stepping_down)
{
    if (state->workers[t].table[slot].uncovered_insert)
        return (false);

    /*
     * A drop can wait on the step-down checkpoint, which waits on the reserved publishes: one
     * queued behind the drop would never apply.
     */
    if (stepping_down && __wt_atomic_load_uint64(&state->stepdown_ckpt_lsn) == 0)
        return (false);

    /* Legacy mode has no epochs to cover, and a lone node or a dead peer has nobody to protect. */
    if (state->cfg->epoch_less || node_is_lone(state->cfg) || !node_peer_alive(state->cfg))
        return (true);

    const uint64_t create_epoch =
      __wt_atomic_load_uint64(&state->workers[t].table[slot].create_epoch);
    return (create_epoch != WT_SCHEMA_EPOCH_NONE &&
      __wt_atomic_load_uint64(&state->adopted_ckpt_epoch) >= create_epoch);
}

/*
 * generator_op --
 *     Advance one slot of the given worker thread through the table lifecycle, taking one of its
 *     state's valid moves at random. Reports whether an event was emitted; taking no move is valid,
 *     and lingering widens the window a checkpoint can land in.
 *
 * A recreate waits until the stable schema epoch passes the slot's published drop.
 */
static bool
generator_op(WORKLOAD_STATE *state, uint32_t t, uint32_t slot, GENERATOR_PHASE phase)
{
    WT_RAND_STATE *rnd = &state->workers[t].rnd;
    TABLE_STATE *slot_state = &state->workers[t].table[slot].state;
    uint64_t *create_epoch = &state->workers[t].table[slot].create_epoch;
    uint64_t *drop_epoch = &state->workers[t].table[slot].drop_epoch;
    /* Set when no checkpoint of this phase can cover the insert; such a slot is not droppable. */
    bool *uncovered_insert = &state->workers[t].table[slot].uncovered_insert;

    const bool stepping_down = phase == GEN_STEPDOWN;
    SCHEMA_EVENT ev = {0}; /* EVENT_NONE until a move is taken */
    switch (*slot_state) {
    case TABLE_NONE:
        /* A legacy create is complete immediately; epoch mode publishes it in a later event. */
        ev.type = EVENT_CREATE;
        *slot_state = state->cfg->epoch_less ? TABLE_PUBLISHED : TABLE_CREATED;
        break;
    case TABLE_CREATED:
        testutil_assert(!state->cfg->epoch_less);
        switch (__wt_random(rnd) % 3) {
        case 0: /* publish the create */
            return (generator_publish(state, t, slot));
        case 1: /* cancel it: an unpublished create dropped again leaves no trace */
            ev.type = EVENT_DROP;
            *slot_state = TABLE_NONE;
            break;
        default: /* linger, widening the op-publish window */
            break;
        }
        break;
    case TABLE_PUBLISHED:
        /* Take (more) data, drop the table, or linger. */
        if (__wt_random(rnd) % GEN_INSERT_ODDS == 0) {
            ev.type = EVENT_INSERT;
            /* No checkpoint of this phase can cover it, so the slot stops being droppable. */
            if (stepping_down || !state->leads)
                *uncovered_insert = true;
        } else if (__wt_random(rnd) % GEN_DROP_ODDS == 0 &&
          generator_slot_droppable(state, t, slot, stepping_down)) {
            ev.type = EVENT_DROP;
            /* Consume the create's epoch: the next table in this slot publishes its own. */
            __wt_atomic_store_uint64(create_epoch, WT_SCHEMA_EPOCH_NONE);
            *slot_state = state->cfg->epoch_less ? TABLE_NONE : TABLE_DROPPED;
        }
        break;
    case TABLE_DROPPED:
        testutil_assert(!state->cfg->epoch_less);
        /* Publish the drop, or linger in the window. */
        if (__wt_random(rnd) % 2 == 0)
            return (generator_publish(state, t, slot));
        break;
    case TABLE_REMOVED: {
        /*
         * Free the slot once the stable epoch passes the published drop; zero is not applied yet.
         */
        const uint64_t published_drop = __wt_atomic_load_uint64(drop_epoch);
        if (published_drop != WT_SCHEMA_EPOCH_NONE &&
          __wt_atomic_load_uint64(&state->stable_epoch) >= published_drop) {
            __wt_atomic_store_uint64(drop_epoch, WT_SCHEMA_EPOCH_NONE);
            *slot_state = TABLE_NONE;
        }
        break;
    }
    }
    if (ev.type == EVENT_NONE)
        return (false);

    if (ev.type == EVENT_INSERT) {
        ev.key_min = DATA_KEY_MIN;
        ev.key_max = DATA_KEY_MAX;
    }

    generator_emit_slot(state, t, slot, &ev);
    return (true);
}

/*
 * generator_round --
 *     Feed every worker thread one generated operation, round-robin. Reports whether anything was
 *     emitted: nothing is while the lead over the workers is spent, or when every pick was an
 *     uncovered drop, and the caller waits instead of spinning on the slot model.
 */
static bool
generator_round(WORKLOAD_STATE *state, uint64_t lead_max, GENERATOR_PHASE phase)
{
    if (state->emitted - __wt_atomic_load_uint64(&state->applied) > lead_max)
        return (false);

    bool emitted = false;

    for (uint32_t thread_index = 0;
      thread_index < state->worker_count && workload_active(state, STAGE_GENERATOR);
      thread_index++) {
        const uint32_t slot =
          __wt_random(&state->workers[thread_index].rnd) % state->cfg->pool_size;
        switch (state->workers[thread_index].table[slot].publish_ts_source) {
        case PUBLISH_TS_CURRENT:
            if (generator_op(state, thread_index, slot, phase))
                emitted = true;
            break;
        case PUBLISH_TS_STEPDOWN:
            testutil_assert(generator_publish(state, thread_index, slot));
            emitted = true;
            break;
        }
    }
    return (emitted);
}

/* A leading generator's pacing state. */
typedef struct {
    uint64_t lead_max; /* events that may be in flight */
    struct timespec last_poll;

    uint64_t stepdown_emitted;      /* events emitted since the step-down started */
    struct timespec stepdown_start; /* when the step-down event is emitted */
} GENERATOR_PACING;

/*
 * generator_pacing_init --
 *     Initialize the pacing state at the start of a leading phase.
 */
static void
generator_pacing_init(GENERATOR_PACING *pacing, const TEST_CONFIG *cfg)
{
    WT_CLEAR(*pacing);
    __wt_epoch(NULL, &pacing->last_poll);
    /*
     * How much lead the generator may have over the workers: enough to keep every worker fed, and
     * enough that a role switch, which drains what is queued first, completes in reasonable time.
     * Bounding it also keeps a graceful stop prompt, since the stop drains the same queues.
     */
    pacing->lead_max = WT_MAX((uint64_t)cfg->thread_count * GEN_LEAD_PER_THREAD,
      (uint64_t)cfg->switch_interval * GEN_APPLY_RATE_FLOOR);
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
 * generator_publish_batch_capture --
 *     Capture pending publishes for the step-down timestamp reservation without changing lifecycle
 *     states.
 */
static uint32_t
generator_publish_batch_capture(WORKLOAD_STATE *state)
{
    testutil_assert(state->stepdown_publish_remaining == 0);
    for (uint32_t thread_index = 0; thread_index < state->worker_count; thread_index++)
        for (uint32_t slot = 0; slot < state->cfg->pool_size; slot++) {
            testutil_assert(
              state->workers[thread_index].table[slot].publish_ts_source == PUBLISH_TS_CURRENT);
            switch (state->workers[thread_index].table[slot].state) {
            case TABLE_CREATED:
            case TABLE_DROPPED:
                state->workers[thread_index].table[slot].publish_ts_source = PUBLISH_TS_STEPDOWN;
                ++state->stepdown_publish_remaining;
                break;
            case TABLE_NONE:
            case TABLE_PUBLISHED:
            case TABLE_REMOVED:
                break;
            }
        }
    return (state->stepdown_publish_remaining);
}

/*
 * generator_publish_pending --
 *     Emit all pending publishes using the given timestamp source.
 */
static void
generator_publish_pending(WORKLOAD_STATE *state, PUBLISH_TIMESTAMP_SOURCE source)
{
    for (uint32_t thread_index = 0; thread_index < state->worker_count; thread_index++)
        for (uint32_t slot = 0; slot < state->cfg->pool_size; slot++)
            if (state->workers[thread_index].table[slot].publish_ts_source == source)
                (void)generator_publish(state, thread_index, slot);
}

/*
 * generator_stepdown_ended --
 *     Whether the step-down may complete: the peer adopted the step-down checkpoint or died, or a
 *     lone node emitted its share of events.
 */
static bool
generator_stepdown_ended(WORKLOAD_STATE *state, GENERATOR_PACING *pacing)
{
    /* Zero until the reader's step-down work completes. */
    const uint64_t ckpt_lsn = __wt_atomic_load_uint64(&state->stepdown_ckpt_lsn);
    const bool lone = node_is_lone(state->cfg);

    /* A dead peer cannot adopt the checkpoint. */
    if (ckpt_lsn != 0 && !lone && !node_peer_alive(state->cfg))
        return (true);

    struct timespec now;
    __wt_epoch(NULL, &now);
    if (WT_TIMEDIFF_SEC(now, pacing->last_poll) < 1)
        return (false); /* Too early, come back later. */
    pacing->last_poll = now;

    const uint64_t stepdown_events = state->emitted - pacing->stepdown_emitted;
    /* Lone node exhausted step-down events or peer adopted the checkpoint. */
    const bool ended = ckpt_lsn != 0 &&
      (lone ? stepdown_events >= GEN_STEPDOWN_MIN_EVENTS : adopted_ckpt_read(NULL) >= ckpt_lsn);

    if (ended)
        return (true);

    /* Report which part of the step-down stalled. */
    if (WT_TIMEDIFF_SEC(now, pacing->stepdown_start) > MAX_STEPDOWN_WAIT) {
        if (ckpt_lsn == 0)
            testutil_die(ETIMEDOUT,
              "Node %" PRIu32 ": the step-down checkpoint did not complete in %d seconds",
              state->cfg->node_id, MAX_STEPDOWN_WAIT);
        else if (lone)
            testutil_die(ETIMEDOUT,
              "Node %" PRIu32 ": the step-down emitted %" PRIu64 " of %d events in %d seconds",
              state->cfg->node_id, stepdown_events, GEN_STEPDOWN_MIN_EVENTS, MAX_STEPDOWN_WAIT);
        else
            testutil_die(ETIMEDOUT,
              "Node %" PRIu32 ": peer did not adopt the step-down checkpoint (lsn %" PRIu64
              ") in %d seconds",
              state->cfg->node_id, ckpt_lsn, MAX_STEPDOWN_WAIT);
    }

    return (false);
}

/*
 * generator_transition_emit --
 *     Emit a transition event (stepdown or switch), with the number of publish timestamps to
 *     reserve.
 */
static void
generator_transition_emit(WORKLOAD_STATE *state, EVENT_TYPE type, uint32_t publish_count)
{
    SCHEMA_EVENT ev = {0};
    ev.type = type;
    ev.publish_count = publish_count;
    generator_emit(state, &ev);
}

/*
 * thread_generator_run --
 *     The generator thread procedure.
 */
WT_THREAD_RET
thread_generator_run(void *arg)
{
    WORKLOAD_STATE *state = arg;

    GENERATOR_PACING pacing;
    generator_pacing_init(&pacing, state->cfg);

    GENERATOR_PHASE phase = GEN_NORMAL;

    while (phase != GEN_STOP && workload_active(state, STAGE_GENERATOR)) {
        /*
         * Transition-only states count as progress so the loop does not sleep between their
         * actions.
         */
        bool progressed = true;

        switch (phase) {
        case GEN_NORMAL:
            progressed = generator_round(state, pacing.lead_max, GEN_NORMAL);
            /* Only a leader steps down; a lone follower goes straight to the hand-over. */
            if (generator_switch_requested(&pacing))
                phase = state->leads ? GEN_BEGIN_STEPDOWN : GEN_HANDOVER;
            break;
        case GEN_BEGIN_STEPDOWN:
            /*
             * The step-down event reserves an epoch at or below the step-down timestamp for each
             * unpublished operation. The step-down workload, paced from here, publishes them among
             * its own operations, and the step-down checkpoint carries them.
             */
            generator_transition_emit(
              state, EVENT_STEPDOWN, generator_publish_batch_capture(state));
            __wt_epoch(NULL, &pacing.stepdown_start);
            pacing.stepdown_emitted = state->emitted;
            phase = GEN_STEPDOWN;
            break;
        case GEN_STEPDOWN: {
            /* A dead peer cannot carry the operations, so stop adding them. */
            const bool carried = node_is_lone(state->cfg) || node_peer_alive(state->cfg);
            progressed = carried && generator_round(state, pacing.lead_max, GEN_STEPDOWN);
            /*
             * The step-down checkpoint waits on the reserved publishes: once the step-down has
             * emitted its share of events, or nobody carries them, publish the rest at once.
             */
            if (state->stepdown_publish_remaining != 0 &&
              (!carried || state->emitted - pacing.stepdown_emitted >= GEN_STEPDOWN_MIN_EVENTS)) {
                generator_publish_pending(state, PUBLISH_TS_STEPDOWN);
                testutil_assert(state->stepdown_publish_remaining == 0);
            }
            if (generator_stepdown_ended(state, &pacing)) {
                struct timespec now;
                __wt_epoch(NULL, &now);
                println("Node %" PRIu32 ": step-down emitted %" PRIu64 " events in %" PRIu64
                        " ms (budget %d s)",
                  state->cfg->node_id, state->emitted - pacing.stepdown_emitted,
                  (uint64_t)WT_TIMEDIFF_MS(now, pacing.stepdown_start), MAX_STEPDOWN_WAIT);
                phase = GEN_HANDOVER;
            }
            break;
        }
        case GEN_HANDOVER:
            /* Nothing the term originated may stay unpublished past the switch event. */
            testutil_assert(state->stepdown_publish_remaining == 0);
            generator_publish_pending(state, PUBLISH_TS_CURRENT);
            generator_transition_emit(state, EVENT_SWITCH, 0);
            phase = GEN_STOP;
            break;
        case GEN_STOP:
            break;
        }

        if (!progressed)
            __wt_sleep(0, 10 * WT_THOUSAND); /* 10 ms */
    }

    return (WT_THREAD_RET_VALUE);
}
