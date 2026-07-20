/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/*
 * Leader role: opens the database as a disaggregated leader and runs schema worker threads, a
 * checkpoint thread, and a timestamp thread until the parent kills the process. In multi-node mode
 * every schema operation and checkpoint is relayed to the follower over the event pipe.
 */

#include "schema_disagg_abort.h"

#include <signal.h>

/* Forward declaration: WORKLOAD_STATE holds a pointer into the THREAD_DATA array. */
typedef struct __thread_data THREAD_DATA;

/*
 * Global state shared by all workload threads. The threads coordinate without locks (WT-18084: a
 * rwlock here starved the checkpoint thread under stress).
 */
typedef struct {
    bool stable_set; /* set once the stable timestamp is first advanced; atomic access */
    bool stop_phase; /* set to quiesce all worker threads between phases; atomic access */
    /* Leader phases checkpoint; a follower phase only advances the epoch. Fixed per phase. */
    bool ckpt_enabled;
    /*
     * Monotonic allocators. Every publish and commit must draw a value above the global stable
     * epoch and timestamp, so these only ever increase.
     */
    uint64_t next_epoch;
    uint64_t next_commit_ts;
    THREAD_DATA *workers; /* schema worker thread data array (length nth_workers) */
    uint32_t nth_workers;
} WORKLOAD_STATE;

/* Per-thread argument. */
struct __thread_data {
    TEST_CONFIG *cfg;
    WT_CONNECTION *conn;
    WORKLOAD_STATE *state;
    uint32_t info;
    WT_RAND_STATE rnd;
    /*
     * The timestamp thread takes the minimum of each field across all workers to set the global
     * stable epoch and stable timestamp. stable_ready_ts trails the thread's commits until each
     * insert's table epoch is stable.
     */
    uint64_t published_epoch;
    uint64_t stable_ready_ts;
    /* Seeded from the caller and copied back so a role switch carries table state across phases. */
    bool table_exists[MAX_POOL_SIZE];
};

/* Per-thread schema worker state. */
typedef struct {
    WT_SESSION *session;
    WORKLOAD_STATE *state;
    WT_RAND_STATE *rnd;
    FILE *schema_fp;
    char tableconf[128];
    char uris[MAX_POOL_SIZE][64];
    bool table_exists[MAX_POOL_SIZE];
} SCHEMA_WORKER_CTX;

/*
 * pipe_write_event --
 *     Relay one event to the follower. Single write() calls of sizeof(SCHEMA_EVENT) are atomic, so
 *     concurrent threads need no extra locking. Stops relaying silently once the follower is gone.
 */
static void
pipe_write_event(TEST_CONFIG *cfg, const SCHEMA_EVENT *ev)
{
    if (cfg->pipe_write_fd < 0)
        return;

    const ssize_t nw = write(cfg->pipe_write_fd, ev, sizeof(*ev));
    if (nw < 0) {
        if (errno == EPIPE || errno == EBADF) {
            close(cfg->pipe_write_fd);
            cfg->pipe_write_fd = -1;
        } else
            testutil_die(errno, "write schema pipe");
    } else
        testutil_assert(nw == (ssize_t)sizeof(*ev));
}

/*
 * schema_worker_open --
 *     Open the session, record file, and URI table for a schema worker thread.
 */
static void
schema_worker_open(THREAD_DATA *td, SCHEMA_WORKER_CTX *ctx)
{
    /* Append so a later phase preserves the earlier phase's records for the post-crash verifier. */
    char fname[128];
    testutil_snprintf(fname, sizeof(fname), SCHEMA_RECORDS_FILE, td->info);
    testutil_assert_errno((ctx->schema_fp = fopen(fname, "a")) != NULL);
    /* Flush the record file per line so entries survive the SIGKILL crash. */
    __wt_stream_set_line_buffer(ctx->schema_fp);

    for (uint32_t i = 0; i < td->cfg->pool_size; i++)
        testutil_snprintf(ctx->uris[i], sizeof(ctx->uris[i]), SCHEMA_TABLE_FMT, td->info, i);

    /* Resume from the carried-over table state so a role switch continues where phase 1 left off.
     */
    memcpy(ctx->table_exists, td->table_exists, sizeof(ctx->table_exists));

    ctx->rnd = &td->rnd;
    ctx->state = td->state;
    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &ctx->session));
    testutil_snprintf(ctx->tableconf, sizeof(ctx->tableconf),
      "key_format=S,value_format=S,type=layered,block_manager=disagg");
}

/*
 * schema_op_execute --
 *     Execute the next schema operation on the given slot and update the caller's table-exists
 *     state.
 */
static int
schema_op_execute(SCHEMA_WORKER_CTX *ctx, uint64_t slot)
{
    const bool is_create = !ctx->table_exists[slot];

    const int ret = is_create ?
      ctx->session->create(ctx->session, ctx->uris[slot], ctx->tableconf) :
      ctx->session->drop(ctx->session, ctx->uris[slot], "force=false,lock_wait=false");
    if (ret == EBUSY)
        return (ret);
    testutil_check(ret);
    ctx->table_exists[slot] = is_create;

    return (0);
}

/*
 * schema_op_publish --
 *     Publish the schema operation at the given epoch so it is visible to followers. Must be called
 *     for both CREATE and DROP.
 */
static int
schema_op_publish(SCHEMA_WORKER_CTX *ctx, uint64_t slot, uint64_t epoch)
{
    char pub_cfg[64];
    testutil_snprintf(pub_cfg, sizeof(pub_cfg), "disaggregated=(schema_epoch=%" PRIx64 ")", epoch);
    return (ctx->session->publish(ctx->session, ctx->uris[slot], pub_cfg));
}

/*
 * schema_op_insert_data --
 *     Populate a newly created table with rows keyed DATA_KEY_MIN..DATA_KEY_MAX, each valued with
 *     the epoch, at the given commit timestamp.
 */
static void
schema_op_insert_data(SCHEMA_WORKER_CTX *ctx, uint64_t slot, uint64_t epoch, uint64_t commit_ts)
{
    char val_buf[32];
    testutil_snprintf(val_buf, sizeof(val_buf), "%" PRIu64, epoch);
    testutil_check(ctx->session->begin_transaction(ctx->session, NULL));

    WT_CURSOR *cursor;
    testutil_check(ctx->session->open_cursor(ctx->session, ctx->uris[slot], NULL, NULL, &cursor));
    for (uint32_t r = DATA_KEY_MIN; r <= DATA_KEY_MAX; r++) {
        char key_buf[16];
        testutil_snprintf(key_buf, sizeof(key_buf), "%" PRIu32, r);
        cursor->set_key(cursor, key_buf);
        cursor->set_value(cursor, val_buf);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));

    char commit_cfg[64];
    testutil_snprintf(commit_cfg, sizeof(commit_cfg), "commit_timestamp=%" PRIx64, commit_ts);
    testutil_check(ctx->session->commit_transaction(ctx->session, commit_cfg));
}

/*
 * workers_min --
 *     Return the minimum of the selected per-thread field across all schema worker threads. Returns
 *     0 if any worker has not yet set the field.
 */
static uint64_t
workers_min(WORKLOAD_STATE *state, bool want_epoch)
{
    uint64_t min_val = UINT64_MAX;
    for (uint32_t i = 0; i < state->nth_workers; i++) {
        THREAD_DATA *w = &state->workers[i];
        const uint64_t val =
          __wt_atomic_load_uint64_acquire(want_epoch ? &w->published_epoch : &w->stable_ready_ts);
        if (val == 0)
            return (0);
        if (val < min_val)
            min_val = val;
    }
    return (min_val);
}

/*
 * Inserts committed on not-yet-stable tables, queued until their table's epoch is stable. Entries
 * arrive already ordered because epochs and commit timestamps come from monotonic counters.
 */
#define PUBLISH_WAIT_QUEUE_MAX 256
typedef struct {
    uint64_t epoch;
    uint64_t commit_ts;
} PUBLISH_WAIT_QUEUE_ENTRY;

typedef struct {
    PUBLISH_WAIT_QUEUE_ENTRY entries[PUBLISH_WAIT_QUEUE_MAX];
    uint64_t head; /* next entry to release */
    uint64_t tail; /* next slot to fill */
} PUBLISH_WAIT_QUEUE;

/*
 * publish_wait_queue_push --
 *     Queue a committed insert awaiting its table's epoch to become stable. Drop the oldest entry
 *     if the queue is full: a later release covers it, since epochs and timestamps only climb.
 */
static void
publish_wait_queue_push(PUBLISH_WAIT_QUEUE *q, uint64_t epoch, uint64_t commit_ts)
{
    if (q->tail - q->head == PUBLISH_WAIT_QUEUE_MAX)
        q->head++;
    PUBLISH_WAIT_QUEUE_ENTRY *e = &q->entries[q->tail++ % PUBLISH_WAIT_QUEUE_MAX];
    e->epoch = epoch;
    e->commit_ts = commit_ts;
}

/*
 * publish_wait_queue_release --
 *     Pop every queued insert whose table epoch has reached the stable frontier and return the
 *     newest released commit timestamp, or 0 if none were released.
 */
static uint64_t
publish_wait_queue_release(PUBLISH_WAIT_QUEUE *q, uint64_t stable_epoch)
{
    uint64_t released = 0;
    while (q->head != q->tail) {
        const PUBLISH_WAIT_QUEUE_ENTRY *e = &q->entries[q->head % PUBLISH_WAIT_QUEUE_MAX];
        if (e->epoch > stable_epoch)
            break;
        released = e->commit_ts;
        q->head++;
    }
    return (released);
}

/*
 * thread_schema_run --
 *     Creates and drops disaggregated tables from a per-thread pool. Each successful operation is
 *     assigned a monotonically increasing schema epoch and durably recorded so the verifier can
 *     reconstruct the expected post-recovery state.
 */
static WT_THREAD_RET
thread_schema_run(void *arg)
{
    THREAD_DATA *td = arg;

    SCHEMA_WORKER_CTX ctx;
    schema_worker_open(td, &ctx);

    SCHEMA_EVENT ev = {0};
    ev.thread_id = td->info;

    PUBLISH_WAIT_QUEUE queue;
    WT_CLEAR(queue);

    for (;;) {
        if (__wt_atomic_load_bool_acquire(&td->state->stop_phase)) {
            /* Carry the final table state back so the next phase resumes from it. */
            memcpy(td->table_exists, ctx.table_exists, sizeof(td->table_exists));
            testutil_check(fclose(ctx.schema_fp));
            testutil_check(ctx.session->close(ctx.session, NULL));
            return (WT_THREAD_RET_VALUE);
        }

        /*
         * Release queued inserts whose table epoch every thread has now published. Until then their
         * data stays unstable, exercising unpublished tables that hold unstable data.
         */
        const uint64_t released = publish_wait_queue_release(&queue, workers_min(td->state, true));
        if (released != 0)
            __wt_atomic_store_uint64_release(&td->stable_ready_ts, released);

        const uint64_t slot = __wt_random(&td->rnd) % td->cfg->pool_size;
        if (schema_op_execute(&ctx, slot) == EBUSY) {
            __wt_yield();
            continue;
        }
        const bool is_create = ctx.table_exists[slot];
        const uint64_t epoch = __wt_atomic_add_uint64(&ctx.state->next_epoch, 1);

        /*
         * Write the record before publishing so it reaches the file before a checkpoint can make
         * the epoch durable. A crash after the record but before the epoch is durable is safe: the
         * verifier ignores records whose epoch is above the recovered durable epoch.
         */
        if (fprintf(ctx.schema_fp, "%s %" PRIu64 " %s\n", is_create ? "CREATE" : "DROP", epoch,
              ctx.uris[slot]) < 0)
            testutil_die(EIO, "fprintf schema record");
        testutil_check(schema_op_publish(&ctx, slot, epoch));

        /*
         * Relay the operation BEFORE the published_epoch release store below. The store is what
         * lets the stable epoch advance past this operation, which is what lets a checkpoint cover
         * it, which is what lets the checkpoint thread send that checkpoint's pipe event. Relaying
         * first therefore guarantees the follower has every event at or below a checkpoint's stable
         * epoch by the time it sees that checkpoint's event.
         */
        ev.type = is_create ? EVENT_CREATE : EVENT_DROP;
        ev.epoch = epoch;
        testutil_snprintf(ev.uri, sizeof(ev.uri), "%s", ctx.uris[slot]);
        pipe_write_event(td->cfg, &ev);

        __wt_atomic_store_uint64_release(&td->published_epoch, epoch);

        if (is_create) {
            const uint64_t commit_ts = __wt_atomic_add_uint64(&ctx.state->next_commit_ts, 1);
            schema_op_insert_data(&ctx, slot, epoch, commit_ts);
            if (fprintf(ctx.schema_fp, "INSERT %" PRIu64 " %" PRIu64 " %d %d %s\n", epoch,
                  commit_ts, DATA_KEY_MIN, DATA_KEY_MAX, ctx.uris[slot]) < 0)
                testutil_die(EIO, "fprintf insert record");
            /*
             * Relay before the queue push for the same reason as above: the push is what later lets
             * this thread's stable_ready_ts (and so the stable timestamp, and so a checkpoint
             * containing this data) advance past the commit.
             */
            ev.type = EVENT_INSERT;
            ev.commit_ts = commit_ts;
            ev.key_min = DATA_KEY_MIN;
            ev.key_max = DATA_KEY_MAX;
            pipe_write_event(td->cfg, &ev);
            publish_wait_queue_push(&queue, epoch, commit_ts);
        }
    }
    /* NOTREACHED */
}

/*
 * thread_ts_run --
 *     Advances the oldest and stable timestamps and the stable schema epoch, keeping stable data on
 *     published tables only. Runs in both roles so a follower phase also advances the epoch.
 */
static WT_THREAD_RET
thread_ts_run(void *arg)
{
    THREAD_DATA *td = arg;

    for (;;) {
        if (__wt_atomic_load_bool_acquire(&td->state->stop_phase))
            return (WT_THREAD_RET_VALUE);

        /*
         * Read the stable timestamp minimum before the epoch minimum, so the epoch covers every
         * commit included in the timestamp. Order matters: the reverse could make data stable on an
         * unpublished table.
         */
        const uint64_t stable_ts = workers_min(td->state, false);
        const uint64_t stable_epoch = workers_min(td->state, true);
        if (stable_epoch == 0 || stable_ts == 0) {
            __wt_sleep(0, 100 * WT_THOUSAND);
            continue;
        }

        char tscfg[128];
        testutil_snprintf(tscfg, sizeof(tscfg),
          "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64
          ",stable_disaggregated_schema_epoch=%" PRIx64,
          stable_ts, stable_ts, stable_epoch);
        testutil_check(td->conn->set_timestamp(td->conn, tscfg));
        __wt_atomic_store_bool_release(&td->state->stable_set, true);
        __wt_sleep(0, 100 * WT_THOUSAND);
    }
    /* NOTREACHED */
}

/*
 * thread_ckpt_run --
 *     Checkpoints periodically in a leader phase, then writes the ready sentinel after the first
 *     checkpoint. A follower phase runs no checkpoint.
 */
static WT_THREAD_RET
thread_ckpt_run(void *arg)
{
    THREAD_DATA *td = arg;

    WT_SESSION *session;
    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));

    SCHEMA_EVENT ev = {0};
    ev.type = EVENT_CKPT;

    struct timespec start;
    __wt_epoch(NULL, &start);

    bool created_ready = false;
    for (int i = 1;; ++i) {
        if (__wt_atomic_load_bool_acquire(&td->state->stop_phase)) {
            testutil_check(session->close(session, NULL));
            return (WT_THREAD_RET_VALUE);
        }
        if (!__wt_atomic_load_bool_acquire(&td->state->stable_set)) {
            struct timespec now;
            __wt_epoch(NULL, &now);
            if (WT_TIMEDIFF_SEC(now, start) > MAX_STARTUP)
                testutil_die(ETIMEDOUT, "stable timestamp not set after %d seconds", MAX_STARTUP);
            __wt_sleep(0, WT_THOUSAND);
            continue;
        }

        const uint64_t sleep_time = __wt_random(&td->rnd) % MAX_CKPT_INVL;
        __wt_sleep(sleep_time, 0);
        if (__wt_atomic_load_bool_acquire(&td->state->stop_phase)) {
            testutil_check(session->close(session, NULL));
            return (WT_THREAD_RET_VALUE);
        }

        /* A follower phase advances the schema epoch only through the timestamp thread. */
        if (!td->state->ckpt_enabled)
            continue;

        /* The timestamp thread owns the stable epoch and timestamps; just checkpoint. */
        testutil_check(session->checkpoint(session, "use_timestamp=true"));

        /* Tell the follower a new checkpoint is available in the page log. */
        pipe_write_event(td->cfg, &ev);

        printf("Checkpoint %d complete\n", i);
        fflush(stdout);

        /* stable_set implies every worker published, so this checkpoint has a schema operation. */
        if (!created_ready) {
            testutil_sentinel(NULL, LEADER_READY_FILE);
            created_ready = true;
        }
    }
    /* NOTREACHED */
}

/* Thread handles have process lifetime; phases join and restart them but never free them. */
static wt_thread_t workload_thr[MAX_TH + 2];
static THREAD_DATA workload_td[MAX_TH + 2];

/*
 * workload_threads_start --
 *     Start all worker threads for one phase: N schema threads plus one checkpoint thread and one
 *     timestamp thread. Each schema thread is seeded with the carried-over table state.
 */
static void
workload_threads_start(
  TEST_CONFIG *cfg, WT_CONNECTION *conn, WORKLOAD_STATE *state, bool (*table_exists)[MAX_POOL_SIZE])
{
    testutil_assert(cfg->nth <= MAX_TH);

    /* Expose the worker array so the timestamp thread can compute the stable minima. */
    state->workers = workload_td;
    state->nth_workers = cfg->nth;

    for (uint32_t i = 0; i < cfg->nth + 2; i++) {
        THREAD_DATA *td = &workload_td[i];
        td->cfg = cfg;
        td->conn = conn;
        td->state = state;
        td->info = i;
        testutil_random_from_random(
          &td->rnd, i < cfg->nth ? &cfg->opts->data_rnd : &cfg->opts->extra_rnd);
        /* The stable minima must wait for this phase's workers, not trust the previous phase's. */
        td->published_epoch = 0;
        td->stable_ready_ts = 0;
    }

    testutil_check(
      __wt_thread_create(NULL, &workload_thr[cfg->nth], thread_ckpt_run, &workload_td[cfg->nth]));
    testutil_check(__wt_thread_create(
      NULL, &workload_thr[cfg->nth + 1], thread_ts_run, &workload_td[cfg->nth + 1]));
    for (uint32_t i = 0; i < cfg->nth; ++i) {
        memcpy(workload_td[i].table_exists, table_exists[i], sizeof(workload_td[i].table_exists));
        testutil_check(
          __wt_thread_create(NULL, &workload_thr[i], thread_schema_run, &workload_td[i]));
    }
}

/*
 * workload_threads_join --
 *     Join all worker threads.
 */
static void
workload_threads_join(const TEST_CONFIG *cfg)
{
    for (uint32_t i = 0; i < cfg->nth + 2; ++i)
        testutil_check(__wt_thread_join(NULL, &workload_thr[i]));
}

/*
 * workload_run_phase --
 *     Run the worker threads for one phase. A duration of zero runs until the parent sends SIGKILL.
 *     A leader phase checkpoints; a follower phase only advances the schema epoch. Bounded phases
 *     are quiesced and joined before returning, carrying the table state back for the next phase.
 */
static void
workload_run_phase(TEST_CONFIG *cfg, WT_CONNECTION *conn, WORKLOAD_STATE *state,
  bool (*table_exists)[MAX_POOL_SIZE], bool leader_phase, uint32_t seconds)
{
    __wt_atomic_store_bool_release(&state->stop_phase, false);
    state->ckpt_enabled = leader_phase;
    workload_threads_start(cfg, conn, state, table_exists);
    fflush(stdout);

    if (seconds == 0)
        workload_threads_join(cfg); /* Blocks until SIGKILL from parent. */
    else {
        /* A leader phase writes the ready sentinel from its checkpoint thread; time after it. */
        if (leader_phase)
            while (!testutil_exists(NULL, LEADER_READY_FILE))
                __wt_sleep(1, 0);
        __wt_sleep(seconds, 0);
        __wt_atomic_store_bool_release(&state->stop_phase, true);
        workload_threads_join(cfg);
    }

    /* Copy the threads' final table state back so the next phase resumes from it. */
    for (uint32_t i = 0; i < cfg->nth; i++)
        memcpy(table_exists[i], workload_td[i].table_exists, sizeof(workload_td[i].table_exists));
}

/*
 * leader_main --
 *     Leader role entry point; never returns. In switch mode the node randomly starts as leader or
 *     follower, runs a first schema phase, switches roles, then resumes the workload under the new
 *     role until the crash.
 */
void
leader_main(TEST_CONFIG *cfg)
{
    /* The leader only writes to the event pipe. */
    if (cfg->pipe_read_fd >= 0) {
        close(cfg->pipe_read_fd);
        cfg->pipe_read_fd = -1;
    }
    /* A dead follower must not kill the leader, ignore SIGPIPE. */
    if (cfg->pipe_write_fd >= 0)
        (void)signal(SIGPIPE, SIG_IGN);

    if (chdir(cfg->home) != 0)
        testutil_die(errno, "Leader chdir: %s", cfg->home);

    /* Discard any record files left by a previous run before the workers start. */
    for (uint32_t i = 0; i < cfg->nth; i++) {
        char fname[128];
        testutil_snprintf(fname, sizeof(fname), SCHEMA_RECORDS_FILE, i);
        (void)unlink(fname);
    }
    /* Remove the ready sentinel once here so a later phase's checkpoint thread cannot recreate a
     * stale one. */
    (void)unlink(LEADER_READY_FILE);

    WORKLOAD_STATE state = {0};
    static bool table_exists[MAX_TH][MAX_POOL_SIZE];

    cfg->opts->disagg.is_enabled = true;
    cfg->opts->disagg.page_log = "palite";
    cfg->opts->disagg.page_log_home = cfg->page_log_home;
    cfg->opts->disagg.drain_threads = 1;

    /* The starting role is leader unless switch mode randomly picks follower. */
    const bool start_as_leader = !cfg->switch_mode || (__wt_random(&cfg->opts->data_rnd) & 1) != 0;
    cfg->opts->disagg.mode = start_as_leader ? "leader" : "follower";

    WT_CONNECTION *conn;
    testutil_wiredtiger_open(cfg->opts, WT_HOME_DIR, ENV_CONFIG_DEF, NULL, &conn, false, false);

    if (!cfg->switch_mode) {
        /* Run the leader workload until the parent sends SIGKILL. */
        workload_run_phase(cfg, conn, &state, table_exists, true, 0);
        _exit(EXIT_SUCCESS); /* NOTREACHED */
    }

    /* Phase 1: run the schema workload for a bounded interval under the starting role. */
    const uint32_t phase1_time = MIN_TIME + __wt_random(&cfg->opts->extra_rnd) % MIN_TIME;
    printf("Switch mode: %s phase 1 for %" PRIu32 " seconds\n",
      start_as_leader ? "leader" : "follower", phase1_time);
    fflush(stdout);
    workload_run_phase(cfg, conn, &state, table_exists, start_as_leader, phase1_time);

    /*
     * Switch roles. Step up with a reconfigure; step down by closing and reopening because graceful
     * step-down is not yet supported.
     */
    if (start_as_leader) {
        testutil_check(conn->close(conn, NULL));
        cfg->opts->disagg.mode = "follower";
        testutil_wiredtiger_open(cfg->opts, WT_HOME_DIR, ENV_CONFIG_DEF, NULL, &conn, false, false);
        printf("Switch mode: stepped down to follower\n");
    } else {
        testutil_check(conn->reconfigure(conn, "disaggregated=(role=leader)"));
        cfg->opts->disagg.mode = "leader";
        printf("Switch mode: stepped up to leader\n");
    }
    fflush(stdout);

    /* Open the crash window: the parent starts its timer only once phase 2 is under way. */
    testutil_sentinel(NULL, SWITCH_DONE_FILE);

    /* Phase 2: resume the schema workload under the new role until the parent sends SIGKILL. */
    workload_run_phase(cfg, conn, &state, table_exists, !start_as_leader, 0);
    exit(EXIT_SUCCESS); /* NOTREACHED */
}
