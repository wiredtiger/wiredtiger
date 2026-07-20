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

#include "schema_disagg_abort.h"

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
 * schema_worker_open --
 *     Open the session, record file, and URI table for a schema worker thread.
 */
static void
schema_worker_open(THREAD_DATA *td, SCHEMA_WORKER_CTX *ctx)
{
    uint32_t i;
    char fname[128];

    testutil_snprintf(fname, sizeof(fname), SCHEMA_RECORDS_FILE, td->info);
    testutil_assert_errno((ctx->schema_fp = fopen(fname, "w")) != NULL);
    /* Flush the record file per line so entries survive the SIGKILL crash. */
    __wt_stream_set_line_buffer(ctx->schema_fp);

    for (i = 0; i < td->cfg->pool_size; i++) {
        testutil_snprintf(ctx->uris[i], sizeof(ctx->uris[i]), SCHEMA_TABLE_FMT, td->info, i);
        ctx->table_exists[i] = false;
    }

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
    WT_DECL_RET;

    if (!ctx->table_exists[slot]) {
        ret = ctx->session->create(ctx->session, ctx->uris[slot], ctx->tableconf);
        if (ret == EBUSY)
            return (ret);
        testutil_check(ret);
        ctx->table_exists[slot] = true;
    } else {
        ret = ctx->session->drop(ctx->session, ctx->uris[slot], "force=false,lock_wait=false");
        if (ret == EBUSY)
            return (ret);
        testutil_check(ret);
        ctx->table_exists[slot] = false;
    }
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
    WT_CURSOR *cursor;
    uint32_t r;
    char commit_cfg[64], key_buf[16], val_buf[32];

    testutil_snprintf(val_buf, sizeof(val_buf), "%" PRIu64, epoch);
    testutil_check(ctx->session->begin_transaction(ctx->session, NULL));
    testutil_check(ctx->session->open_cursor(ctx->session, ctx->uris[slot], NULL, NULL, &cursor));
    for (r = DATA_KEY_MIN; r <= DATA_KEY_MAX; r++) {
        testutil_snprintf(key_buf, sizeof(key_buf), "%" PRIu32, r);
        cursor->set_key(cursor, key_buf);
        cursor->set_value(cursor, val_buf);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));
    testutil_snprintf(commit_cfg, sizeof(commit_cfg), "commit_timestamp=%" PRIx64, commit_ts);
    testutil_check(ctx->session->commit_transaction(ctx->session, commit_cfg));
}

/*
 * workers_min --
 *     Return the minimum of the selected per-thread field across all schema worker threads, read
 *     with acquire ordering to pair with each worker's release store. Returns 0 if any worker has
 *     not yet set the field.
 */
static uint64_t
workers_min(WORKLOAD_STATE *state, bool want_epoch)
{
    THREAD_DATA *w;
    uint64_t min_val, val;
    uint32_t i;

    min_val = UINT64_MAX;
    for (i = 0; i < state->nth_workers; i++) {
        w = &state->workers[i];
        val =
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
    PUBLISH_WAIT_QUEUE_ENTRY *e;

    if (q->tail - q->head == PUBLISH_WAIT_QUEUE_MAX)
        q->head++;
    e = &q->entries[q->tail++ % PUBLISH_WAIT_QUEUE_MAX];
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
    PUBLISH_WAIT_QUEUE_ENTRY *e;
    uint64_t released;

    released = 0;
    while (q->head != q->tail) {
        e = &q->entries[q->head % PUBLISH_WAIT_QUEUE_MAX];
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
 *     reconstruct the expected post-recovery state. Inserted data is committed immediately but held
 *     back from stability until its table is published, so data never goes stable before its table.
 */
static WT_THREAD_RET
thread_schema_run(void *arg)
{
    PUBLISH_WAIT_QUEUE queue;
    SCHEMA_WORKER_CTX ctx;
    THREAD_DATA *td;
    bool is_create;
    uint64_t commit_ts, epoch, released, slot;

    td = (THREAD_DATA *)arg;
    schema_worker_open(td, &ctx);

    WT_CLEAR(queue);
    for (;;) {
        /*
         * Release queued inserts whose table epoch every thread has now published. Until then their
         * data stays unstable, exercising unpublished tables that hold unstable data.
         */
        released = publish_wait_queue_release(&queue, workers_min(td->state, true));
        if (released != 0)
            __wt_atomic_store_uint64_release(&td->stable_ready_ts, released);

        slot = __wt_random(&td->rnd) % td->cfg->pool_size;
        if (schema_op_execute(&ctx, slot) == EBUSY) {
            __wt_yield();
            continue;
        }
        is_create = ctx.table_exists[slot];
        epoch = __wt_atomic_add_uint64(&ctx.state->next_epoch, 1);
        /*
         * Write the record before publishing so it reaches the file before a checkpoint can make
         * the epoch durable. A crash after the record but before the epoch is durable is safe: the
         * verifier ignores records whose epoch is above the recovered durable epoch.
         */
        if (fprintf(ctx.schema_fp, "%s %" PRIu64 " %s\n", is_create ? "CREATE" : "DROP", epoch,
              ctx.uris[slot]) < 0)
            testutil_die(EIO, "fprintf schema record");
        testutil_check(schema_op_publish(&ctx, slot, epoch));
        /* Release so the timestamp thread's paired acquire load sees the completed publish. */
        __wt_atomic_store_uint64_release(&td->published_epoch, epoch);
        if (is_create) {
            commit_ts = __wt_atomic_add_uint64(&ctx.state->next_commit_ts, 1);
            schema_op_insert_data(&ctx, slot, epoch, commit_ts);
            if (fprintf(ctx.schema_fp, "INSERT %" PRIu64 " %" PRIu64 " %d %d %s\n", epoch,
                  commit_ts, DATA_KEY_MIN, DATA_KEY_MAX, ctx.uris[slot]) < 0)
                testutil_die(EIO, "fprintf insert record");
            publish_wait_queue_push(&queue, epoch, commit_ts);
        }
    }
    /* NOTREACHED */
}

/*
 * thread_ts_run --
 *     Advances the oldest and stable timestamps and the stable schema epoch, keeping stable data on
 *     published tables only.
 */
static WT_THREAD_RET
thread_ts_run(void *arg)
{
    THREAD_DATA *td;
    uint64_t stable_epoch, stable_ts;
    char tscfg[128];

    td = (THREAD_DATA *)arg;
    for (;;) {
        /*
         * Read the stable timestamp minimum before the epoch minimum, so the epoch covers every
         * commit included in the timestamp. Order matters: the reverse could make data stable on an
         * unpublished table.
         */
        stable_ts = workers_min(td->state, false);
        stable_epoch = workers_min(td->state, true);
        if (stable_epoch == 0 || stable_ts == 0) {
            __wt_sleep(0, 100 * WT_THOUSAND);
            continue;
        }

        testutil_snprintf(tscfg, sizeof(tscfg),
          "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64
          ",stable_disaggregated_schema_epoch=%" PRIx64,
          stable_ts, stable_ts, stable_epoch);
        testutil_check(td->conn->set_timestamp(td->conn, tscfg));
        if (!td->state->stable_set)
            td->state->stable_set = true;
        __wt_sleep(0, 100 * WT_THOUSAND);
    }
    /* NOTREACHED */
}

/*
 * thread_ckpt_run --
 *     Checkpoints periodically, then writes the ready sentinel after the first checkpoint that
 *     includes a schema operation.
 */
static WT_THREAD_RET
thread_ckpt_run(void *arg)
{
    struct timespec now, start;
    THREAD_DATA *td;
    WT_SESSION *session;
    uint64_t diff_sec, sleep_time;
    int i;
    bool created_ready;

    td = (THREAD_DATA *)arg;
    (void)unlink(READY_FILE);
    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
    created_ready = false;

    __wt_epoch(NULL, &start);
    for (i = 1;; ++i) {
        if (!td->state->stable_set) {
            __wt_epoch(NULL, &now);
            diff_sec = WT_TIMEDIFF_SEC(now, start);
            if (diff_sec > MAX_STARTUP)
                testutil_die(ETIMEDOUT, "stable timestamp not set after %d seconds", MAX_STARTUP);
            __wt_sleep(0, WT_THOUSAND);
            continue;
        }

        sleep_time = __wt_random(&td->rnd) % MAX_CKPT_INVL;
        __wt_sleep(sleep_time, 0);

        testutil_check(session->checkpoint(session, "use_timestamp=true"));

        printf("Checkpoint %d complete\n", i);
        fflush(stdout);

        /* stable_set implies every worker published, so this checkpoint has a schema operation. */
        if (!created_ready) {
            testutil_sentinel(NULL, READY_FILE);
            created_ready = true;
        }
    }
    /* NOTREACHED */
}

/*
 * workload_threads_start --
 *     Allocate and start all worker threads: N schema threads plus one checkpoint thread and one
 *     timestamp thread.
 */
static void
workload_threads_start(TEST_CONFIG *cfg, WT_CONNECTION *conn, WORKLOAD_STATE *state,
  wt_thread_t **thr_out, THREAD_DATA **td_out)
{
    THREAD_DATA *td;
    wt_thread_t *thr;
    uint32_t i;

    thr = dcalloc(cfg->nth + 2, sizeof(*thr));
    td = dcalloc(cfg->nth + 2, sizeof(THREAD_DATA));

    state->workers = td;
    state->nth_workers = cfg->nth;

    for (i = 0; i < cfg->nth + 2; i++) {
        td[i].cfg = cfg;
        td[i].conn = conn;
        td[i].state = state;
        td[i].info = i;
        testutil_random_from_random(
          &td[i].rnd, i < cfg->nth ? &cfg->opts->data_rnd : &cfg->opts->extra_rnd);
    }

    testutil_check(__wt_thread_create(NULL, &thr[cfg->nth], thread_ckpt_run, &td[cfg->nth]));
    testutil_check(__wt_thread_create(NULL, &thr[cfg->nth + 1], thread_ts_run, &td[cfg->nth + 1]));
    for (i = 0; i < cfg->nth; ++i)
        testutil_check(__wt_thread_create(NULL, &thr[i], thread_schema_run, &td[i]));

    *thr_out = thr;
    *td_out = td;
}

/*
 * workload_threads_join --
 *     Join all worker threads.
 */
static void
workload_threads_join(TEST_CONFIG *cfg, wt_thread_t *thr)
{
    uint32_t i;

    for (i = 0; i < cfg->nth + 2; ++i)
        testutil_check(__wt_thread_join(NULL, &thr[i]));
}

/*
 * run_workload --
 *     Leader child: opens the database as a disaggregated leader and runs schema worker threads, a
 *     checkpoint thread, and a timestamp thread until the parent sends SIGKILL.
 */
void
run_workload(TEST_CONFIG *cfg)
{
    WORKLOAD_STATE state;
    WT_CONNECTION *conn;
    THREAD_DATA *td;
    wt_thread_t *thr;
    uint32_t i;
    char fname[128];

    if (chdir(cfg->home) != 0)
        testutil_die(errno, "Child chdir: %s", cfg->home);

    /* Discard any record files left by a previous run before the workers start. */
    for (i = 0; i < cfg->nth; i++) {
        testutil_snprintf(fname, sizeof(fname), SCHEMA_RECORDS_FILE, i);
        (void)unlink(fname);
    }

    WT_CLEAR(state);

    cfg->opts->disagg.is_enabled = true;
    cfg->opts->disagg.mode = "leader";
    cfg->opts->disagg.page_log = "palite";
    cfg->opts->disagg.page_log_home = cfg->page_log_home;
    cfg->opts->disagg.drain_threads = 1;

    testutil_wiredtiger_open(cfg->opts, WT_HOME_DIR, ENV_CONFIG_DEF, NULL, &conn, false, false);

    workload_threads_start(cfg, conn, &state, &thr, &td);
    fflush(stdout);
    workload_threads_join(cfg, thr); /* Blocks until SIGKILL from parent. */

    free(thr);
    free(td);
    _exit(EXIT_SUCCESS);
}
