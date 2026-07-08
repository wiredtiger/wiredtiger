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

#include "schema_disagg_leader_abort.h"

/*
 * Schema workers hold a read lock during session->create + session->publish. The checkpoint thread
 * holds the write lock while advancing stable_disaggregated_schema_epoch and running the
 * checkpoint. This guarantees: when the checkpoint captures the schema epoch watermark, no
 * create→publish sequence is in flight, so every table whose internal pages are included in the
 * checkpoint has already been published with an epoch at or below the watermark. Without this
 * coupling, session->create immediately enqueues a CREATE entry with WT_SCHEMA_EPOCH_UNPUBLISHED.
 * A checkpoint that fires between create and publish would capture that table's stable internal
 * pages and trigger the "stable data checkpointed for unpublished table" invariant violation.
 */
static pthread_rwlock_t schema_ckpt_rwlock;

/* Shared state internal to the workload threads. */
static volatile bool stable_set;
static uint64_t schema_op_epoch;

/* Per-thread schema worker state. */
typedef struct {
    WT_SESSION *session;
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
    char fname[128];
    uint32_t i;

    testutil_snprintf(fname, sizeof(fname), SCHEMA_RECORDS_FILE, td->info);
    (void)unlink(fname);
    testutil_assert_errno((ctx->schema_fp = fopen(fname, "w")) != NULL);
    __wt_stream_set_line_buffer(ctx->schema_fp);

    for (i = 0; i < td->cfg->pool_size; i++) {
        testutil_snprintf(
          ctx->uris[i], sizeof(ctx->uris[i]), SCHEMA_TABLE_FMT, td->info, i);
        ctx->table_exists[i] = false;
    }

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &ctx->session));
    testutil_snprintf(ctx->tableconf, sizeof(ctx->tableconf),
      "key_format=S,value_format=S,type=layered,block_manager=disagg");
}

/*
 * schema_op_execute --
 *     Execute the next schema operation on the given slot and update the caller's table-exists
 *     state. Drop uses lock_wait=false so lock contention returns EBUSY immediately; the caller
 *     yields and retries. Called under the schema_ckpt_rwlock read lock.
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
 *     Assign a monotonically increasing epoch and, for CREATE, publish the table so it is visible
 *     to followers. Called under the schema_ckpt_rwlock read lock.
 *
 *     session->publish is only valid for CREATE: it registers the table in the shared metadata
 *     queue with a real epoch, replacing the WT_SCHEMA_EPOCH_UNPUBLISHED placeholder that
 *     session->create inserts. DROP has no equivalent; the table is already gone.
 *
 *     stable_disaggregated_schema_epoch is NOT advanced here. Only the checkpoint thread advances
 *     it, while holding the write lock, so the epoch watermark and the checkpoint are always
 *     consistent.
 */
static uint64_t
schema_op_publish(SCHEMA_WORKER_CTX *ctx, uint64_t slot, bool is_create)
{
    char pub_cfg[64];
    uint64_t epoch;

    epoch = __wt_atomic_add_uint64(&schema_op_epoch, 1);
    if (is_create) {
        testutil_snprintf(
          pub_cfg, sizeof(pub_cfg), "disaggregated=(schema_epoch=%" PRIx64 ")", epoch);
        testutil_check(ctx->session->publish(ctx->session, ctx->uris[slot], pub_cfg));
    }
    return (epoch);
}

/*
 * schema_op_insert_data --
 *     Populate a newly created table with DATA_NROWS rows, each keyed by row index with the epoch
 *     as value.
 *
 *     The commit timestamp is set to stable_timestamp + 10, placing the data above the current
 *     checkpoint's timestamp so it is not captured until the next checkpoint cycle. By that point
 *     the checkpoint thread will have advanced stable_disaggregated_schema_epoch past this table's
 *     epoch, satisfying the invariant that only published tables have checkpointed data.
 */
static void
schema_op_insert_data(WT_CONNECTION *conn, SCHEMA_WORKER_CTX *ctx, uint64_t slot, uint64_t epoch)
{
    WT_CURSOR *cursor;
    char commit_cfg[64], key_buf[16], ts_buf[64], val_buf[32];
    uint32_t r;
    uint64_t stable_ts;

    testutil_snprintf(val_buf, sizeof(val_buf), "%" PRIu64, epoch);
    testutil_check(ctx->session->begin_transaction(ctx->session, NULL));
    testutil_check(
      ctx->session->open_cursor(ctx->session, ctx->uris[slot], NULL, NULL, &cursor));
    for (r = 0; r < DATA_NROWS; r++) {
        testutil_snprintf(key_buf, sizeof(key_buf), "%" PRIu32, r);
        cursor->set_key(cursor, key_buf);
        cursor->set_value(cursor, val_buf);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));

    testutil_check(conn->query_timestamp(conn, ts_buf, "get=stable"));
    stable_ts = 0;
    (void)sscanf(ts_buf, "%" SCNx64, &stable_ts);
    testutil_snprintf(commit_cfg, sizeof(commit_cfg), "commit_timestamp=%" PRIx64, stable_ts + 10);
    testutil_check(ctx->session->commit_transaction(ctx->session, commit_cfg));
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
    SCHEMA_WORKER_CTX ctx;
    THREAD_DATA *td;
    bool is_create;
    uint64_t epoch, slot;

    td = (THREAD_DATA *)arg;
    schema_worker_open(td, &ctx);

    for (;;) {
        slot = __wt_random(&td->rnd) % td->cfg->pool_size;

        /*
         * Hold the read lock for the entire create/drop + publish window. The checkpoint thread
         * holds the write lock while advancing the schema epoch watermark and checkpointing, which
         * ensures the checkpoint never fires while a table is between create and publish.
         */
        testutil_check(pthread_rwlock_rdlock(&schema_ckpt_rwlock));
        if (schema_op_execute(&ctx, slot) == EBUSY) {
            testutil_check(pthread_rwlock_unlock(&schema_ckpt_rwlock));
            __wt_yield();
            continue;
        }
        is_create = ctx.table_exists[slot];
        epoch = schema_op_publish(&ctx, slot, is_create);
        testutil_check(pthread_rwlock_unlock(&schema_ckpt_rwlock));

        if (fprintf(ctx.schema_fp, "%s %" PRIu64 " %s\n",
              is_create ? "CREATE" : "DROP", epoch, ctx.uris[slot]) < 0)
            testutil_die(EIO, "fprintf schema record");

        if (is_create)
            schema_op_insert_data(td->conn, &ctx, slot, epoch);
    }
    /* NOTREACHED */
}

/*
 * thread_ts_run --
 *     Advances oldest and stable timestamps at a fixed cadence so precise_checkpoint always has a
 *     valid stable timestamp.
 */
static WT_THREAD_RET
thread_ts_run(void *arg)
{
    THREAD_DATA *td;
    uint64_t ts;
    char tscfg[64];

    td = (THREAD_DATA *)arg;
    for (ts = 1;; ts++) {
        testutil_snprintf(tscfg, sizeof(tscfg),
          "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64, ts, ts);
        testutil_check(td->conn->set_timestamp(td->conn, tscfg));
        if (!stable_set)
            stable_set = true;
        __wt_sleep(0, 100 * WT_THOUSAND);
    }
    /* NOTREACHED */
}

/*
 * thread_ckpt_run --
 *     Checkpoints periodically. Holds the write lock while advancing
 *     stable_disaggregated_schema_epoch and running the checkpoint so no create→publish sequences
 *     are in flight during either operation. Writes the ready sentinel after the first checkpoint.
 */
static WT_THREAD_RET
thread_ckpt_run(void *arg)
{
    struct timespec now, start;
    THREAD_DATA *td;
    WT_SESSION *session;
    char ts_cfg[64];
    uint64_t diff_sec, sleep_time;
    int i;
    bool created_ready;

    td = (THREAD_DATA *)arg;
    (void)unlink(READY_FILE);
    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
    created_ready = false;

    __wt_epoch(NULL, &start);
    for (i = 1;; ++i) {
        if (!stable_set) {
            __wt_epoch(NULL, &now);
            diff_sec = WT_TIMEDIFF_SEC(now, start);
            if (diff_sec > MAX_STARTUP)
                testutil_die(ETIMEDOUT,
                  "stable timestamp not set after %d seconds", MAX_STARTUP);
            __wt_sleep(0, WT_THOUSAND);
            continue;
        }

        sleep_time = __wt_random(&td->rnd) % MAX_CKPT_INVL;
        __wt_sleep(sleep_time, 0);

        /*
         * Advance stable_disaggregated_schema_epoch to cover all published tables, then
         * checkpoint. Both operations are done under the write lock so no create→publish window
         * is open when the checkpoint captures the epoch watermark.
         */
        testutil_check(pthread_rwlock_wrlock(&schema_ckpt_rwlock));
        testutil_snprintf(
          ts_cfg, sizeof(ts_cfg), "stable_disaggregated_schema_epoch=%" PRIx64, schema_op_epoch);
        (void)td->conn->set_timestamp(td->conn, ts_cfg);
        testutil_check(session->checkpoint(session, "use_timestamp=true"));
        testutil_check(pthread_rwlock_unlock(&schema_ckpt_rwlock));

        printf("Checkpoint %d complete\n", i);
        fflush(stdout);

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
workload_threads_start(TEST_CONFIG *cfg, WT_CONNECTION *conn,
  wt_thread_t **thr_out, THREAD_DATA **td_out)
{
    THREAD_DATA *td;
    wt_thread_t *thr;
    uint32_t i;

    thr = dcalloc(cfg->nth + 2, sizeof(*thr));
    td = dcalloc(cfg->nth + 2, sizeof(THREAD_DATA));

    for (i = 0; i < cfg->nth + 2; i++) {
        td[i].cfg = cfg;
        td[i].conn = conn;
        td[i].info = i;
        testutil_random_from_random(&td[i].rnd,
          i < cfg->nth ? &cfg->opts->data_rnd : &cfg->opts->extra_rnd);
    }

    testutil_check(__wt_thread_create(NULL, &thr[cfg->nth], thread_ckpt_run, &td[cfg->nth]));
    testutil_check(
      __wt_thread_create(NULL, &thr[cfg->nth + 1], thread_ts_run, &td[cfg->nth + 1]));
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
 *     Leader child: opens the database as a disaggregated leader and runs schema worker threads,
 *     a checkpoint thread, and a timestamp thread until the parent sends SIGKILL.
 */
void
run_workload(TEST_CONFIG *cfg)
{
    WT_CONNECTION *conn;
    THREAD_DATA *td;
    wt_thread_t *thr;
    char envconf[1024];

    if (chdir(cfg->home) != 0)
        testutil_die(errno, "Child chdir: %s", cfg->home);

    strcpy(envconf, ENV_CONFIG_DEF);
    if (cfg->aggressive_sweep)
        strcat(envconf, ENV_CONFIG_SWEEP);

    testutil_check(pthread_rwlock_init(&schema_ckpt_rwlock, NULL));
    stable_set = false;

    cfg->opts->disagg.is_enabled = true;
    cfg->opts->disagg.mode = "leader";
    cfg->opts->disagg.page_log = "palite";
    cfg->opts->disagg.page_log_home = cfg->page_log_home;
    cfg->opts->disagg.drain_threads = 1;

    testutil_wiredtiger_open(cfg->opts, WT_HOME_DIR, envconf, NULL, &conn, false, false);

    workload_threads_start(cfg, conn, &thr, &td);
    fflush(stdout);
    workload_threads_join(cfg, thr); /* Blocks until SIGKILL from parent. */

    free(thr);
    free(td);
    _exit(EXIT_SUCCESS);
}
