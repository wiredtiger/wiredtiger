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

/* Per-thread schema worker state. */
typedef struct {
    WT_SESSION *session;
    FILE *schema_fp;
    FILE *data_fp;
    char tableconf[128];
    char uris[SCHEMA_POOL_SIZE][64];
    bool table_exists[SCHEMA_POOL_SIZE];
} SCHEMA_WORKER_CTX;

/*
 * schema_worker_open --
 *     Open the session, record files, and URI table for a schema worker thread.
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

    testutil_snprintf(fname, sizeof(fname), SCHEMA_DATA_FILE, td->info);
    (void)unlink(fname);
    testutil_assert_errno((ctx->data_fp = fopen(fname, "w")) != NULL);
    __wt_stream_set_line_buffer(ctx->data_fp);

    for (i = 0; i < SCHEMA_POOL_SIZE; i++) {
        testutil_snprintf(
          ctx->uris[i], sizeof(ctx->uris[i]), SCHEMA_TABLE_FMT, td->info, i);
        ctx->table_exists[i] = false;
    }

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &ctx->session));
    testutil_snprintf(ctx->tableconf, sizeof(ctx->tableconf),
      "key_format=S,value_format=S,type=layered,block_manager=disagg");
}

/*
 * schema_op_try --
 *     Attempt the next schema operation on the given slot. Returns EBUSY or ENOENT when the
 *     caller should yield and retry, 0 on success. Updates the caller's table-exists state.
 */
static int
schema_op_try(SCHEMA_WORKER_CTX *ctx, uint64_t slot)
{
    WT_DECL_RET;

    if (!ctx->table_exists[slot]) {
        ret = ctx->session->create(ctx->session, ctx->uris[slot], ctx->tableconf);
        if (ret == EBUSY || ret == EEXIST)
            return (ret);
        testutil_check(ret);
        ctx->table_exists[slot] = true;
    } else {
        ret = ctx->session->drop(ctx->session, ctx->uris[slot], "force=false");
        if (ret == EBUSY || ret == ENOENT)
            return (ret);
        testutil_check(ret);
        ctx->table_exists[slot] = false;
    }
    return (0);
}

/*
 * schema_op_publish --
 *     Assign an epoch, publish the schema operation, and advance
 *     stable_disaggregated_schema_epoch. Serialized under the publish lock so epochs are strictly
 *     increasing; the verifier relies on this ordering when replaying record files.
 */
static uint64_t
schema_op_publish(WT_CONNECTION *conn, SCHEMA_WORKER_CTX *ctx, uint64_t slot)
{
    WT_DECL_RET;
    char pub_cfg[64], ts_cfg[64];
    uint64_t epoch;

    testutil_check(pthread_mutex_lock(&schema_publish_lock));
    epoch = __wt_atomic_add_uint64(&schema_op_epoch, 1);
    testutil_snprintf(pub_cfg, sizeof(pub_cfg), "disaggregated=(schema_epoch=%" PRIx64 ")", epoch);
    ret = ctx->session->publish(ctx->session, ctx->uris[slot], pub_cfg);
    if (ret == 0) {
        testutil_snprintf(
          ts_cfg, sizeof(ts_cfg), "stable_disaggregated_schema_epoch=%" PRIx64, epoch);
        (void)conn->set_timestamp(conn, ts_cfg);
    }
    testutil_check(pthread_mutex_unlock(&schema_publish_lock));
    return (epoch);
}

/*
 * schema_op_insert_data --
 *     Insert a data row into a newly created table, keyed by a fixed sentinel key with the epoch
 *     as value. This lets the verifier confirm data durability for surviving tables.
 */
static void
schema_op_insert_data(SCHEMA_WORKER_CTX *ctx, uint64_t slot, uint64_t epoch)
{
    WT_CURSOR *cursor;
    char val_buf[32];

    testutil_snprintf(val_buf, sizeof(val_buf), "%" PRIu64, epoch);
    testutil_check(ctx->session->begin_transaction(ctx->session, NULL));
    testutil_check(
      ctx->session->open_cursor(ctx->session, ctx->uris[slot], NULL, NULL, &cursor));
    cursor->set_key(cursor, DATA_KEY);
    cursor->set_value(cursor, val_buf);
    testutil_check(cursor->insert(cursor));
    testutil_check(cursor->close(cursor));
    testutil_check(ctx->session->commit_transaction(ctx->session, NULL));
}

/*
 * schema_op_record --
 *     Persist the schema event and, on CREATE, the data row written to it. The records are
 *     consumed by the verifier after recovery.
 */
static void
schema_op_record(SCHEMA_WORKER_CTX *ctx, uint64_t slot, bool is_create, uint64_t epoch)
{
    if (fprintf(ctx->schema_fp, "%s %" PRIu64 " %s\n",
          is_create ? "CREATE" : "DROP", epoch, ctx->uris[slot]) < 0)
        testutil_die(EIO, "fprintf schema record");

    if (is_create) {
        schema_op_insert_data(ctx, slot, epoch);
        if (fprintf(ctx->data_fp, "%" PRIu64 " %" PRIu64 "\n", slot, epoch) < 0)
            testutil_die(EIO, "fprintf data record");
    }
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
    uint64_t epoch, slot;

    td = (THREAD_DATA *)arg;
    schema_worker_open(td, &ctx);

    for (;;) {
        slot = __wt_random(&td->rnd) % SCHEMA_POOL_SIZE;
        if (schema_op_try(&ctx, slot) != 0) {
            __wt_yield();
            continue;
        }
        epoch = schema_op_publish(td->conn, &ctx, slot);
        schema_op_record(&ctx, slot, ctx.table_exists[slot], epoch);
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
 *     Checkpoints periodically. Waits until a valid stable timestamp has been set before the first
 *     checkpoint, then writes the ready sentinel so the parent knows at least one checkpoint has
 *     completed.
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
    (void)unlink(ready_file);
    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
    created_ready = false;

    __wt_epoch(NULL, &start);
    for (i = 1;; ++i) {
        if (!stable_set) {
            __wt_epoch(NULL, &now);
            diff_sec = WT_TIMEDIFF_SEC(now, start);
            if (diff_sec > MAX_STARTUP) {
                fprintf(stderr, "Stable timestamp not set after %d seconds\n", MAX_STARTUP);
                abort();
            }
            __wt_sleep(0, WT_THOUSAND);
            continue;
        }

        sleep_time = __wt_random(&td->rnd) % MAX_CKPT_INVL;
        __wt_sleep(sleep_time, 0);

        testutil_check(session->checkpoint(session, "use_timestamp=true"));

        printf("Checkpoint %d complete\n", i);
        fflush(stdout);

        if (!created_ready) {
            testutil_sentinel(NULL, ready_file);
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
workload_threads_start(WT_CONNECTION *conn, wt_thread_t **thr_out, THREAD_DATA **td_out)
{
    THREAD_DATA *td;
    wt_thread_t *thr;
    uint32_t i;

    thr = dcalloc(nth + 2, sizeof(*thr));
    td = dcalloc(nth + 2, sizeof(THREAD_DATA));

    for (i = 0; i < nth + 2; i++) {
        td[i].conn = conn;
        td[i].info = i;
        testutil_random_from_random(&td[i].rnd, i < nth ? &opts->data_rnd : &opts->extra_rnd);
    }

    testutil_check(__wt_thread_create(NULL, &thr[nth], thread_ckpt_run, &td[nth]));
    testutil_check(__wt_thread_create(NULL, &thr[nth + 1], thread_ts_run, &td[nth + 1]));
    for (i = 0; i < nth; ++i)
        testutil_check(__wt_thread_create(NULL, &thr[i], thread_schema_run, &td[i]));

    *thr_out = thr;
    *td_out = td;
}

/*
 * workload_threads_join --
 *     Join all worker threads.
 */
static void
workload_threads_join(wt_thread_t *thr)
{
    uint32_t i;

    for (i = 0; i < nth + 2; ++i)
        testutil_check(__wt_thread_join(NULL, &thr[i]));
}

/*
 * run_workload --
 *     Leader child: opens the database as a disaggregated leader and runs schema worker threads,
 *     a checkpoint thread, and a timestamp thread until the parent sends SIGKILL.
 */
void
run_workload(void)
{
    WT_CONNECTION *conn;
    THREAD_DATA *td;
    wt_thread_t *thr;
    char envconf[1024];

    if (chdir(home) != 0)
        testutil_die(errno, "Child chdir: %s", home);

    strcpy(envconf, ENV_CONFIG_DEF);
    if (aggressive_sweep)
        strcat(envconf, ENV_CONFIG_SWEEP);

    testutil_check(pthread_mutex_init(&schema_publish_lock, NULL));
    stable_set = false;

    opts->disagg.is_enabled = true;
    opts->disagg.mode = "leader";
    opts->disagg.page_log = "palite";
    opts->disagg.page_log_home = page_log_home;
    opts->disagg.drain_threads = 1;

    testutil_wiredtiger_open(opts, WT_HOME_DIR, envconf, NULL, &conn, false, false);

    workload_threads_start(conn, &thr, &td);
    fflush(stdout);
    workload_threads_join(thr); /* Blocks until SIGKILL from parent. */

    free(thr);
    free(td);
    _exit(EXIT_SUCCESS);
}
