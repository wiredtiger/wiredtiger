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
 * thread_schema_run --
 *     Creates and drops disaggregated tables from a per-thread pool. Each successful operation is
 *     assigned a monotonically increasing schema epoch under schema_publish_lock so the checkpoint
 *     thread can advance the stable epoch and the verifier can reconstruct which operations landed
 *     in a given checkpoint. On CREATE, inserts a data row keyed by epoch so the verifier can
 *     confirm data durability as well.
 */
static WT_THREAD_RET
thread_schema_run(void *arg)
{
    THREAD_DATA *td;
    WT_DECL_RET;
    WT_SESSION *session;
    FILE *schema_fp, *data_fp;
    bool table_exists[SCHEMA_POOL_SIZE];
    char schema_fname[128], data_fname[128], tableconf[128], uris[SCHEMA_POOL_SIZE][64];
    uint64_t epoch, slot;

    td = (THREAD_DATA *)arg;

    testutil_snprintf(schema_fname, sizeof(schema_fname), SCHEMA_RECORDS_FILE, td->info);
    (void)unlink(schema_fname);
    testutil_assert_errno((schema_fp = fopen(schema_fname, "w")) != NULL);
    __wt_stream_set_line_buffer(schema_fp);

    testutil_snprintf(data_fname, sizeof(data_fname), SCHEMA_DATA_FILE, td->info);
    (void)unlink(data_fname);
    testutil_assert_errno((data_fp = fopen(data_fname, "w")) != NULL);
    __wt_stream_set_line_buffer(data_fp);

    for (slot = 0; slot < SCHEMA_POOL_SIZE; slot++) {
        testutil_snprintf(
          uris[slot], sizeof(uris[slot]), SCHEMA_TABLE_FMT, td->info, (uint32_t)slot);
        table_exists[slot] = false;
    }

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
    testutil_snprintf(
      tableconf, sizeof(tableconf), "key_format=S,value_format=S,type=layered,block_manager=disagg");

    for (;;) {
        slot = __wt_random(&td->rnd) % SCHEMA_POOL_SIZE;

        if (!table_exists[slot]) {
            ret = session->create(session, uris[slot], tableconf);
            if (ret == EBUSY || ret == EEXIST) {
                __wt_yield();
                continue;
            }
            testutil_check(ret);
            table_exists[slot] = true;
        } else {
            ret = session->drop(session, uris[slot], "force=false");
            if (ret == EBUSY || ret == ENOENT) {
                __wt_yield();
                continue;
            }
            testutil_check(ret);
            table_exists[slot] = false;
        }

        /*
         * Epoch assignment, publish, and stable-epoch advancement are serialized under
         * schema_publish_lock so epochs are published in strictly increasing order, matching the
         * guarantee the verifier relies on when replaying record files.
         */
        {
            char pub_cfg[64], ts_cfg[64];
            bool is_create = table_exists[slot];

            testutil_check(pthread_mutex_lock(&schema_publish_lock));
            epoch = __wt_atomic_add_uint64(&schema_op_epoch, 1);
            testutil_snprintf(
              pub_cfg, sizeof(pub_cfg), "disaggregated=(schema_epoch=%" PRIx64 ")", epoch);
            ret = session->publish(session, uris[slot], pub_cfg);
            if (ret == 0) {
                testutil_snprintf(ts_cfg, sizeof(ts_cfg),
                  "stable_disaggregated_schema_epoch=%" PRIx64, epoch);
                (void)td->conn->set_timestamp(td->conn, ts_cfg);
            }
            testutil_check(pthread_mutex_unlock(&schema_publish_lock));

            if (fprintf(schema_fp, "%s %" PRIu64 " %s\n",
                  is_create ? "CREATE" : "DROP", epoch, uris[slot]) < 0)
                testutil_die(EIO, "fprintf schema record");

            if (is_create) {
                char val_buf[32];
                WT_CURSOR *dcursor;

                testutil_snprintf(val_buf, sizeof(val_buf), "%" PRIu64, epoch);
                testutil_check(session->begin_transaction(session, NULL));
                testutil_check(
                  session->open_cursor(session, uris[slot], NULL, NULL, &dcursor));
                dcursor->set_key(dcursor, DATA_KEY);
                dcursor->set_value(dcursor, val_buf);
                testutil_check(dcursor->insert(dcursor));
                testutil_check(dcursor->close(dcursor));
                testutil_check(session->commit_transaction(session, NULL));

                if (fprintf(data_fp, "%" PRIu64 " %" PRIu64 "\n", slot, epoch) < 0)
                    testutil_die(EIO, "fprintf data record");
            }
        }
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
 *     Checkpoints periodically. Waits for the timestamp thread to set a valid stable timestamp
 *     before the first checkpoint, then writes the ready sentinel so the parent knows at least one
 *     checkpoint has completed.
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
    uint32_t i;
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

    /* nth schema threads + 1 checkpoint thread + 1 timestamp thread. */
    thr = dcalloc(nth + 2, sizeof(*thr));
    td = dcalloc(nth + 2, sizeof(THREAD_DATA));

    for (i = 0; i < nth + 2; i++) {
        td[i].conn = conn;
        td[i].info = i;
        testutil_random_from_random(&td[i].rnd,
          i < nth ? &opts->data_rnd : &opts->extra_rnd);
    }

    testutil_check(__wt_thread_create(NULL, &thr[nth], thread_ckpt_run, &td[nth]));
    testutil_check(__wt_thread_create(NULL, &thr[nth + 1], thread_ts_run, &td[nth + 1]));

    for (i = 0; i < nth; ++i)
        testutil_check(__wt_thread_create(NULL, &thr[i], thread_schema_run, &td[i]));

    fflush(stdout);
    /* Blocks until SIGKILL from parent. */
    for (i = 0; i < nth + 2; ++i)
        testutil_check(__wt_thread_join(NULL, &thr[i]));

    free(thr);
    free(td);
    _exit(EXIT_SUCCESS);
}
