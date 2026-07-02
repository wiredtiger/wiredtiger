/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 */

#include "schema_disagg_abort.h"

/* Phase 2: set by the switch timer to stop all worker threads before the role transition. */
volatile bool stop_workload;

/* --- Phase 1: in-memory MPSC queue --- */

/*
 * schema_queue_push --
 *     Enqueue one event. Spins if the queue is full.
 */
void
schema_queue_push(const SCHEMA_EVENT *ev)
{
    for (;;) {
        testutil_check(pthread_mutex_lock(&schema_queue.lock));
        if (schema_queue.head - schema_queue.tail < SCHEMA_QUEUE_SIZE) {
            schema_queue.buf[schema_queue.head % SCHEMA_QUEUE_SIZE] = *ev;
            ++schema_queue.head;
            testutil_check(pthread_mutex_unlock(&schema_queue.lock));
            return;
        }
        testutil_check(pthread_mutex_unlock(&schema_queue.lock));
        __wt_yield();
    }
}

/*
 * schema_queue_pop --
 *     Dequeue one event. Returns false if the queue is empty.
 */
bool
schema_queue_pop(SCHEMA_EVENT *ev)
{
    bool found;

    testutil_check(pthread_mutex_lock(&schema_queue.lock));
    found = (schema_queue.head > schema_queue.tail);
    if (found) {
        *ev = schema_queue.buf[schema_queue.tail % SCHEMA_QUEUE_SIZE];
        ++schema_queue.tail;
    }
    testutil_check(pthread_mutex_unlock(&schema_queue.lock));
    return (found);
}

/* --- Phase 2: worker threads --- */

/*
 * thread_schema_run --
 *     Creates and drops disaggregated tables from a per-thread pool, pushing
 *     each successful operation onto the shared queue.
 */
static WT_THREAD_RET
thread_schema_run(void *arg)
{
    THREAD_DATA *td;
    WT_DECL_RET;
    WT_SESSION *session;
    SCHEMA_EVENT ev;
    FILE *data_fp;
    bool table_exists[SCHEMA_POOL_SIZE];
    char fname[128], tableconf[128], uris[SCHEMA_POOL_SIZE][64];
    uint64_t slot;

    td = (THREAD_DATA *)arg;

    /* Phase 1: open per-thread data record file for post-recovery verification. */
    testutil_snprintf(fname, sizeof(fname), SCHEMA_DATA_FILE, td->info);
    (void)unlink(fname);
    testutil_assert_errno((data_fp = fopen(fname, "w")) != NULL);
    __wt_stream_set_line_buffer(data_fp);

    for (slot = 0; slot < SCHEMA_POOL_SIZE; slot++) {
        testutil_snprintf(
          uris[slot], sizeof(uris[slot]), SCHEMA_TABLE_FMT, td->info, (uint32_t)slot);
        table_exists[slot] = false;
    }

    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
    testutil_snprintf(
      tableconf, sizeof(tableconf), "key_format=S,value_format=S,type=layered,block_manager=disagg");

    memset(&ev, 0, sizeof(ev));
    ev.thread_id = td->info;

    for (;;) {
        /* Phase 2: stop check for role-switch mode. */
        if (stop_workload)
            return (WT_THREAD_RET_VALUE);

        slot = __wt_random(&td->rnd) % SCHEMA_POOL_SIZE;

        if (!table_exists[slot]) {
            ret = session->create(session, uris[slot], tableconf);
            if (ret == EBUSY || ret == EEXIST) {
                __wt_yield();
                continue;
            }
            testutil_check(ret);
            table_exists[slot] = true;
            ev.type = SCHEMA_OP_CREATE;

            /*
             * Yield before publishing.  This widens the window between table creation and the
             * point where the table gets a schema epoch, giving the checkpoint thread more
             * opportunity to fire while the table is unpublished.
             */
            __wt_yield();
        } else {
            ret = session->drop(session, uris[slot], "force=false");
            if (ret == EBUSY || ret == ENOENT) {
                __wt_yield();
                continue;
            }
            testutil_check(ret);
            table_exists[slot] = false;
            ev.type = SCHEMA_OP_DROP;
        }

        /*
         * Phase 1: epoch assignment, publish, and stable-epoch advancement serialized under
         * schema_publish_lock so epochs are published in strict increasing order.
         */
        {
            char pub_cfg[64], ts_cfg[64];
            testutil_check(pthread_mutex_lock(&schema_publish_lock));
            ev.epoch = __wt_atomic_add_uint64(&schema_op_epoch, 1);
            testutil_snprintf(pub_cfg, sizeof(pub_cfg), "disaggregated=(schema_epoch=%" PRIx64 ")",
              ev.epoch);
            ret = session->publish(session, uris[slot], pub_cfg);
            if (ret == 0) {
                testutil_snprintf(ts_cfg, sizeof(ts_cfg),
                  "stable_disaggregated_schema_epoch=%" PRIx64, ev.epoch);
                (void)td->conn->set_timestamp(td->conn, ts_cfg);
            }
            testutil_check(pthread_mutex_unlock(&schema_publish_lock));
        }

        testutil_snprintf(ev.uri, sizeof(ev.uri), "%s", uris[slot]);

        /* Phase 1: push to queue so oplog writer can record to schema record files. */
        schema_queue_push(&ev);

        if (ev.type == SCHEMA_OP_CREATE) {
            char val_buf[32];
            WT_CURSOR *dcursor;

            testutil_snprintf(val_buf, sizeof(val_buf), "%" PRIu64, ev.epoch);
            testutil_check(session->begin_transaction(session, NULL));
            testutil_check(session->open_cursor(session, uris[slot], NULL, NULL, &dcursor));
            dcursor->set_key(dcursor, DATA_KEY);
            dcursor->set_value(dcursor, val_buf);
            testutil_check(dcursor->insert(dcursor));
            testutil_check(dcursor->close(dcursor));
            testutil_check(session->commit_transaction(session, NULL));

            if (fprintf(data_fp, "%" PRIu64 " %" PRIu64 "\n", (uint64_t)slot, ev.epoch) < 0)
                testutil_die(EIO, "fprintf data record");
        }
    }
    /* NOTREACHED */
}

/*
 * thread_ts_run --
 *     Phase 1: advances oldest and stable timestamps at a fixed cadence so
 *     precise_checkpoint always has a valid stable timestamp.
 */
static WT_THREAD_RET
thread_ts_run(void *arg)
{
    THREAD_DATA *td;
    uint64_t ts;
    char tscfg[64];

    td = (THREAD_DATA *)arg;
    for (ts = 1;; ts++) {
        /* Phase 2: stop check for role-switch mode. */
        if (stop_workload)
            return (WT_THREAD_RET_VALUE);

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
 *     Phase 2: checkpoints periodically and pushes a SCHEMA_OP_CKPT event after
 *     each checkpoint so the follower can pick up the new state.
 */
static WT_THREAD_RET
thread_ckpt_run(void *arg)
{
    struct timespec now, start;
    THREAD_DATA *td;
    WT_SESSION *session;
    SCHEMA_EVENT ev;
    uint64_t diff_sec, sleep_time;
    int i;
    bool created_ready;

    td = (THREAD_DATA *)arg;
    (void)unlink(ready_file);
    testutil_check(td->conn->open_session(td->conn, NULL, NULL, &session));
    created_ready = false;
    memset(&ev, 0, sizeof(ev));
    ev.type = SCHEMA_OP_CKPT;

    __wt_epoch(NULL, &start);
    for (i = 1;; ++i) {
        /* Phase 2: stop check for role-switch mode. */
        if (stop_workload)
            return (WT_THREAD_RET_VALUE);

        if (!stable_set) {
            __wt_epoch(NULL, &now);
            diff_sec = WT_TIMEDIFF_SEC(now, start);
            if (diff_sec > MAX_STARTUP) {
                fprintf(stderr, "Stable not set after %d seconds\n", MAX_STARTUP);
                abort();
            }
            __wt_sleep(0, WT_THOUSAND);
            continue;
        }

        sleep_time = __wt_random(&td->rnd) % MAX_CKPT_INVL;
        __wt_sleep(sleep_time, 0);

        /* Phase 2: stop check after sleep, before checkpoint. */
        if (stop_workload)
            return (WT_THREAD_RET_VALUE);

        testutil_check(session->checkpoint(session, "use_timestamp=true"));

        /* Phase 3: notify follower that a checkpoint is available via the queue/pipe. */
        schema_queue_push(&ev);

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
 * pipe_write_event --
 *     Phase 3: write one event to the pipe. Silently closes the pipe on
 *     broken-pipe error so the oplog writer keeps running after the follower dies.
 */
static void
pipe_write_event(const SCHEMA_EVENT *ev)
{
    ssize_t nw;

    if (schema_pipe[1] < 0)
        return;
    nw = write(schema_pipe[1], ev, sizeof(*ev));
    if (nw < 0) {
        if (errno == EPIPE || errno == EBADF) {
            close(schema_pipe[1]);
            schema_pipe[1] = -1;
        } else
            testutil_die(errno, "write schema pipe");
    }
}

/*
 * thread_oplog_run --
 *     Phase 1: single consumer of the SCHEMA_EVENT queue.  Writes CREATE/DROP
 *     events to per-thread schema record files.
 *     Phase 3: relays every event to the follower via the pipe.
 */
static WT_THREAD_RET
thread_oplog_run(void *arg)
{
    SCHEMA_EVENT ev;
    FILE *record_fps[MAX_TH];
    char fname[128];
    uint32_t i;

    (void)arg;

    for (i = 0; i < MAX_TH; i++)
        record_fps[i] = NULL;

    for (;;) {
        if (!schema_queue_pop(&ev)) {
            /* Phase 2: drain remaining events then exit when workload is stopping. */
            if (stop_workload)
                goto done;
            __wt_sleep(0, WT_THOUSAND);
            continue;
        }

        switch (ev.type) {
        case SCHEMA_OP_CREATE:
        case SCHEMA_OP_DROP:
            /* Phase 1: write to schema record file for post-recovery verification. */
            if (record_fps[ev.thread_id] == NULL) {
                testutil_snprintf(fname, sizeof(fname), SCHEMA_RECORDS_FILE, ev.thread_id);
                (void)unlink(fname);
                testutil_assert_errno((record_fps[ev.thread_id] = fopen(fname, "w")) != NULL);
                __wt_stream_set_line_buffer(record_fps[ev.thread_id]);
            }
            if (fprintf(record_fps[ev.thread_id], "%s %" PRIu64 " %s\n",
                  ev.type == SCHEMA_OP_CREATE ? "CREATE" : "DROP", ev.epoch, ev.uri) < 0)
                testutil_die(EIO, "fprintf schema record");
            break;
        case SCHEMA_OP_CKPT:
            /* Checkpoint notification — no file I/O needed. */
            break;
        case SCHEMA_OP_EOF:
            /* Phase 3: forward EOF sentinel to follower then exit. */
            pipe_write_event(&ev);
            goto done;
        }

        /* Phase 3: relay event to follower process. */
        pipe_write_event(&ev);
    }
done:
    for (i = 0; i < MAX_TH; i++)
        if (record_fps[i] != NULL)
            (void)fclose(record_fps[i]);
    return (WT_THREAD_RET_VALUE);
}

/*
 * thread_switch_timer --
 *     Phase 2 (role-switch): sleep for half the run timeout, then set
 *     stop_workload so all worker threads quiesce before the role transition.
 */
static WT_THREAD_RET
thread_switch_timer(void *arg)
{
    (void)arg;
    __wt_sleep(run_timeout / 2, 0);
    printf("Switch timer fired: quiescing worker threads for role transition\n");
    fflush(stdout);
    stop_workload = true;
    return (WT_THREAD_RET_VALUE);
}

/*
 * start_threads --
 *     Allocate and start all worker threads for one workload phase.
 *     Returns the thread + data arrays (caller frees).
 */
static void
start_threads(WT_CONNECTION *conn, bool with_timer,
  wt_thread_t **thr_out, THREAD_DATA **td_out,
  uint32_t *ckpt_id_out, uint32_t *ts_id_out, uint32_t *oplog_id_out)
{
    THREAD_DATA *td;
    wt_thread_t *thr;
    uint32_t ckpt_id, i, oplog_id, timer_id, ts_id;

    /* nth + 3 fixed threads (ckpt, ts, oplog) + optional timer. */
    uint32_t total = nth + 3 + (with_timer ? 1 : 0);

    thr = dcalloc(total, sizeof(*thr));
    td = dcalloc(total, sizeof(THREAD_DATA));

    ckpt_id = nth;
    ts_id = nth + 1;
    oplog_id = nth + 2;
    timer_id = nth + 3;

    for (i = 0; i < total; i++) {
        td[i].conn = conn;
        td[i].info = i;
        testutil_random_from_random(&td[i].rnd,
          i < nth ? &opts->data_rnd : &opts->extra_rnd);
    }

    testutil_check(__wt_thread_create(NULL, &thr[oplog_id], thread_oplog_run, &td[oplog_id]));
    testutil_check(__wt_thread_create(NULL, &thr[ckpt_id], thread_ckpt_run, &td[ckpt_id]));
    testutil_check(__wt_thread_create(NULL, &thr[ts_id], thread_ts_run, &td[ts_id]));

    for (i = 0; i < nth; ++i)
        testutil_check(__wt_thread_create(NULL, &thr[i], thread_schema_run, &td[i]));

    if (with_timer)
        testutil_check(
          __wt_thread_create(NULL, &thr[timer_id], thread_switch_timer, &td[timer_id]));

    *thr_out = thr;
    *td_out = td;
    *ckpt_id_out = ckpt_id;
    *ts_id_out = ts_id;
    *oplog_id_out = oplog_id;
}

/*
 * join_threads --
 *     Join all threads for one workload phase.
 */
static void
join_threads(wt_thread_t *thr, uint32_t total)
{
    uint32_t i;

    for (i = 0; i < total; ++i)
        testutil_check(__wt_thread_join(NULL, &thr[i]));
}

/*
 * open_leader --
 *     Open the leader WiredTiger connection at WT_HOME_DIR.
 */
static WT_CONNECTION *
open_leader(const char *envconf)
{
    WT_CONNECTION *conn;

    opts->disagg.is_enabled = true;
    opts->disagg.mode = "leader";
    opts->disagg.page_log = "palite";
    opts->disagg.page_log_home = page_log_home;
    opts->disagg.drain_threads = 1;

    testutil_wiredtiger_open(opts, WT_HOME_DIR, envconf, NULL, &conn, false, false);
    return (conn);
}

/*
 * run_workload --
 *     Phase 2: leader child.  Opens the database as a disaggregated leader and
 *     runs schema/checkpoint/timestamp threads.
 *
 *     Phase 2 (role-switch mode, -w): runs threads for run_timeout/2 seconds,
 *     then performs a close-reopen-as-follower-reopen-as-leader cycle before
 *     continuing as leader until killed by the parent.
 */
void
run_workload(void)
{
    WT_CONNECTION *conn;
    THREAD_DATA *td;
    wt_thread_t *thr;
    uint32_t ckpt_id, oplog_id, ts_id;
    char envconf[1024];

    /* Phase 3: close the read end — the leader only writes to the pipe. */
    if (schema_pipe[0] >= 0) {
        close(schema_pipe[0]);
        schema_pipe[0] = -1;
    }

    signal(SIGPIPE, SIG_IGN);

    if (chdir(home) != 0)
        testutil_die(errno, "Child chdir: %s", home);

    strcpy(envconf, ENV_CONFIG_DEF);
    if (aggressive_sweep)
        strcat(envconf, ENV_CONFIG_SWEEP);

    /* Phase 1: initialize the shared queue and publish lock. */
    testutil_check(pthread_mutex_init(&schema_queue.lock, NULL));
    testutil_check(pthread_mutex_init(&schema_publish_lock, NULL));

    stable_set = false;
    stop_workload = false;
    conn = open_leader(envconf);

    if (!role_switch) {
        /*
         * Phase 3: normal multi-node run — start threads and run until the
         * parent sends SIGKILL.
         */
        start_threads(conn, false, &thr, &td, &ckpt_id, &ts_id, &oplog_id);
        fflush(stdout);
        join_threads(thr, nth + 3); /* NOTREACHED — blocked until SIGKILL */
        free(thr);
        free(td);
    } else {
        /*
         * Phase 2 (role-switch): run threads with a timer.  When the timer
         * fires it sets stop_workload, all worker threads return, and we
         * perform the close → follower → leader cycle before restarting.
         */
        printf("Role-switch mode: running phase A for %" PRIu32 " seconds\n", run_timeout / 2);
        fflush(stdout);

        start_threads(conn, true, &thr, &td, &ckpt_id, &ts_id, &oplog_id);
        fflush(stdout);
        join_threads(thr, nth + 4); /* +1 for timer thread */
        free(thr);
        free(td);

        printf("Phase A complete; closing leader connection for role transition\n");
        fflush(stdout);

        testutil_check(conn->close(conn, "debug=(skip_checkpoint=true)"));

        /*
         * Phase 2 (role-switch): open as follower and pick up the latest page
         * log checkpoint.  This exercises the leader-to-follower transition and
         * the follower checkpoint-pickup path in a single process.
         */
        {
            WT_CONNECTION *fconn;
            WT_DECL_RET;
            WT_PAGE_LOG *page_log;
            WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS ckpt_args;
            WT_SESSION *fsession;
            char meta_config[4096];

            printf("Role-switch: opening as follower to pick up checkpoint\n");
            fflush(stdout);

            opts->disagg.mode = "follower";
            testutil_wiredtiger_open(
              opts, WT_HOME_DIR, envconf, NULL, &fconn, false, false);
            testutil_check(fconn->open_session(fconn, NULL, NULL, &fsession));
            testutil_check(fconn->get_page_log(fconn, "palite", &page_log));

            memset(&ckpt_args, 0, sizeof(ckpt_args));
            ret = page_log->pl_get_complete_checkpoint(page_log, fsession, &ckpt_args);
            if (ret == 0) {
                testutil_snprintf(meta_config, sizeof(meta_config),
                  "disaggregated=(checkpoint_meta=\"%.*s\")",
                  (int)ckpt_args.checkpoint_metadata.size,
                  (const char *)ckpt_args.checkpoint_metadata.data);
                testutil_check(fconn->reconfigure(fconn, meta_config));
                free(ckpt_args.checkpoint_metadata.mem);
                printf("Role-switch: follower picked up checkpoint\n");
            } else if (ret == WT_NOTFOUND)
                printf("Role-switch: no checkpoint in page log yet, continuing\n");
            else
                testutil_die(ret, "pl_get_complete_checkpoint");

            testutil_check(page_log->terminate(page_log, NULL));
            testutil_check(fsession->close(fsession, NULL));
            testutil_check(fconn->close(fconn, "debug=(skip_checkpoint=true)"));
        }

        /*
         * Phase 2 (role-switch): reopen as leader and run a second workload
         * phase until the parent sends SIGKILL.
         */
        printf("Role-switch: reopening as leader for phase B\n");
        fflush(stdout);

        stable_set = false;
        stop_workload = false;
        schema_queue.head = schema_queue.tail = 0;

        conn = open_leader(envconf);

        start_threads(conn, false, &thr, &td, &ckpt_id, &ts_id, &oplog_id);
        fflush(stdout);
        join_threads(thr, nth + 3); /* NOTREACHED — blocked until SIGKILL */
        free(thr);
        free(td);
    }

    _exit(EXIT_SUCCESS);
}

/*
 * run_follower --
 *     Phase 3: follower child.  Opens the database as a disaggregated follower
 *     and processes schema events from the pipe.  SCHEMA_OP_CKPT events trigger
 *     a page log checkpoint pickup.  On pipe EOF the follower steps up to leader,
 *     takes a checkpoint, and signals the parent.
 */
void
run_follower(int pipe_rd)
{
    WT_CONNECTION *conn;
    WT_DECL_RET;
    WT_PAGE_LOG *page_log;
    WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS ckpt_args;
    WT_SESSION *session;
    SCHEMA_EVENT ev;
    FILE *record_fps[MAX_TH];
    ssize_t nr;
    uint32_t i;
    char fname[128], meta_config[4096];
    bool created_ready;

    if (chdir(home) != 0)
        testutil_die(errno, "Follower chdir: %s", home);

    opts->disagg.is_enabled = true;
    opts->disagg.mode = "follower";
    opts->disagg.page_log = "palite";
    opts->disagg.page_log_home = page_log_home;
    opts->disagg.drain_threads = 1;

    testutil_wiredtiger_open(opts, FOLLOWER_HOME_DIR, ENV_CONFIG_DEF, NULL, &conn, false, false);
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_check(conn->get_page_log(conn, "palite", &page_log));

    memset(record_fps, 0, sizeof(record_fps));
    created_ready = false;

    for (;;) {
        nr = read(pipe_rd, &ev, sizeof(ev));
        if (nr == 0)
            break; /* Leader died; pipe write end closed. */
        if (nr < 0) {
            if (errno == EINTR)
                continue;
            testutil_die(errno, "follower read pipe");
        }
        if (nr != (ssize_t)sizeof(ev))
            break; /* Partial read: leader died mid-write. */

        switch (ev.type) {
        case SCHEMA_OP_CREATE:
        case SCHEMA_OP_DROP:
            /* Phase 1: record schema events for post-recovery verification on the follower. */
            if (record_fps[ev.thread_id] == NULL) {
                testutil_snprintf(fname, sizeof(fname), FOLLOWER_RECORDS_FILE, ev.thread_id);
                (void)unlink(fname);
                testutil_assert_errno((record_fps[ev.thread_id] = fopen(fname, "w")) != NULL);
                __wt_stream_set_line_buffer(record_fps[ev.thread_id]);
            }
            if (fprintf(record_fps[ev.thread_id], "%s %" PRIu64 " %s\n",
                  ev.type == SCHEMA_OP_CREATE ? "CREATE" : "DROP", ev.epoch, ev.uri) < 0)
                testutil_die(EIO, "fprintf follower record");
            break;
        case SCHEMA_OP_CKPT:
            /* Phase 3: pick up the latest page log checkpoint from the leader. */
            memset(&ckpt_args, 0, sizeof(ckpt_args));
            ret = page_log->pl_get_complete_checkpoint(page_log, session, &ckpt_args);
            if (ret == 0) {
                testutil_snprintf(meta_config, sizeof(meta_config),
                  "disaggregated=(checkpoint_meta=\"%.*s\")",
                  (int)ckpt_args.checkpoint_metadata.size,
                  (const char *)ckpt_args.checkpoint_metadata.data);
                testutil_check(conn->reconfigure(conn, meta_config));
                free(ckpt_args.checkpoint_metadata.mem);
                if (!created_ready) {
                    testutil_sentinel(NULL, follower_ready_file);
                    created_ready = true;
                }
            } else if (ret != WT_NOTFOUND)
                testutil_die(ret, "pl_get_complete_checkpoint");
            break;
        case SCHEMA_OP_EOF:
            goto done;
        }
    }
done:
    for (i = 0; i < MAX_TH; i++)
        if (record_fps[i] != NULL)
            (void)fclose(record_fps[i]);
    testutil_check(page_log->terminate(page_log, NULL));

    /*
     * Phase 3 (KILL_LEADER): leader died (pipe EOF).  Step up: reconfigure to
     * leader role, set a valid stable timestamp, take a checkpoint to stabilize
     * the new leader's state, then signal the parent.
     */
    printf("Follower: stepping up to leader\n");
    fflush(stdout);

    testutil_check(conn->reconfigure(conn, "disaggregated=(role=leader)"));

    {
        char step_ts_buf[64];
        wt_timestamp_t step_ts = 1;
        if (conn->query_timestamp(conn, step_ts_buf, "get=last_checkpoint") == 0) {
            wt_timestamp_t q;
            if (sscanf(step_ts_buf, "%" SCNx64, &q) == 1 && q > 0)
                step_ts = q;
        }
        {
            char step_set[128];
            testutil_snprintf(step_set, sizeof(step_set),
              "stable_timestamp=%" PRIx64 ",oldest_timestamp=%" PRIx64, step_ts, step_ts);
            testutil_check(conn->set_timestamp(conn, step_set));
        }
    }

    testutil_check(session->checkpoint(session, "use_timestamp=true"));
    testutil_sentinel(NULL, follower_stepped_up_file);
    printf("Follower: stepped up, checkpoint complete\n");
    fflush(stdout);

    testutil_check(session->close(session, NULL));
    testutil_check(conn->close(conn, "debug=(skip_checkpoint=true)"));
    _exit(EXIT_SUCCESS);
}
