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
 * Follower role: opens its own home as a disaggregated follower and processes schema events from
 * the pipe. Checkpoint events trigger a page log checkpoint pickup. On pipe EOF (the leader died)
 * in kill-leader mode the follower steps up, then exits cleanly.
 */

#include "schema_disagg_abort.h"

/*
 * follower_record_event --
 *     Append a schema or insert event to the per-thread follower record file, creating it on first
 *     use. The formats match the leader's record files so the verifier can parse both.
 */
static void
follower_record_event(FILE *record_fps[MAX_TH], const SCHEMA_EVENT *ev)
{
    testutil_assert(ev->thread_id < MAX_TH);

    FILE *fp = record_fps[ev->thread_id];

    if (fp == NULL) {
        char fname[128];
        testutil_snprintf(fname, sizeof(fname), FOLLOWER_RECORDS_FILE, ev->thread_id);
        (void)unlink(fname);
        testutil_assert_errno((fp = fopen(fname, "w")) != NULL);
        record_fps[ev->thread_id] = fp;
        /* Flush per line so the records survive a SIGKILL. */
        __wt_stream_set_line_buffer(fp);
    }

    switch (ev->type) {
    case EVENT_INSERT:
        if (fprintf(fp, "INSERT %" PRIu64 " %" PRIu64 " %" PRIu32 " %" PRIu32 " %s\n", ev->epoch,
              ev->commit_ts, ev->key_min, ev->key_max, ev->uri) < 0)
            testutil_die(EIO, "fprintf follower insert record");
        break;
    case EVENT_CREATE:
    case EVENT_DROP:
        if (fprintf(fp, "%s %" PRIu64 " %s\n", ev->type == EVENT_CREATE ? "CREATE" : "DROP",
              ev->epoch, ev->uri) < 0)
            testutil_die(EIO, "fprintf follower schema record");
        break;
    case EVENT_CKPT:
        testutil_assertfmt(false, "Unexpected event type: %d", ev->type);
    }
}

/*
 * follower_pick_up_checkpoint --
 *     Fetch the latest complete checkpoint from the page log and apply it to the follower
 *     connection. Writes the ready sentinel after the first successful pickup.
 */
static void
follower_pick_up_checkpoint(
  WT_CONNECTION *conn, WT_SESSION *session, WT_PAGE_LOG *page_log, bool *picked_up)
{
    WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS ckpt_args = {0};

    const int ret = page_log->pl_get_complete_checkpoint(page_log, session, &ckpt_args);
    if (ret == WT_NOTFOUND)
        return;
    testutil_check(ret);

    char meta_config[4096];
    testutil_snprintf(meta_config, sizeof(meta_config), "disaggregated=(checkpoint_meta=\"%.*s\")",
      (int)ckpt_args.checkpoint_metadata.size, (const char *)ckpt_args.checkpoint_metadata.data);
    testutil_check(conn->reconfigure(conn, meta_config));
    free(ckpt_args.checkpoint_metadata.mem);

    if (!*picked_up) {
        testutil_sentinel(NULL, FOLLOWER_READY_FILE);
        *picked_up = true;
    }
}

/*
 * follower_step_up --
 *     Step up to the leader role after the leader dies: reconfigure, set a valid stable timestamp,
 *     take a checkpoint, and signal the parent via the stepped-up sentinel.
 */
static void
follower_step_up(WT_CONNECTION *conn, WT_SESSION *session)
{
    printf("Follower: stepping up to leader\n");
    fflush(stdout);

    testutil_check(conn->reconfigure(conn, "disaggregated=(role=leader)"));

    char ts_buf[64];
    uint64_t step_ts = 1;
    if (conn->query_timestamp(conn, ts_buf, "get=last_checkpoint") == 0)
        (void)sscanf(ts_buf, "%" SCNx64, &step_ts);
    if (step_ts == 0)
        step_ts = 1;

    char ts_cfg[128];
    testutil_snprintf(ts_cfg, sizeof(ts_cfg),
      "stable_timestamp=%" PRIx64 ",oldest_timestamp=%" PRIx64, step_ts, step_ts);
    testutil_check(conn->set_timestamp(conn, ts_cfg));

    /* Carry the schema epoch forward so the step-up checkpoint does not publish epoch zero. */
    uint64_t step_epoch = 0;
    memset(ts_buf, 0, sizeof(ts_buf));
    if (conn->query_timestamp(conn, ts_buf, "get=last_disaggregated_schema_epoch") == 0)
        (void)sscanf(ts_buf, "%" SCNx64, &step_epoch);
    if (step_epoch != 0) {
        testutil_snprintf(
          ts_cfg, sizeof(ts_cfg), "stable_disaggregated_schema_epoch=%" PRIx64, step_epoch);
        testutil_check(conn->set_timestamp(conn, ts_cfg));
    }

    testutil_check(session->checkpoint(session, "use_timestamp=true"));
    testutil_sentinel(NULL, FOLLOWER_STEPPED_UP_FILE);

    printf("Follower: stepped up, checkpoint complete\n");
    fflush(stdout);
}

/*
 * pipe_read_event --
 *     Read one complete event from the pipe. Returns false on EOF (the leader died). The leader's
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
            testutil_die(errno, "follower read pipe");
        }
        if (nr == 0)
            return (false);
        have += (size_t)nr;
    }
    return (true);
}

/*
 * follower_main --
 *     Follower role entry point; never returns.
 */
void
follower_main(TEST_CONFIG *cfg)
{
    /* The follower only reads from the event pipe. */
    testutil_assert(cfg->pipe_read_fd >= 0 && cfg->pipe_write_fd >= 0);
    close(cfg->pipe_write_fd);
    cfg->pipe_write_fd = -1;

    if (chdir(cfg->home) != 0)
        testutil_die(errno, "Follower chdir: %s", cfg->home);

    cfg->opts->disagg.is_enabled = true;
    cfg->opts->disagg.mode = "follower";
    cfg->opts->disagg.page_log = "palite";
    cfg->opts->disagg.page_log_home = cfg->page_log_home;
    cfg->opts->disagg.drain_threads = 1;

    WT_CONNECTION *conn;
    testutil_wiredtiger_open(
      cfg->opts, FOLLOWER_HOME_DIR, ENV_CONFIG_DEF, NULL, &conn, false, false);

    WT_SESSION *session;
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    WT_PAGE_LOG *page_log;
    testutil_check(conn->get_page_log(conn, "palite", &page_log));

    FILE *record_fps[MAX_TH] = {0};
    bool picked_up = false;

    SCHEMA_EVENT ev;
    while (pipe_read_event(cfg->pipe_read_fd, &ev))
        switch (ev.type) {
        case EVENT_CREATE:
        case EVENT_DROP:
        case EVENT_INSERT:
            follower_record_event(record_fps, &ev);
            break;
        case EVENT_CKPT:
            follower_pick_up_checkpoint(conn, session, page_log, &picked_up);
            break;
        }

    for (uint32_t i = 0; i < MAX_TH; i++)
        if (record_fps[i] != NULL)
            (void)fclose(record_fps[i]);
    memset(record_fps, 0, sizeof(record_fps));
    testutil_check(page_log->terminate(page_log, NULL));

    /* Step up only when the test intended the leader to die first. */
    if (cfg->kill_mode == KILL_LEADER)
        follower_step_up(conn, session);

    testutil_check(session->close(session, NULL));
    testutil_check(conn->close(conn, "debug=(skip_checkpoint=true)"));
    exit(EXIT_SUCCESS);
}
