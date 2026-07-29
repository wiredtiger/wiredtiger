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

#include "format.h"

/*
 * follower_fetch_full_metadata --
 *     Fetch the full checkpoint metadata from the page log.
 */
int
follower_fetch_full_metadata(WT_SESSION *session, WT_PAGE_LOG *page_log,
  const WT_ITEM *checkpoint_metadata, WT_ITEM *full_metadata)
{
    WT_CONFIG_ITEM cval;
    WT_DECL_RET;
    WT_PAGE_LOG_GET_ARGS get_args;
    WT_PAGE_LOG_HANDLE *plh;
    uint64_t metadata_lsn;
    uint32_t count;
    char *meta_str;

    meta_str = NULL;
    plh = NULL;
    memset(full_metadata, 0, sizeof(*full_metadata));

    /* Extract the checkpoint_metadata into a null-terminated string for config parsing. */
    WT_ERR(__wt_strndup(
      (WT_SESSION_IMPL *)session, checkpoint_metadata->data, checkpoint_metadata->size, &meta_str));

    /* Extract the metadata_lsn from the checkpoint_metadata. */
    WT_ERR(__wt_config_getones((WT_SESSION_IMPL *)session, meta_str, "metadata_lsn", &cval));
    metadata_lsn = (uint64_t)cval.val;

    /* Open a handle for the metadata table. */
    WT_ERR(page_log->pl_open_handle(page_log, session, WT_SPECIAL_PALI_TURTLE_FILE_ID, &plh));

    /* Read the metadata page at the specified LSN. */
    memset(&get_args, 0, sizeof(get_args));
    get_args.lsn = metadata_lsn;
    count = 1;
    WT_ERR(plh->plh_get(
      plh, session, WT_DISAGG_METADATA_MAIN_PAGE_ID, 0, &get_args, full_metadata, &count));

    if (count == 0) {
        ret = WT_NOTFOUND;
        goto err;
    }

err:
    if (plh != NULL)
        testutil_check(plh->plh_close(plh, session));
    __wt_free((WT_SESSION_IMPL *)session, meta_str);
    return (ret);
}

/*
 * follower_try_pickup_checkpoint --
 *     Attempt to pick up a checkpoint. Returns true if the checkpoint was picked up, false if
 *     skipped due to timestamp constraints (checkpoint's oldest timestamp > follower's
 *     pinned_timestamp).
 */
static bool
follower_try_pickup_checkpoint(WT_SESSION *session, WT_CONNECTION *conn, WT_PAGE_LOG *page_log,
  WT_ITEM *checkpoint_metadata, wt_timestamp_t checkpoint_ts)
{
    WT_DISAGG_METADATA metadata;
    WT_ITEM full_metadata;
    wt_timestamp_t pinned_ts;
    char config[1024];
    bool picked_up;

    picked_up = false;
    memset(&full_metadata, 0, sizeof(full_metadata));

    /*
     * Before picking up the checkpoint, compare the checkpoint's oldest timestamp with the
     * follower's current pinned timestamp. If the checkpoint's oldest timestamp is greater than the
     * pinned timestamp, we cannot safely pick up this checkpoint yet - skip it and wait for the
     * next attempt when timestamps have caught up.
     *
     * The checkpoint_metadata from pl_get_complete_checkpoint() only contains pointer information
     * (metadata_lsn, etc.). We need to fetch the actual metadata page from the page log to get the
     * full checkpoint config with oldest_timestamp.
     */
    testutil_assert(g.transaction_timestamps_config);
    testutil_check(
      follower_fetch_full_metadata(session, page_log, checkpoint_metadata, &full_metadata));
    testutil_check(__wt_disagg_parse_meta((WT_SESSION_IMPL *)session, &full_metadata, &metadata));
    testutil_assert(metadata.oldest_timestamp != WT_TS_NONE);
    testutil_check(timestamp_query("get=pinned", &pinned_ts));
    if (pinned_ts != WT_TS_NONE && metadata.oldest_timestamp > pinned_ts) {
        printf("--- [Follower] Skipping checkpoint pickup: oldest_timestamp(hex)=%" PRIx64
               " > pinned_timestamp(hex)=%" PRIx64 " ---\n",
          metadata.oldest_timestamp, pinned_ts);
        goto done;
    }

    testutil_snprintf(config, sizeof(config), "disaggregated=(checkpoint_meta=\"%.*s\")",
      (int)checkpoint_metadata->size, (const char *)checkpoint_metadata->data);
    testutil_check(conn->reconfigure(conn, config));
    printf("--- [Follower] Picked up checkpoint (metadata=[%.*s],timestamp=%#" PRIx64 ") ---\n",
      (int)checkpoint_metadata->size, (const char *)checkpoint_metadata->data, checkpoint_ts);
    picked_up = true;

done:
    free(full_metadata.mem);
    return (picked_up);
}

/*
 * follower_read_latest_checkpoint --
 *     Read the latest checkpoint. Only followers should be able to do so.
 */
void
follower_read_latest_checkpoint(void)
{
    SAP sap;
    WT_CONNECTION *conn;
    WT_DECL_RET;
    WT_PAGE_LOG *page_log;
    WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args;
    WT_SESSION *session;
    const char *disagg_page_log;

    conn = g.wts_conn;
    disagg_page_log = (char *)GVS(DISAGG_PAGE_LOG);
    memset(&sap, 0, sizeof(sap));
    memset(&args, 0, sizeof(args));

    /* Only follower can pickup checkpoints. */
    testutil_assert(!g.disagg_leader);
    testutil_check(conn->get_page_log(conn, disagg_page_log, &page_log));

    wt_wrap_open_session(conn, &sap, NULL, NULL, &session);
    ret = page_log->pl_get_complete_checkpoint(page_log, session, &args);
    testutil_check_error_ok(ret, WT_NOTFOUND);
    if (ret != WT_NOTFOUND)
        (void)follower_try_pickup_checkpoint(
          session, conn, page_log, &args.checkpoint_metadata, args.checkpoint_timestamp);

    free(args.checkpoint_metadata.mem);
    wt_wrap_close_session(session);
    testutil_check(page_log->terminate(page_log, NULL));
}

/*
 * follower_read_no_ts --
 *     Repeatedly run transactional snapshot reads with no read timestamp on the follower, racing
 *     checkpoint pickups: scan a table's first rows twice within one transaction, through freshly
 *     opened cursors, and fail on any difference. The snapshot must observe exactly one state for
 *     its lifetime; a refused read (rollback) is an acceptable outcome and retried.
 */
WT_THREAD_RET
follower_read_no_ts(void *arg)
{
#define FOLLOWER_READ_ROWS 200
#define FOLLOWER_READ_PASSES 12
    SAP sap;
    TABLE *table;
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_ITEM key, value;
    WT_ITEM keys[FOLLOWER_READ_ROWS], values[FOLLOWER_READ_ROWS];
    WT_ITEM start_key;
    WT_SESSION *session;
    uint64_t iterations, start_keyno;
    u_int count, i, pass;
    int exact;
    bool failed;

    (void)(arg); /* Unused parameter */
    conn = g.wts_conn;
    memset(keys, 0, sizeof(keys));
    memset(values, 0, sizeof(values));

    /* Restrict to row-store, like the random cursor reader. */
    for (i = 0; i <= ntables; ++i)
        if (tables[i] != NULL && tables[i]->type == ROW)
            break;
    if (i > ntables)
        return (WT_THREAD_RET_VALUE);

    memset(&sap, 0, sizeof(sap));
    wt_wrap_open_session(conn, &sap, NULL, NULL, &session);

    printf("--- [Follower] snapshot read stress running ---\n");
    for (iterations = 0; !g.workers_finished; ++iterations) {
        table = table_select_type(ROW, false);
        if (table == NULL)
            break;
        testutil_check(session->begin_transaction(session, "isolation=snapshot"));

        /* Scan from a random position, so updates anywhere in the table are candidates. */
        start_keyno = mmrand(&g.extra_rnd, 1, table->rows_current);
        key_gen_init(&start_key);
        key_gen(table, &start_key, start_keyno);

        count = 0;
        failed = false;
        for (pass = 0; pass < FOLLOWER_READ_PASSES && !failed && !g.workers_finished; ++pass) {
            /*
             * Hold the snapshot across pickups: the first pass records the baseline and the later
             * passes re-read it every few hundred milliseconds, so most transactions span an
             * adoption and every scanned row is a candidate to catch a leaked change.
             */
            if (pass > 0)
                __wt_sleep(0, mmrand(&g.extra_rnd, 200, 400) * WT_THOUSAND);
            wt_wrap_open_cursor(session, table->uri, NULL, &cursor);
            cursor->set_key(cursor, &start_key);
            if ((ret = cursor->search_near(cursor, &exact)) != 0) {
                testutil_assertfmt(ret == WT_NOTFOUND || ret == WT_ROLLBACK ||
                    ret == WT_PREPARE_CONFLICT || ret == WT_CACHE_FULL,
                  "follower_read_no_ts: search_near: %d", ret);
                failed = true;
                testutil_check(cursor->close(cursor));
                break;
            }
            for (i = 0; i < FOLLOWER_READ_ROWS; ++i) {
                if (i > 0 && (ret = cursor->next(cursor)) != 0) {
                    testutil_assertfmt(ret == WT_NOTFOUND || ret == WT_ROLLBACK ||
                        ret == WT_PREPARE_CONFLICT || ret == WT_CACHE_FULL,
                      "follower_read_no_ts: next: %d", ret);
                    /* A refusal or conflict abandons the transaction; it is not a failure. */
                    failed = ret != WT_NOTFOUND;
                    if (ret == WT_NOTFOUND && pass > 0)
                        testutil_assertfmt(i == count,
                          "follower_read_no_ts: snapshot row count changed within a transaction "
                          "(%u != %u)",
                          i, count);
                    break;
                }
                testutil_check(cursor->get_key(cursor, &key));
                testutil_check(cursor->get_value(cursor, &value));
                if (pass == 0) {
                    testutil_check(
                      __wt_buf_set((WT_SESSION_IMPL *)session, &keys[i], key.data, key.size));
                    testutil_check(
                      __wt_buf_set((WT_SESSION_IMPL *)session, &values[i], value.data, value.size));
                    count = i + 1;
                } else {
                    /* The second pass must observe exactly the first pass's rows. */
                    testutil_assertfmt(i < count && key.size == keys[i].size &&
                        memcmp(key.data, keys[i].data, key.size) == 0,
                      "follower_read_no_ts: snapshot key changed within a transaction (row %u)", i);
                    testutil_assertfmt(value.size == values[i].size &&
                        memcmp(value.data, values[i].data, value.size) == 0,
                      "follower_read_no_ts: snapshot value changed within a transaction (row %u)", i);
                }
            }
            testutil_check(cursor->close(cursor));
        }
        key_gen_teardown(&start_key);
        testutil_check(session->rollback_transaction(session, NULL));
    }
    printf("--- [Follower] snapshot read stress: %" PRIu64 " transactions ---\n", iterations);

    for (i = 0; i < FOLLOWER_READ_ROWS; ++i) {
        __wt_buf_free((WT_SESSION_IMPL *)session, &keys[i]);
        __wt_buf_free((WT_SESSION_IMPL *)session, &values[i]);
    }
    wt_wrap_close_session(session);
    return (WT_THREAD_RET_VALUE);
}

/*
 * follower --
 *     Periodically check for a new checkpoint from the leader, and reconfigure to use it.
 */
WT_THREAD_RET
follower(void *arg)
{
    SAP sap;
    WT_CONNECTION *conn;
    WT_DECL_RET;
    WT_PAGE_LOG *page_log;
    WT_PAGE_LOG_GET_COMPLETE_CHECKPOINT_ARGS args;
    WT_SESSION *session;
    const char *disagg_page_log;
    u_int period;

    (void)(arg); /* Unused parameter */
    conn = g.wts_conn;
    disagg_page_log = (char *)GVS(DISAGG_PAGE_LOG);
    memset(&sap, 0, sizeof(sap));
    memset(&args, 0, sizeof(args));

    wt_wrap_open_session(conn, &sap, NULL, NULL, &session);
    testutil_check(conn->get_page_log(conn, disagg_page_log, &page_log));

    while (!g.workers_finished) {
        /*
         * FIXME-WT-15788: Eventually have the leader send checkpoint metadata to the follower (via
         * shared memory or pipe) so it can be picked up. Required once we start running against the
         * library version of PALI, which doesn't implement pl_get_complete_checkpoint().
         */
        free(args.checkpoint_metadata.mem);
        memset(&args, 0, sizeof(args));
        ret = page_log->pl_get_complete_checkpoint(page_log, session, &args);
        testutil_check_error_ok(ret, WT_NOTFOUND);
        /* Only reconfigure if there's a new checkpoint. */
        if (ret != WT_NOTFOUND) {
            if (g.checkpoint_metadata[0] == '\0' ||
              memcmp(g.checkpoint_metadata, (const char *)args.checkpoint_metadata.data,
                args.checkpoint_metadata.size) != 0) {
                if (follower_try_pickup_checkpoint(session, conn, page_log,
                      &args.checkpoint_metadata, args.checkpoint_timestamp))
                    testutil_snprintf(g.checkpoint_metadata, sizeof(g.checkpoint_metadata), "%.*s",
                      (int)args.checkpoint_metadata.size,
                      (const char *)args.checkpoint_metadata.data);
            }
        }
        period = mmrand(&g.extra_rnd, 1, 3);
        while (period > 0 && !g.workers_finished) {
            --period;
            __wt_sleep(1, 0);
        }
    }
    free(args.checkpoint_metadata.mem);
    wt_wrap_close_session(session);
    testutil_check(page_log->terminate(page_log, NULL));

    return (WT_THREAD_RET_VALUE);
}
