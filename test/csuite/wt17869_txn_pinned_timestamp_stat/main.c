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
#include "test_util.h"

#define OLDEST_TS 10
#define COMMIT_TS 20
#define ADVANCED_OLDEST_TS 30

/*
 * read_conn_stat --
 *     Read a single connection statistic by key and return its value.
 */
static int64_t
read_conn_stat(WT_SESSION *session, int stat_key)
{
    WT_CURSOR *cursor;
    int64_t value;
    const char *desc, *pvalue;

    testutil_check(session->open_cursor(session, "statistics:", NULL, "statistics=(all)", &cursor));
    cursor->set_key(cursor, stat_key);
    testutil_check(cursor->search(cursor));
    testutil_check(cursor->get_value(cursor, &desc, &pvalue, &value));
    testutil_check(cursor->close(cursor));

    return (value);
}

/*
 * check_stat_non_negative --
 *     Fail the test if the named statistic has a negative value.
 */
static void
check_stat_non_negative(WT_SESSION *session, int stat_key, const char *label)
{
    int64_t value;

    value = read_conn_stat(session, stat_key);
    testutil_assertfmt(value >= 0, "%s: expected non-negative value, got %" PRId64, label, value);
}

/*
 * main --
 *     Entry point for the txn_pinned_timestamp_oldest statistic test.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    int64_t durable_ts, oldest_ts, pinned_oldest_stat;
    char ts_cfg[64];

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_recreate_dir(opts->home);

    testutil_check(wiredtiger_open(opts->home, NULL, "create,statistics=(all)", &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &opts->session));
    session = opts->session;

    /*
     * Set oldest and stable timestamps, then commit a transaction at a timestamp above oldest.
     * After the commit durable_timestamp equals the commit timestamp. The stat must equal durable -
     * oldest.
     */
    testutil_check(opts->conn->set_timestamp(opts->conn, "oldest_timestamp=a,stable_timestamp=a"));

    testutil_check(session->create(session, "table:stat_test", "key_format=i,value_format=i"));
    testutil_check(session->open_cursor(session, "table:stat_test", NULL, NULL, &cursor));

    testutil_check(session->begin_transaction(session, NULL));
    cursor->set_key(cursor, 1);
    cursor->set_value(cursor, 1);
    testutil_check(cursor->insert(cursor));
    testutil_check(session->commit_transaction(session, "commit_timestamp=14"));

    testutil_check(cursor->close(cursor));

    pinned_oldest_stat = read_conn_stat(session, WT_STAT_CONN_TXN_PINNED_TIMESTAMP_OLDEST);
    durable_ts = read_conn_stat(session, WT_STAT_CONN_TXN_GLOBAL_DURABLE_TIMESTAMP);
    oldest_ts = read_conn_stat(session, WT_STAT_CONN_TXN_GLOBAL_OLDEST_TIMESTAMP);

    testutil_assertfmt(pinned_oldest_stat >= 0,
      "phase 1: txn_pinned_timestamp_oldest must be non-negative, got %" PRId64,
      pinned_oldest_stat);

    testutil_assertfmt(durable_ts == COMMIT_TS,
      "phase 1: expected durable_timestamp=%" PRId64 ", got %" PRId64, (int64_t)COMMIT_TS,
      durable_ts);

    testutil_assertfmt(oldest_ts == OLDEST_TS,
      "phase 1: expected oldest_timestamp=%" PRId64 ", got %" PRId64, (int64_t)OLDEST_TS,
      oldest_ts);

    testutil_assertfmt(pinned_oldest_stat == durable_ts - oldest_ts,
      "phase 1: txn_pinned_timestamp_oldest=%" PRId64 " != durable-oldest=%" PRId64,
      pinned_oldest_stat, durable_ts - oldest_ts);

    testutil_snprintf(ts_cfg, sizeof(ts_cfg), "oldest_timestamp=1e,stable_timestamp=1e");
    testutil_check(opts->conn->set_timestamp(opts->conn, ts_cfg));

    oldest_ts = read_conn_stat(session, WT_STAT_CONN_TXN_GLOBAL_OLDEST_TIMESTAMP);
    testutil_assertfmt(oldest_ts == ADVANCED_OLDEST_TS,
      "phase 2: expected oldest_timestamp=%" PRId64 ", got %" PRId64, (int64_t)ADVANCED_OLDEST_TS,
      oldest_ts);

    check_stat_non_negative(
      session, WT_STAT_CONN_TXN_PINNED_TIMESTAMP_OLDEST, "txn_pinned_timestamp_oldest");

    check_stat_non_negative(
      session, WT_STAT_CONN_TXN_PINNED_TIMESTAMP_LAG, "txn_pinned_timestamp_lag");
    check_stat_non_negative(session, WT_STAT_CONN_TXN_PINNED_TIMESTAMP_CHECKPOINT_LAG,
      "txn_pinned_timestamp_checkpoint_lag");
    check_stat_non_negative(
      session, WT_STAT_CONN_TXN_PINNED_TIMESTAMP_READER_LAG, "txn_pinned_timestamp_reader_lag");

    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
