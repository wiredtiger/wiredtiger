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
#include "wt_internal.h"

/*
 * create_session --
 *     Open a session and create a layered, disagg-backed table at the given URI.
 */
static WT_SESSION *
create_session(WT_CONNECTION *conn, const char *uri)
{
    WT_SESSION *session;

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    const char *config =
      "key_format=i,value_format=i,log=(enabled=false),"
      "type=layered,block_manager=disagg";

    testutil_check(session->create(session, uri, config));

    return session;
}

/*
 * populate_with_data --
 *     Insert keys in the inclusive range [first_key, last_key] into the table.
 */
static void
populate_with_data(WT_SESSION *session, const char *uri, int first_key, int last_key)
{
    WT_CURSOR *c;
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &c));

    for (int i = first_key; i <= last_key; i++) {
        c->set_key(c, i);
        c->set_value(c, i);
        testutil_check(c->insert(c));
    }

    testutil_check(c->close(c));
}

/*
 * update_key --
 *     Update a single key in the table to create an ingest entry for it.
 */
static void
update_key(WT_SESSION *session, const char *uri, int key)
{
    WT_CURSOR *c;
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &c));

    /* Change the value of the provided key. */
    c->set_key(c, key);
    c->set_value(c, 1234);
    testutil_check(c->update(c));

    testutil_check(c->close(c));
}

/*
 * table_truncate --
 *     Perform a truncate operation on the provided key range.
 */
static void
table_truncate(WT_SESSION *session, const char *uri, int start_key, int end_key)
{
    WT_CURSOR *start, *stop;
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &start));
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &stop));

    start->set_key(start, start_key);
    stop->set_key(stop, end_key);

    WT_SESSION_IMPL *session_impl = (WT_SESSION_IMPL *)session;
    testutil_check(__wt_session_range_truncate(session_impl, NULL, start, stop));

    testutil_check(start->close(start));
    testutil_check(stop->close(stop));
}

/*
 * test_truncate_with_promotion --
 *     Test body that drives the WT-16995 scenario.
 */
static void
test_truncate_with_promotion(WT_CONNECTION *conn)
{
    static const char *uri = "table:disagg_truncate";
    WT_SESSION *session = create_session(conn, uri);

    /* 1. As leader, insert 1...100 into the stable table. */
    const int first_key = 1, last_key = 100;
    populate_with_data(session, uri, first_key, last_key);

    /* 2. Switch to the follower role. */
    testutil_check(conn->reconfigure(conn, "disaggregated=(role=\"follower\")"));

    /* 3. As follower, create an ingest update for key 20. */
    const int key = 20;
    update_key(session, uri, key);

    /* 4. Promote to leader. */
    ((WT_CONNECTION_IMPL *)conn)->layered_table_manager.leader = true;

    /* 5. Perform a truncate operation, where key 20 is within the range. */
    const int truncate_start = 20, truncate_end = 30;
    table_truncate(session, uri, truncate_start, truncate_end); /* Assert raised in here. */

    /* 6. Clean up. */
    testutil_check(session->close(session, NULL));
}

/*
 * set_up_test_opts --
 *     Enable disaggregated storage in the test framework - required to reproduce the issue in
 *     WT-16695.
 */
static void
set_up_test_opts(TEST_OPTS *opts, int argc, char *argv[])
{
    memset(opts, 0, sizeof(TEST_OPTS));

    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_deduce_build_dir(opts);

    /* Enable disaggregated storage with basic defaults. */
    opts->disagg.is_enabled = true;
    opts->disagg.mode = "leader";
    opts->disagg.page_log = "palite";
    opts->disagg.page_log_home = opts->home;
    opts->disagg.drain_threads = 4;
    opts->disagg.internal_page_delta = true;
    opts->disagg.leaf_page_delta = true;
    opts->disagg.key_provider = false;
}

/*
 * main --
 *     This test reproduces the assertion in WT-16695, where a layered cursor truncate can hit an
 *     assert in __clayered_remove_leader().
 *
 * When stepping up from follower to leader, the assert observes a state where the layered cursor is
 *     positioned, yet the stable cursor is not.
 *
 * Given a range of keys in a layered table, the test: - Ensures one key is present in both stable
 *     and ingest tables. - Starts a range truncate in follower mode. - Promotes to leader during
 *     the truncate.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS opts;
    set_up_test_opts(&opts, argc, argv);
    testutil_recreate_dir(opts.home);

    /* Open WiredTiger connection. */
    WT_CONNECTION *conn;
    const char *config = "create,statistics=(fast),log=(enabled=false),precise_checkpoint=true";
    testutil_wiredtiger_open(&opts, opts.home, config, NULL, &conn, false, false);
    testutil_check(conn->set_timestamp(conn, "stable_timestamp=1"));

    /* The test itself. */
    test_truncate_with_promotion(conn);

    /* Clean up. */
    testutil_check(conn->close(conn, NULL));
    testutil_remove(opts.home);
    testutil_cleanup(&opts);

    return EXIT_SUCCESS;
}
