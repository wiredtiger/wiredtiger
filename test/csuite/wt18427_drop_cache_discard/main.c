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

/*
 * This is a regression test for WT-18427: a non-forced WT_SESSION::drop of an already-clean
 * (checkpointed) table used to close its dhandle by walking and freeing every page resident in
 * cache, synchronously, while holding the schema and dhandle-list write locks. The fix marks a
 * clean tree's dhandle dead instead and defers the cache discard to the sweep server, so the drop
 * call itself never fully closes the handle.
 *
 * A clean tree has nothing dirty to flush either way, so comparing the backing file's bytes before
 * and after the drop cannot tell the two behaviors apart: neither path writes to the file. The
 * property that does tell them apart is whether the file's own dhandle is torn down as part of the
 * drop call. Dropping a simple table closes two dhandles: the table-layer one (always closed
 * synchronously, its type isn't subject to the clean/dirty distinction the fix makes) and the
 * underlying file/btree one (the one the fix defers to sweep when the tree is clean). So
 * WT_STAT_CONN_BTREE_OPEN (conn->open_btree_count) drops by 1 across a clean-tree drop with the fix
 * applied, and by 2 without it. Checking it immediately after drop() returns needs no sleep or
 * polling: the call has already completed, so whatever it was going to do to that counter has
 * already happened.
 *
 * A second scenario checks the fix left the existing dirty-data protection alone: a table with
 * committed but uncheckpointed content must still refuse a non-forced drop with EBUSY, exactly as
 * before.
 */

#define TABLE_ROWS (10 * WT_THOUSAND)
#define VALUE_SIZE 200

/*
 * read_btree_open_stat --
 *     Return the connection's "btrees currently open" statistic (conn->open_btree_count).
 */
static int64_t
read_btree_open_stat(WT_SESSION *session)
{
    WT_CURSOR *cursor;
    int64_t value;
    const char *desc, *pvalue;

    testutil_check(session->open_cursor(session, "statistics:", NULL, NULL, &cursor));
    cursor->set_key(cursor, WT_STAT_CONN_BTREE_OPEN);
    testutil_check(cursor->search(cursor));
    testutil_check(cursor->get_value(cursor, &desc, &pvalue, &value));
    testutil_check(cursor->close(cursor));

    return (value);
}

/*
 * populate --
 *     Create a table and insert a fixed number of rows with a fixed-size value.
 */
static void
populate(WT_SESSION *session, const char *uri, uint64_t rows)
{
    WT_CURSOR *cursor;
    uint64_t i;
    char value[VALUE_SIZE];

    memset(value, 'a', sizeof(value) - 1);
    value[sizeof(value) - 1] = '\0';

    testutil_check(session->create(session, uri, "key_format=Q,value_format=S"));
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    for (i = 0; i < rows; ++i) {
        cursor->set_key(cursor, i);
        cursor->set_value(cursor, value);
        testutil_check(cursor->insert(cursor));
    }
    testutil_check(cursor->close(cursor));
}

/*
 * test_clean_drop_defers_handle_close --
 *     A non-forced drop of an already-clean table must not tear down its file/btree handle as part
 *     of the drop call: that handle is marked dead and left for sweep, so open_btree_count drops by
 *     exactly 1 (the table-layer handle only) immediately after the drop returns, not by 2.
 */
static void
test_clean_drop_defers_handle_close(WT_SESSION *session)
{
    int64_t btree_open_after, btree_open_before;
    const char *uri = "table:wt18427_clean";

    populate(session, uri, TABLE_ROWS);

    /*
     * Give the oldest id time to catch up: otherwise checkpoint's own reconciliation can find the
     * last insert not yet globally visible, skip it, and leave the tree modified again right after
     * marking it clean.
     */
    sleep(1);
    testutil_check(session->checkpoint(session, NULL));

    btree_open_before = read_btree_open_stat(session);
    testutil_check(session->drop(session, uri, NULL));
    btree_open_after = read_btree_open_stat(session);

    testutil_assertfmt(btree_open_after == btree_open_before - 1,
      "open_btree_count changed by %" PRId64 " across a clean-tree drop (%" PRId64 " before, %"
      PRId64 " after) -- expected exactly -1: the file/btree handle should have been marked dead "
      "and deferred to sweep, not closed synchronously",
      btree_open_after - btree_open_before, btree_open_before, btree_open_after);
}

/*
 * test_dirty_drop_still_fails --
 *     A non-forced drop of a table with committed but uncheckpointed content must still fail with
 *     EBUSY: the fix only changes what happens to an already-clean tree.
 */
static void
test_dirty_drop_still_fails(WT_SESSION *session)
{
    const char *uri = "table:wt18427_dirty";

    populate(session, uri, 1);

    testutil_assert(session->drop(session, uri, NULL) == EBUSY);
}

/*
 * main --
 *     Test's entry point.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_SESSION *session;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_recreate_dir(opts->home);

    testutil_check(
      wiredtiger_open(opts->home, NULL, "create,cache_size=1G,statistics=(all)", &opts->conn));
    testutil_check(opts->conn->open_session(opts->conn, NULL, NULL, &session));

    test_clean_drop_defers_handle_close(session);
    test_dirty_drop_still_fails(session);

    testutil_cleanup(opts);

    return (EXIT_SUCCESS);
}
