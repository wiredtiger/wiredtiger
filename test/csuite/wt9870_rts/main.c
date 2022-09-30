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
#include "../random_directio/util.c"

/*
 * TODO
 */

/*
 * main --
 *     Top level test.
 */
int
main(int argc, char *argv[])
{
    WT_CONNECTION *conn = NULL;
    WT_SESSION *session = NULL;
    WT_CURSOR *cursor = NULL;
    WT_CURSOR *evict_cursor = NULL;
    char tscfg[1000];
    char valueA[500], valueB[500], valueC[500], valueD[500];
    const char *val;

    uint64_t index, nrows = 3, commit_ts, read_ts, oldest_ts, stable_ts;

    /* Initial db directory. */
    const char *home = "WT_TEST";
    /* Db directory after unclean shutdown. */
    const char *destination = "WT_TEST.restart";

    const char *connection_cfg = "create,cache_size=1MB,statistics=(all),log=(enabled=true)";
    const char *uri = "table:rollback_to_stable40";

    /* Create four values. */
    for (index = 0; index < 500 - 1; ++index) {
        valueA[index] = 'a';
        valueB[index] = 'b';
        valueC[index] = 'c';
        valueD[index] = 'd';
    }
    valueA[499] = '\0';
    valueB[499] = '\0';
    valueC[499] = '\0';
    valueD[499] = '\0';

    WT_UNUSED(argc);
    WT_UNUSED(argv);
    testutil_make_work_dir(home);
    testutil_check(wiredtiger_open(home, NULL, connection_cfg, &conn));

    testutil_check(conn->open_session(conn, NULL, "isolation=snapshot", &session));

    /* Create a table without logging. */
    testutil_check(
      session->create(session, uri, "key_format=i,value_format=S,log=(enabled=false)"));

    /* Pin oldest and stable timestamps @ 10. */
    oldest_ts = 10;
    stable_ts = 10;
    testutil_check(__wt_snprintf(tscfg, sizeof(tscfg),
      "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64, oldest_ts, stable_ts));
    testutil_check(conn->set_timestamp(conn, tscfg));

    /* Insert 3 keys with value A. */
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
    testutil_check(session->begin_transaction(session, NULL));

    cursor->set_key(cursor, 1);
    cursor->set_value(cursor, valueA);
    testutil_check(cursor->insert(cursor));

    cursor->set_key(cursor, 2);
    cursor->set_value(cursor, valueA);
    testutil_check(cursor->insert(cursor));

    cursor->set_key(cursor, 3);
    cursor->set_value(cursor, valueA);
    testutil_check(cursor->insert(cursor));

    commit_ts = 20;
    testutil_check(__wt_snprintf(tscfg, sizeof(tscfg), "commit_timestamp=%" PRIx64, commit_ts));
    testutil_check(session->commit_transaction(session, tscfg));

    /* Update the first and last keys with another value with a large timestamp. */
    testutil_check(session->begin_transaction(session, NULL));

    cursor->set_key(cursor, 1);
    cursor->set_value(cursor, valueD);
    testutil_check(cursor->insert(cursor));

    cursor->set_key(cursor, 3);
    cursor->set_value(cursor, valueD);
    testutil_check(cursor->insert(cursor));

    commit_ts = 1000;
    testutil_check(__wt_snprintf(tscfg, sizeof(tscfg), "commit_timestamp=%" PRIx64, commit_ts));
    testutil_check(session->commit_transaction(session, tscfg));

    /* Update the middle key with lots of updates to generate more history. */
    for (index = 21; index < 499; ++index) {
        testutil_check(session->begin_transaction(session, NULL));
        cursor->set_key(cursor, 2);
        testutil_check(__wt_snprintf(tscfg, sizeof(tscfg), "%s%" PRIx64, valueB, index));
        cursor->set_value(cursor, tscfg);
        testutil_check(cursor->insert(cursor));
        commit_ts = index;
        testutil_check(__wt_snprintf(tscfg, sizeof(tscfg), "commit_timestamp=%" PRIx64, commit_ts));
        testutil_check(session->commit_transaction(session, tscfg));
    }

    /* With this checkpoint, all the updates in the history store are persisted to disk. */
    testutil_check(session->checkpoint(session, NULL));

    /* Update the middle key with value C. */
    testutil_check(session->begin_transaction(session, NULL));

    cursor->set_key(cursor, 2);
    cursor->set_value(cursor, valueC);
    testutil_check(cursor->insert(cursor));

    commit_ts = 500;
    testutil_check(__wt_snprintf(tscfg, sizeof(tscfg), "commit_timestamp=%" PRIx64, commit_ts));
    testutil_check(session->commit_transaction(session, tscfg));

    /* Pin oldest and stable to timestamp 500. */
    oldest_ts = 500;
    stable_ts = 500;
    testutil_check(__wt_snprintf(tscfg, sizeof(tscfg),
      "oldest_timestamp=%" PRIx64 ",stable_timestamp=%" PRIx64, oldest_ts, stable_ts));
    testutil_check(conn->set_timestamp(conn, tscfg));

    /* Evict the globally visible update to write to the disk, this will reset the time window. */
    testutil_check(
      session->open_cursor(session, uri, NULL, "debug=(release_evict)", &evict_cursor));
    testutil_check(session->begin_transaction(session, "ignore_prepare=true"));
    evict_cursor->set_key(evict_cursor, 2);
    testutil_check(evict_cursor->search(evict_cursor));

    evict_cursor->get_value(evict_cursor, &val);
    testutil_assert(strcmp(val, valueC) == 0);

    testutil_check(evict_cursor->reset(evict_cursor));
    testutil_check(evict_cursor->close(evict_cursor));
    testutil_check(session->rollback_transaction(session, NULL));

    /* Update middle key with value D. */
    testutil_check(session->begin_transaction(session, NULL));
    cursor->set_key(cursor, 2);
    cursor->set_value(cursor, valueD);
    testutil_check(cursor->insert(cursor));
    commit_ts = 501;
    testutil_check(__wt_snprintf(tscfg, sizeof(tscfg), "commit_timestamp=%" PRIx64, commit_ts));
    testutil_check(session->commit_transaction(session, tscfg));

    /*
     * 1. This checkpoint will move the globally visible update to the first of the key range.
     * 2. The existing updates in the history store are having with a larger timestamp are
     *    obsolete, so they are not explicitly removed.
     * 3. Any of the history store updates that are already evicted will not rewrite by the
     *    checkpoint.
     */
    testutil_check(session->checkpoint(session, NULL));

    /* Verify data is visible and correct. */
    read_ts = 1000;
    testutil_check(__wt_snprintf(tscfg, sizeof(tscfg), "read_timestamp=%" PRIx64, read_ts));
    testutil_check(session->begin_transaction(session, tscfg));
    for (index = 1; index < nrows + 1; ++index) {
        cursor->set_key(cursor, index);
        testutil_check(cursor->search(cursor));
        cursor->get_value(cursor, &val);
        testutil_assert(strcmp(val, valueD) == 0);
    }
    testutil_check(session->rollback_transaction(session, NULL));
    testutil_check(cursor->close(cursor));

    /* Copy all the current files in a new directory. */
    testutil_make_work_dir(destination);
    copy_directory(home, destination, false);
    // return (EXIT_SUCCESS);

    /* Close the connection. */
    testutil_check(conn->close(conn, NULL));

    /*
     * Open the connection from the directory where all the files have been copied to simulate an
     * unclean shutdown.
     */
    testutil_check(wiredtiger_open(destination, NULL, connection_cfg, &conn));

    testutil_check(conn->open_session(conn, NULL, "isolation=snapshot", &session));

    /* Verify data is visible and correct. */
    testutil_check(session->open_cursor(session, uri, NULL, NULL, &cursor));

    read_ts = 1000;
    testutil_check(__wt_snprintf(tscfg, sizeof(tscfg), "read_timestamp=%" PRIx64, read_ts));
    testutil_check(session->begin_transaction(session, tscfg));

    for (index = 1; index < nrows + 1; ++index) {
        cursor->set_key(cursor, index);
        testutil_check(cursor->search(cursor));
        cursor->get_value(cursor, &val);
        if (index % 2 == 0) {
            testutil_assert(strcmp(val, valueC) == 0);
        } else {
            testutil_assert(strcmp(val, valueA) == 0);
        }
    }
    testutil_check(session->rollback_transaction(session, NULL));

    testutil_check(conn->close(conn, NULL));

    return (EXIT_SUCCESS);
}