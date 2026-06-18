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
 * A table: URI with an empty config string. The absent "columns" key causes
 * __wt_config_gets to return WT_NOTFOUND inside
 * __metadata_clean_incomplete_table, exercising the error-propagation path in
 * __recovery_file_scan. No companion colgroup: or file: entry is created, so
 * the table is also structurally incomplete.
 */
#define TABLE_URI "table:wt17866_txn_recovery_error"
#define TABLE_CONFIG ""

/*
 * setup_db --
 *     Open a fresh database, insert a table: entry with an empty config string directly into the
 *     metadata, and close cleanly so the entry is written to the checkpoint.
 */
static void
setup_db(const char *home)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    WT_SESSION_IMPL *session_impl;

    testutil_check(wiredtiger_open(home, NULL, "create", &conn));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    session_impl = (WT_SESSION_IMPL *)session;
    testutil_check(__wt_metadata_insert(session_impl, TABLE_URI, TABLE_CONFIG));
    testutil_check(conn->close(conn, ""));
}

/*
 * main --
 *     Open a database containing an incomplete table: entry and verify that the resulting recovery
 *     error is propagated to the caller.
 */
int
main(int argc, char *argv[])
{
    TEST_OPTS *opts, _opts;
    WT_CONNECTION *conn;
    int ret;

    opts = &_opts;
    memset(opts, 0, sizeof(*opts));
    testutil_check(testutil_parse_opts(argc, argv, opts));
    testutil_recreate_dir(opts->home);

    setup_db(opts->home);

    /*
     * Reopen the database. Recovery scans all table: entries and calls
     * __metadata_clean_incomplete_table for each. The empty config string
     * has no "columns" key, so __wt_config_gets returns WT_NOTFOUND.
     * __recovery_file_scan must propagate this error; wiredtiger_open must
     * not return success.
     */
    ret = wiredtiger_open(opts->home, NULL, "", &conn);
    testutil_assert(ret != 0);

    testutil_cleanup(opts);
    return (EXIT_SUCCESS);
}
