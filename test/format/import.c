/*-
 * Public Domain 2014-2020 MongoDB, Inc.
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

static void copy_file_into_directory(WT_SESSION *session, const char *dir, const char *name);
static void import_with_repair(WT_SESSION *session);
static void import_with_file_metadata(WT_SESSION *session, WT_SESSION *import_session);
static void verify_import(WT_SESSION *session, int start_value);

/*
 * Import directory initialize command, remove and re-create the primary backup directory, plus a
 * copy we maintain for recovery testing.
 */
#define HOME_IMPORT_INIT_CMD "rm -rf %s/IMPORT && mkdir %s/IMPORT"
#define IMPORT_DIRNAME "IMPORT"
#define IMPORT_URI "table:import"
#define IMPORT_URI_CONFIG "key_format=i,value_format=i"
#define IMPORT_ENTRIES 1000

/*
 * import --
 *     Periodically import table.
 */
WT_THREAD_RET
import(void *arg)
{
    WT_CONNECTION *conn, *import_conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION *import_session, *session;
    size_t len;
    u_int period;
    int counter;
    uint32_t import_value;
    char *cmd;

    (void)(arg);
    conn = g.wts_conn;
    import_value = false;
    counter = 0;

    len = strlen(g.home) * 2 + strlen(HOME_IMPORT_INIT_CMD) + 1;
    cmd = dmalloc(len);
    testutil_check(__wt_snprintf(cmd, len, HOME_IMPORT_INIT_CMD, g.home, g.home));
    testutil_checkfmt(system(cmd), "%s", "import directory creation failed");
    free(cmd);

    len = strlen(g.home) + strlen(IMPORT_DIRNAME) + 10;
    cmd = dmalloc(len);
    testutil_check(__wt_snprintf(cmd, len, "%s/%s", g.home, IMPORT_DIRNAME));
    /* Open a connection to the database, creating it if necessary. */
    testutil_check(wiredtiger_open(cmd, NULL, "create", &import_conn));
    free(cmd);

    /* Open a session */
    testutil_check(import_conn->open_session(import_conn, NULL, NULL, &import_session));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_checkfmt(
      import_session->create(import_session, IMPORT_URI, IMPORT_URI_CONFIG), "%s", g.uri);

    /*
     * open_cursor can return EBUSY if concurrent with a metadata operation, retry in that case.
     */
    testutil_check(import_session->open_cursor(import_session, IMPORT_URI, NULL, NULL, &cursor));
    while (!g.workers_finished) {
        period = mmrand(NULL, 1, 10);

        for (int i = 0; i < IMPORT_ENTRIES; ++i) {
            cursor->set_key(cursor, i);
            cursor->set_value(cursor, counter + i);
            testutil_check(cursor->insert(cursor));
        }
        counter += IMPORT_ENTRIES;
        testutil_check(import_session->checkpoint(import_session, NULL));

        /* Copy table into current test/format directory */
        copy_file_into_directory(session, IMPORT_DIRNAME, "import.wt");

        import_value = mmrand(NULL, 0, 1);
        if (import_value == 0) {
            import_with_repair(session);
        } else {
            import_with_file_metadata(session, import_session);
        }

        /* Drop import table, so we can import next iteration */
        while ((ret = session->drop(session, IMPORT_URI, NULL)) == EBUSY) {
            __wt_yield();
        }
        testutil_check(ret);

        verify_import(import_session, counter - IMPORT_ENTRIES);
        while (period > 0 && !g.workers_finished) {
            --period;
            __wt_sleep(1, 0);
        }
    }

    testutil_check(cursor->close(cursor));
    testutil_check(import_session->close(import_session, NULL));
    testutil_check(import_conn->close(import_conn, NULL));
    testutil_check(session->close(session, NULL));
    return (WT_THREAD_RET_VALUE);
}

/*
 * verify_import --
 *     Verify all the values inside the imported table.
 */
static void
verify_import(WT_SESSION *session, int start_value)
{
    WT_CURSOR *cursor;
    WT_DECL_RET;
    int counter, key, value;

    counter = 0;
    testutil_check(session->open_cursor(session, IMPORT_URI, NULL, NULL, &cursor));

    while ((ret = cursor->next(cursor)) == 0) {
        error_check(cursor->get_key(cursor, &key));
        testutil_assert(key == counter);
        error_check(cursor->get_value(cursor, &value));
        testutil_assert(value == counter + start_value);
        counter++;
    }
    testutil_assert(counter == IMPORT_ENTRIES);
    scan_end_check(ret == WT_NOTFOUND);
}

/*
 * import_with_repair --
 *     Perform import with repair option
 */
static void
import_with_repair(WT_SESSION *session)
{
    WT_DECL_RET;
    char buf[2048];

    memset(buf, 0, sizeof(buf));
    testutil_check(__wt_snprintf(buf, sizeof(buf), "import=(enabled,repair=true)"));
    if ((ret = session->create(session, IMPORT_URI, buf)) != 0)
        testutil_die(ret, "session.import", ret);
}

/*
 * import_with_file_metadata --
 *     Perform import with the file metadata information.
 */
static void
import_with_file_metadata(WT_SESSION *session, WT_SESSION *import_session)
{
    WT_CURSOR *metadata_cursor;
    WT_DECL_RET;
    char buf[2048];
    const char *table_config, *file_config;

    memset(buf, 0, sizeof(buf));
    testutil_check(
      import_session->open_cursor(import_session, "metadata:", NULL, NULL, &metadata_cursor));

    metadata_cursor->set_key(metadata_cursor, IMPORT_URI);
    metadata_cursor->search(metadata_cursor);
    metadata_cursor->get_value(metadata_cursor, &table_config);

    metadata_cursor->set_key(metadata_cursor, "file:import.wt");
    metadata_cursor->search(metadata_cursor);
    metadata_cursor->get_value(metadata_cursor, &file_config);
    testutil_check(__wt_snprintf(buf, sizeof(buf),
      "%s,import=(enabled,repair=false,file_metadata=(%s))", table_config, file_config));
    if ((ret = session->create(session, IMPORT_URI, buf)) != 0)
        testutil_die(ret, "session.import", ret);

    testutil_check(metadata_cursor->close(metadata_cursor));
}

/*
 * copy_file_into_directory --
 *     Copy a single file into the current session directory.
 */
static void
copy_file_into_directory(WT_SESSION *session, const char *dir, const char *name)
{
    size_t len;
    char *from;

    len = strlen(dir) + strlen(name) + 10;
    from = dmalloc(len);
    testutil_check(__wt_snprintf(from, len, "%s/%s", dir, name));
    testutil_check(__wt_copy_and_sync(session, from, name));
    free(from);
}
