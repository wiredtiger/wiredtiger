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

static int cursor_insert(WT_CURSOR *cursor, int key, int val);
static void copy_file(WT_SESSION *session, const char *name);

/*
 * Import directory initialize command, remove and re-create the primary backup directory, plus a
 * copy we maintain for recovery testing.
 */
#define HOME_IMPORT_INIT_CMD "rm -rf %s/IMPORT && mkdir %s/IMPORT"
#define IMPORT_URI "table:import"
#define IMPORT_URI_CONFIG "key_format=i,value_format=i"
/*
 * import  --
 *     Periodically import table.
 */
WT_THREAD_RET
import(void *arg)
{
    WT_CONNECTION *conn, *import_conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION *import_session, *session;
    u_int period;
    char buf[32], *cmd;
    size_t len;

    (void)(arg);
    conn = g.wts_conn;

    len = strlen(g.home) * 2 + strlen(HOME_IMPORT_INIT_CMD) + 1;
    cmd = dmalloc(len);
    testutil_check(
        __wt_snprintf(cmd, len, HOME_IMPORT_INIT_CMD, g.home, g.home));
    testutil_checkfmt(system(cmd), "%s", "import directory creation failed");
    free(cmd);

    len = strlen(g.home) + strlen("IMPORT") + 10;
    cmd = dmalloc(len);
    printf("len %lu\n", len);
    testutil_check(__wt_snprintf(cmd, len, "%s/IMPORT", g.home));
    /* Open a connection to the database, creating it if necessary. */
    testutil_check(wiredtiger_open(cmd, NULL, "create", &import_conn));
    free(cmd);

    /* Open a session */
    testutil_check(import_conn->open_session(import_conn, NULL, NULL, &import_session));
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    testutil_checkfmt(import_session->create(import_session, IMPORT_URI, IMPORT_URI_CONFIG), "%s", g.uri);

    while (!g.workers_finished) {
        period = mmrand(NULL, 1, 10);
        /*
        * open_cursor can return EBUSY if concurrent with a metadata operation, retry in that case.
        */
        while ((ret = import_session->open_cursor(import_session, IMPORT_URI, NULL, NULL, &cursor)) == EBUSY)
            __wt_yield();
        testutil_check(ret);

        for (int i = 0; i < 1000; i++) {
            testutil_check(cursor_insert(cursor, i, i));
        }

        copy_file(import_session, "import.wt");
        
        fprintf(stdout, "Performing import...\n");
        memset(buf, 0, sizeof(buf));
        testutil_check(__wt_snprintf(buf, sizeof(buf), "import=(enabled,repair=true)"));
        while ((ret = session->create(session, "file:import.wt", buf)) != 0)
            testutil_die(ret, "session.import");
        fprintf(stdout, "Finished import...\n");
        while (period > 0 && !g.workers_finished) {
            --period;
            __wt_sleep(1, 0);
        }
    }

    testutil_check(import_session->close(import_session, NULL));
    return (WT_THREAD_RET_VALUE);
}

/*! [cursor insert] */
static int
cursor_insert(WT_CURSOR *cursor, int key, int val)
{
    cursor->set_key(cursor, key);
    cursor->set_value(cursor, val);

    return (cursor->insert(cursor));
}
/*! [cursor insert] */

/*
 * copy_file --
 *     Copy a single file into the directories.
 */
static void
copy_file(WT_SESSION *session, const char *name)
{
    size_t len;
    char *to;

    len = strlen("../") + strlen(name) + 10;
    to = dmalloc(len);
    testutil_check(__wt_snprintf(to, len, "../%s", name));
    testutil_check(__wt_copy_and_sync(session, name, to));
    free(to);
}

// /*
//  * Drop can return EBUSY if concurrent with other operations.
//  */
// testutil_check(__wt_snprintf(buf, sizeof(buf), "remove_files=false"));
// while ((ret = session->drop(session, g.uri, buf)) != 0 && ret != EBUSY)
//     testutil_die(ret, "session.drop");

// if (ret != EBUSY) {

//     fprintf(stdout, "Performing import...\n");
//     memset(buf, 0, sizeof(buf));
//     testutil_check(__wt_snprintf(buf, sizeof(buf), "import=(enabled,repair=true)"));
//     while ((ret = session->create(session, g.uri, buf)) != 0)
//         testutil_die(ret, "session.import");
// }