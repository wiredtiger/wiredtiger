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
 *
 * ex_access.c
 * 	demonstrates how to create and access a simple table, include insert data and load exist table's data.
 */
#include <test_util.h>

static const char *home = "WT_HOME";

/*
 * usage --
 *     wtperf usage print, no error.
 */
static void
usage(void)
{
    printf("ex_access [-i] [-l]\n");
    printf("\t-i insert data and scan data\n");
    printf("\t-l load exist data and scan data\n");
    printf("\n");
}

static void
access_example(int argc, char *argv[])
{
    /*! [access example connection] */
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_SESSION *session;
    const char *key, *value;
    int ch, ret;
    bool insertConfig = false;
    bool loadDataConfig = false;
    char cmd_buf[512];

    const char *cmdflags = "i:l:";
    /* Do a basic validation of options */
    while ((ch = __wt_getopt("ex_access", argc, argv, cmdflags)) != EOF) {
        switch (ch) {
        /* insert and scan data */
        case 'i':
            insertConfig = true;
            break;
        /* load and scan data */
        case 'l':
            loadDataConfig = true;
            break;
        case '?':
        default:
            usage();
            return;
        }
    }

    if (!insertConfig && !loadDataConfig) {
        usage();
        return;
    }

    /* prepare data */
    if (insertConfig) {
        (void)snprintf(cmd_buf, sizeof(cmd_buf), "rm -rf %s && mkdir %s", home, home);
        error_check(system(cmd_buf));

        /* Open a connection to the database, creating it if necessary. */
        error_check(wiredtiger_open(home, NULL, "create,statistics=(all)", &conn));

        /* Open a session handle for the database. */
        error_check(conn->open_session(conn, NULL, NULL, &session));
        /*! [access example connection] */

        /*! [access example table create] */
        error_check(session->create(session, "table:access", "key_format=S,value_format=S"));
        /*! [access example table create] */

        /*! [access example cursor open] */
        error_check(session->open_cursor(session, "table:access", NULL, NULL, &cursor));
        /*! [access example cursor open] */

        /*! [access example cursor insert] */
        cursor->set_key(cursor, "key1"); /* Insert a record. */
        cursor->set_value(cursor, "value1");
        error_check(cursor->insert(cursor));
        /*! [access example cursor insert] */

        /*! [access example cursor list] */
        error_check(cursor->reset(cursor)); /* Restart the scan. */
        while ((ret = cursor->next(cursor)) == 0) {
            error_check(cursor->get_key(cursor, &key));
            error_check(cursor->get_value(cursor, &value));

            printf("Got record: %s : %s\n", key, value);
        }
        scan_end_check(ret == WT_NOTFOUND); /* Check for end-of-table. */
        /*! [access example cursor list] */

        /*! [access example close] */
        error_check(conn->close(conn, NULL)); /* Close all handles. */
                                              /*! [access example close] */
    }

    /* load exist data, for example: when process restart, wo should warmup and load exist data*/
    if (loadDataConfig) {
        /* Open a connection to the database, creating it if necessary. */
        error_check(wiredtiger_open(home, NULL, "statistics=(all)", &conn));

        /* Open a session handle for the database. */
        error_check(conn->open_session(conn, NULL, NULL, &session));
        /*! [access example connection] */

        /*! [access example cursor open] */
        error_check(session->open_cursor(session, "table:access", NULL, NULL, &cursor));
        /*! [access example cursor open] */

        error_check(cursor->reset(cursor)); /* Restart the scan. */
        while ((ret = cursor->next(cursor)) == 0) {
            error_check(cursor->get_key(cursor, &key));
            error_check(cursor->get_value(cursor, &value));

            printf("Load record: %s : %s\n", key, value);
        }
        scan_end_check(ret == WT_NOTFOUND); /* Check for end-of-table. */
        /*! [access example cursor list] */

        cursor->set_key(cursor, "key1");
        error_check(cursor->search(cursor));
        error_check(cursor->get_value(cursor, &value));
        printf("Load search record: %s : %s\n", "key1", value);

        /*! [access example close] */
        error_check(conn->close(conn, NULL)); /* Close all handles. */
                                              /*! [access example close] */
    }
}

/*
run step:
  step 1(prepare data):                ./ex_access -i 1
  step 2(warmup and load exist data):  ./ex_access -l 1
*/
int
main(int argc, char *argv[])
{
    access_example(argc, argv);

    return (EXIT_SUCCESS);
}
