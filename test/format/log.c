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

#define LOG_DIR "OPS.LOG"
#define LOG_INIT_CMD "rm -rf %s/" LOG_DIR " && mkdir %s/" LOG_DIR

int
oplog_config(const char *config)
{
    WT_DECL_RET;
    char *copy, *p;

    copy = dstrdup(config);
    for (;;) {
        if ((p = strstr(copy, "all")) != NULL) {
            g.log_all = true;
            memset(p, ' ', strlen("all"));
            continue;
        }
        if ((p = strstr(copy, "local")) != NULL) {
            g.log_local = true;
            memset(p, ' ', strlen("local"));
            continue;
        }
        break;
    }

    for (p = copy; *p != '\0'; ++p)
        if (*p != ',' && !__wt_isspace((u_char)*p)) {
            ret = EINVAL;
            break;
        }

    free(copy);
    return (ret);
}

void
oplog_init(void)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    size_t len;
    char *p;
    const char *config;

    if (!g.logging)
        return;

    /* Log to a separate database by default, optionally log to the primary. */
    if (g.log_local) {
        if (!g.c_logging)
            testutil_die(EINVAL,
              "operation logging to the primary database requires logging be configured for that "
              "database");

        conn = g.wts_conn;
        testutil_check(conn->reconfigure(conn, "debug_mode=(log_retain=10)"));
    } else {
        len = strlen(g.home) * 2 + strlen(LOG_INIT_CMD) + 10;
        p = dmalloc(len);
        testutil_check(__wt_snprintf(p, len, LOG_INIT_CMD, g.home, g.home));
        testutil_checkfmt(system(p), "%s", "logging directory creation failed");
        free(p);

        /* Configure log archival, and keep the last 20 log files. */
        len = strlen(g.home) * strlen(LOG_DIR) + 10;
        p = dmalloc(len);
        testutil_check(__wt_snprintf(p, len, "%s/%s", g.home, LOG_DIR));
        config = "create,log=(enabled,archive),debug_mode=(log_retain=10)";
        testutil_checkfmt(wiredtiger_open(p, NULL, config, &conn), "%s: %s", p, config);
        free(p);
    }

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    g.oplog_conn = conn;
    g.oplog_session = session;
}

void
oplog_teardown(void)
{
    WT_CONNECTION *conn;

    conn = g.oplog_conn;
    g.oplog_conn = NULL;

    if (!g.logging || g.log_local || conn == NULL)
        return;

    testutil_check(conn->close(conn, NULL));
}

void
oplog_ops_init(TINFO *tinfo)
{
    WT_SESSION *session;

    if (!g.logging)
        return;

    testutil_check(g.oplog_conn->open_session(g.oplog_conn, NULL, NULL, &session));
    tinfo->log = session;
}
