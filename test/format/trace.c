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

#define TRACE_DIR "OPS.TRACE"
#define TRACE_INIT_CMD "rm -rf %s/" TRACE_DIR " && mkdir %s/" TRACE_DIR

/*
 * trace_init --
 *     Initialize operation tracing.
 */
void
trace_init(void)
{
    WT_CONNECTION *conn;
    WT_SESSION *session;
    uint32_t retain;
    char config[100], tracedir[MAX_FORMAT_PATH * 2];

    if (!g.trace)
        return;

    /* Retain a minimum of 10 log files. */
    retain = WT_MAX(GV(TRACE_LOG_RETAIN), 10);

    /* Write traces to a separate database by default, optionally write traces to the primary. */
    if (GV(TRACE_LOCAL)) {
        if (!GV(LOGGING))
            testutil_die(EINVAL,
              "operation logging to the primary database requires logging be configured for that "
              "database");

        conn = g.wts_conn;

        /* Keep the last N log files. */
        testutil_check(
          __wt_snprintf(config, sizeof(config), "debug_mode=(log_retention=%" PRIu32 ")", retain));
        testutil_check(conn->reconfigure(conn, config));
    } else {
        /* Create the trace directory. */
        testutil_check(__wt_snprintf(tracedir, sizeof(tracedir), TRACE_INIT_CMD, g.home, g.home));
        testutil_checkfmt(system(tracedir), "%s", "logging directory creation failed");

        /* Configure logging with archival, and keep the last N log files. */
        testutil_check(__wt_snprintf(config, sizeof(config),
          "create,log=(enabled,archive),debug_mode=(log_retention=%" PRIu32 ")", retain));
        testutil_check(__wt_snprintf(tracedir, sizeof(tracedir), "%s/%s", g.home, TRACE_DIR));
        testutil_checkfmt(
          wiredtiger_open(tracedir, NULL, config, &conn), "%s: %s", tracedir, config);
    }

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    g.trace_conn = conn;
    g.trace_session = session;
}

/*
 * trace_teardown --
 *     Close operation tracing.
 */
void
trace_teardown(void)
{
    WT_CONNECTION *conn;

    conn = g.trace_conn;
    g.trace_conn = NULL;

    if (conn != NULL)
        testutil_check(conn->close(conn, NULL));
}

/*
 * trace_ops_init --
 *     Per thread operation tracing setup.
 */
void
trace_ops_init(TINFO *tinfo)
{
    WT_SESSION *session;

    if (!g.trace)
        return;

    testutil_check(g.trace_conn->open_session(g.trace_conn, NULL, NULL, &session));
    tinfo->trace = session;
}
