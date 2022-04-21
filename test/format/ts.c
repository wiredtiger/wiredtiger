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

/*
 * query_ts --
 *     Query a timestamp.
 */
void
query_ts(const char *query, uint64_t *tsp)
{
    WT_CONNECTION *conn;
    char tsbuf[WT_TS_HEX_STRING_SIZE];

    conn = g.wts_conn;

    testutil_check(conn->query_timestamp(conn, tsbuf, query));
    *tsp = testutil_timestamp_parse(tsbuf);
}

/*
 * timestamp_init --
 *     Set the timestamp on open to the database's recovery timestamp.
 */
void
timestamp_init(void)
{
    query_ts("get=recovery", &g.timestamp);
}

/*
 * timestamp_once --
 *     Update the timestamp once.
 */
void
timestamp_once(WT_SESSION *session, bool allow_lag, bool final)
{
    static const char *oldest_timestamp_str = "oldest_timestamp=";
    static const char *stable_timestamp_str = "stable_timestamp=";
    WT_CONNECTION *conn;
    WT_DECL_RET;
    uint64_t all_durable;
    char buf[WT_TS_HEX_STRING_SIZE * 2 + 64];

    conn = g.wts_conn;

    /* Lock out transaction timestamp operations. */
    lock_writelock(session, &g.ts_lock);

    if (final)
        g.oldest_timestamp = g.stable_timestamp = ++g.timestamp;
    else {
        if ((ret = conn->query_timestamp(conn, buf, "get=all_durable")) == 0)
            all_durable = testutil_timestamp_parse(buf);
        else {
            testutil_assert(ret == WT_NOTFOUND);
            lock_writeunlock(session, &g.ts_lock);
            return;
        }

        /*
         * If a lag is permitted, move the oldest timestamp half the way to the current
         * "all_durable" timestamp. Move the stable timestamp to "all_durable".
         */
        if (allow_lag)
            g.oldest_timestamp = (all_durable + g.oldest_timestamp) / 2;
        else
            g.oldest_timestamp = all_durable;
        g.stable_timestamp = all_durable;
    }

    lock_writeunlock(session, &g.ts_lock);

    testutil_check(__wt_snprintf(buf, sizeof(buf), "%s%" PRIx64 ",%s%" PRIx64, oldest_timestamp_str,
      g.oldest_timestamp, stable_timestamp_str, g.stable_timestamp));

    lock_writelock(session, &g.prepare_commit_lock);
    testutil_check(conn->set_timestamp(conn, buf));
    lock_writeunlock(session, &g.prepare_commit_lock);

    if (g.trace_timestamp)
        trace_msg(session, "setts oldest=%" PRIu64 ", stable=%" PRIu64, g.oldest_timestamp,
          g.stable_timestamp);
}

/*
 * timestamp --
 *     Periodically update the oldest timestamp.
 */
WT_THREAD_RET
timestamp(void *arg)
{
    SAP sap;
    WT_CONNECTION *conn;
    WT_SESSION *session;

    (void)arg; /* Unused argument */

    conn = g.wts_conn;

    /* Locks need session */
    memset(&sap, 0, sizeof(sap));
    wiredtiger_open_session(conn, &sap, NULL, &session);

    /* Update the oldest and stable timestamps at least once every 15 seconds. */
    while (!g.workers_finished) {
        random_sleep(&g.rnd, 15);

        timestamp_once(session, true, false);
    }

    wiredtiger_close_session(session);
    return (WT_THREAD_RET_VALUE);
}

/*
 * timestamp_teardown --
 *     Wrap up timestamp operations.
 */
void
timestamp_teardown(WT_SESSION *session)
{
    /*
     * Do a final bump of the oldest and stable timestamps, otherwise recent operations can prevent
     * verify from running.
     */
    timestamp_once(session, false, true);
}

/*
 * set_oldest_timestamp --
 *     Query the oldest timestamp from wiredtiger and set it as our global oldest timestamp. This
 *     should only be called on runs for pre existing databases.
 */
void
set_oldest_timestamp(void)
{
    static const char *oldest_timestamp_str = "oldest_timestamp=";

    WT_CONNECTION *conn;
    WT_DECL_RET;
    uint64_t oldest_ts;
    char buf[WT_TS_HEX_STRING_SIZE * 2 + 64], tsbuf[WT_TS_HEX_STRING_SIZE];

    conn = g.wts_conn;

    if ((ret = conn->query_timestamp(conn, tsbuf, "get=oldest_timestamp")) == 0) {
        oldest_ts = testutil_timestamp_parse(tsbuf);
        g.timestamp = oldest_ts;
        testutil_check(
          __wt_snprintf(buf, sizeof(buf), "%s%" PRIx64, oldest_timestamp_str, g.oldest_timestamp));
    } else if (ret != WT_NOTFOUND)
        /*
         * Its possible there may not be an oldest timestamp as such we could get not found. This
         * should be okay assuming timestamps are not configured if they are, it's still okay as we
         * could have configured timestamps after not running with timestamps. As such only error if
         * we get a non not found error. If we were supposed to fail with not found we'll see an
         * error later on anyway.
         */
        testutil_die(ret, "unable to query oldest timestamp");
}

/*
 * maximum_read_ts --
 *     Return the largest safe read timestamp.
 */
uint64_t
maximum_read_ts(void)
{
    TINFO **tlp;
    uint64_t ts;

    /*
     *  We can't use a read timestamp that's ahead of a commit timestamp. Find the maximum safe read
     * timestamp.
     */
    for (ts = g.timestamp, tlp = tinfo_list; *tlp != NULL; ++tlp)
        ts = WT_MIN(ts, (*tlp)->commit_ts);
    if (ts != 0)
        --ts;
    return (ts);
}
