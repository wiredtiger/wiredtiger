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
 * bulk_begin_transaction --
 *     Begin a bulk-load transaction.
 */
static void
bulk_begin_transaction(WT_SESSION *session)
{
    uint64_t ts;
    char buf[64];

    /* Writes require snapshot isolation. */
    wiredtiger_begin_transaction(session, NULL);
    ts = __wt_atomic_addv64(&g.timestamp, 1);
    testutil_check(__wt_snprintf(buf, sizeof(buf), "read_timestamp=%" PRIx64, ts));
    testutil_check(session->timestamp_transaction(session, buf));
}

/*
 * bulk_commit_transaction --
 *     Commit a bulk-load transaction.
 */
static void
bulk_commit_transaction(WT_SESSION *session)
{
    uint64_t ts;
    char buf[64];

    ts = __wt_atomic_addv64(&g.timestamp, 1);
    testutil_check(__wt_snprintf(buf, sizeof(buf), "commit_timestamp=%" PRIx64, ts));
    testutil_check(session->commit_transaction(session, buf));

    /* Update the oldest timestamp, otherwise updates are pinned in memory. */
    timestamp_once(false, false);
}

/*
 * bulk_rollback_transaction --
 *     Rollback a bulk-load transaction.
 */
static void
bulk_rollback_transaction(WT_SESSION *session)
{
    testutil_check(session->rollback_transaction(session, NULL));
}

void
wts_load(void)
{
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_ITEM key, value;
    WT_SESSION *session;
    uint32_t committed_keyno, keyno, v;
    bool is_bulk;

    conn = g.wts_conn;

    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    trace_msg("%s", "=============== bulk load start");

    /*
     * No bulk load with custom collators, the order of insertion will not match the collation
     * order.
     */
    is_bulk = true;
    if (g.c_reverse)
        is_bulk = false;

    /*
     * open_cursor can return EBUSY if concurrent with a metadata operation, retry in that case.
     */
    while ((ret = session->open_cursor(
              session, g.uri, NULL, is_bulk ? "bulk,append" : NULL, &cursor)) == EBUSY)
        __wt_yield();
    testutil_check(ret);

    /* Set up the key/value buffers. */
    key_gen_init(&key);
    val_gen_init(&value);

    if (g.c_txn_timestamps)
        bulk_begin_transaction(session);

    for (committed_keyno = keyno = 0; ++keyno <= g.c_rows;) {
        val_gen(NULL, &value, keyno);

        switch (g.type) {
        case FIX:
            if (!is_bulk)
                cursor->set_key(cursor, keyno);
            cursor->set_value(cursor, *(uint8_t *)value.data);
            if (g.trace_all)
                trace_msg("bulk %" PRIu32 " {0x%02" PRIx8 "}", keyno, ((uint8_t *)value.data)[0]);
            break;
        case VAR:
            if (!is_bulk)
                cursor->set_key(cursor, keyno);
            cursor->set_value(cursor, &value);
            if (g.trace_all)
                trace_msg("bulk %" PRIu32 " {%.*s}", keyno, (int)value.size, (char *)value.data);
            break;
        case ROW:
            key_gen(&key, keyno);
            cursor->set_key(cursor, &key);
            cursor->set_value(cursor, &value);
            if (g.trace_all)
                trace_msg("bulk %" PRIu32 " {%.*s}, {%.*s}", keyno, (int)key.size, (char *)key.data,
                  (int)value.size, (char *)value.data);
            break;
        }

        /*
         * We don't want to size the cache to ensure the initial data set can load in the in-memory
         * case, guaranteeing the load succeeds probably means future updates are also guaranteed to
         * succeed, which isn't what we want. If we run out of space in the initial load, reset the
         * row counter and continue.
         */
        if ((ret = cursor->insert(cursor)) != 0) {
            testutil_assert(ret == WT_CACHE_FULL || ret == WT_ROLLBACK);

            if (g.c_txn_timestamps) {
                bulk_rollback_transaction(session);
                bulk_begin_transaction(session);
            }

            /*
             * Decrease inserts since they won't be successful if we're hitting cache limits, and
             * increase the delete percentage to get some extra space once the run starts. We can't
             * simply modify the values because they have to equal 100 when the database is reopened
             * (we are going to rewrite the CONFIG file, too).
             */
            if (g.c_insert_pct > 5) {
                g.c_delete_pct += g.c_insert_pct - 5;
                g.c_insert_pct = 5;
            }
            v = g.c_write_pct / 2;
            g.c_delete_pct += v;
            g.c_write_pct -= v;

            break;
        }

        /*
         * When first starting up, restart the enclosing transaction every 10 operations so we never
         * end up with an empty object. After 5K records, restart the transaction every 5K records
         * so we don't overflow the cache.
         */
        if ((keyno < 5000 && keyno % 10 == 0) || keyno % 5000 == 0) {
            /* Report on progress. */
            track("bulk load", keyno, NULL);

            if (g.c_txn_timestamps) {
                bulk_commit_transaction(session);
                committed_keyno = keyno;
                bulk_begin_transaction(session);
            }
        }
    }

    if (g.c_txn_timestamps)
        bulk_commit_transaction(session);

    /*
     * Ideally, the insert loop runs until the number of rows plus one, in which case row counts are
     * correct. If the loop exited early, reset the counters and rewrite the CONFIG file (so reopens
     * aren't surprised).
     */
    if (keyno != g.c_rows + 1) {
        g.c_rows = g.c_txn_timestamps ? committed_keyno : (keyno - 1);
        testutil_assert(g.c_rows > 0);
        g.rows = g.c_rows;

        config_print(false);
    }

    testutil_check(cursor->close(cursor));

    trace_msg("%s", "=============== bulk load stop");

    testutil_check(session->close(session, NULL));

    key_gen_teardown(&key);
    val_gen_teardown(&value);
}
