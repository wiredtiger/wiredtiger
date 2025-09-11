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
 * wts_prepare_discover --
 *     Discover and process prepared transactions.
 */
void
wts_prepare_discover(TABLE *table, void *arg)
{
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_SESSION *session;
    uint64_t prepared_id, commit_ts, durable_ts;
    uint32_t claim_count, discover_count, rand_val;
    char buf[128];
    bool claim_all, should_commit;
    SAP sap;

    (void)arg; /* unused argument */
    testutil_assert(table != NULL);

    conn = g.wts_conn;
    memset(&sap, 0, sizeof(sap));
    wt_wrap_open_session(conn, &sap, table->track_prefix, NULL, &session);

    /* Open the prepare discover cursor */
    ret = session->open_cursor(session, "prepared_discover:", NULL, NULL, &cursor);
    if (ret == WT_NOTFOUND) {
        /* No prepared transactions found - this is normal */
        testutil_check(session->close(session, NULL));
        return;
    }
    testutil_check(ret);

    /*
     * If the cursor was opened successfully, there should be at least one prepared transaction to
     * discover.
     */
    trace_msg(session, "Starting prepare discover operation %s", "");

    /*
     * Iterate through all prepared transactions This is similar to how test_prepare_discover03.py
     * walks through the cursor
     */
    discover_count = 0;
    claim_count = 0;
    claim_all = mmrand(&g.extra_rnd, 0, 9) < 8; /* 80% chance to claim all */

    while ((ret = cursor->next(cursor)) == 0) {
        discover_count++;
        testutil_check(cursor->get_key(cursor, &prepared_id));

        trace_msg(session, "Discovered prepared transaction with ID: %" PRIu64, prepared_id);

        /*
         * Decide whether to claim this transaction. If claim_all is true, claim all transactions.
         * Otherwise, randomly decide for each transaction.
         */
        if (claim_all || mmrand(&g.extra_rnd, 0, 9) < 7) { /* 70% chance to claim */
            /* Claim the prepared transaction */
            testutil_snprintf(buf, sizeof(buf), "claim_prepared=%" PRIx64, prepared_id);
            testutil_check(session->begin_transaction(session, buf));

            /* Randomly decide whether to commit or roll back */
            rand_val = mmrand(&g.extra_rnd, 0, 9);
            should_commit = (rand_val < 8); /* 80% chance to commit */

            if (should_commit) {
                /*
                 * Commit with a timestamp greater than the prepare timestamp We use the current
                 * timestamp + 10 to ensure it's newer
                 */
                commit_ts = __wt_atomic_addv64(&g.timestamp, 10);
                durable_ts = commit_ts + 10;

                testutil_snprintf(buf, sizeof(buf),
                  "commit_timestamp=%" PRIu64 ",durable_timestamp=%" PRIu64, commit_ts, durable_ts);

                testutil_check(session->commit_transaction(session, buf));
                trace_msg(session,
                  "Claimed and committed prepared transaction %" PRIu64 " with ts=%" PRIu64
                  "/%" PRIu64,
                  prepared_id, commit_ts, durable_ts);
            } else {
                /* Roll back the transaction */
                testutil_check(session->rollback_transaction(session, NULL));
                trace_msg(
                  session, "Claimed and rolled back prepared transaction %" PRIu64, prepared_id);
            }

            claim_count++;
        } else {
            trace_msg(session, "Skipped claiming prepared transaction %" PRIu64, prepared_id);
        }
    }

    /* WT_NOTFOUND is expected when we reach the end of the cursor */
    testutil_assert(ret == WT_NOTFOUND);

    /* Report what we found and did */
    if (discover_count > 0) {
        trace_msg(session,
          "Prepare discover: found %" PRIu32 " prepared transactions, claimed %" PRIu32,
          discover_count, claim_count);
    }

    /*
     * If we're not claiming all transactions, expect an error when closing the cursor.
     */
    if (discover_count > 0 && claim_count < discover_count) {
        ret = cursor->close(cursor);
        testutil_assert(ret != 0);
        trace_msg(session, "Expected error when closing cursor with unclaimed transactions: %s",
          wiredtiger_strerror(ret));

        /* We still need to clean up the session */
        testutil_check(session->close(session, NULL));

        /*
         * Reopen a connection and clean up any remaining prepared transactions This is to avoid
         * leaving the system in a state with unclaimed transactions
         */
        wts_open(g.home, &conn, false);
        memset(&sap, 0, sizeof(sap));
        wt_wrap_open_session(conn, &sap, table->track_prefix, NULL, &session);
        testutil_check(session->open_cursor(session, "prepared_discover:", NULL, NULL, &cursor));

        while ((ret = cursor->next(cursor)) == 0) {
            testutil_check(cursor->get_key(cursor, &prepared_id));

            /* Claim and roll back all remaining prepared transactions */
            testutil_snprintf(buf, sizeof(buf), "claim_prepared_id=%" PRIx64, prepared_id);
            testutil_check(session->begin_transaction(session, buf));
            testutil_check(session->rollback_transaction(session, NULL));

            trace_msg(session, "Cleanup: claimed and rolled back prepared transaction %" PRIu64,
              prepared_id);
        }

        testutil_assert(ret == WT_NOTFOUND);
        testutil_check(cursor->close(cursor));
        testutil_check(session->close(session, NULL));
        wts_close(&conn);
    } else {
        /* If we claimed all transactions, we should be able to close the cursor cleanly */
        testutil_check(cursor->close(cursor));
        testutil_check(session->close(session, NULL));
    }
}
