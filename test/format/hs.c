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
 * hs_cursor --
 *     Do history store cursor operations.
 */
WT_THREAD_RET
hs_cursor(void *arg)
{
#if WIREDTIGER_VERSION_MAJOR < 10
    WT_UNUSED(arg);
#else
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_ITEM hs_key, hs_value, key;
    WT_SESSION *session;
    wt_timestamp_t hs_durable_timestamp, hs_start_ts, hs_stop_durable_ts;
    uint64_t hs_counter, hs_upd_type;
    uint32_t hs_btree_id, i;
    u_int period;
    int exact;
    bool next, restart;

    (void)(arg); /* Unused parameter */

    conn = g.wts_conn;

    /*
     * Trigger the internal WiredTiger cursor order checking on the history-store file. Open a
     * cursor on the history-store file, retrieve some records, close cursor, repeat.
     *
     * Open a session.
     */
    testutil_check(conn->open_session(conn, NULL, NULL, &session));

    memset(&hs_key, 0, sizeof(hs_key));
    memset(&hs_value, 0, sizeof(hs_value));
    memset(&key, 0, sizeof(key));
    hs_start_ts = 0; /* [-Wconditional-uninitialized] */
    hs_counter = 0;  /* [-Wconditional-uninitialized] */
    hs_btree_id = 0; /* [-Wconditional-uninitialized] */
    for (restart = true;;) {
        testutil_check(__wt_curhs_open((WT_SESSION_IMPL *)session, NULL, &cursor));
        F_SET(cursor, WT_CURSTD_HS_READ_COMMITTED);

        /* Search to the last-known location. */
        if (!restart) {
            cursor->set_key(cursor, 4, hs_btree_id, &key, hs_start_ts, hs_counter);

            /*
             * Limit expected errors because this is a diagnostic check (the WiredTiger API allows
             * prepare-conflict, but that would be unexpected from the history store file).
             */
            ret = cursor->search_near(cursor, &exact);
            testutil_assert(ret == 0 || ret == WT_NOTFOUND || ret == WT_ROLLBACK);
        }

        /*
         * Get some more key/value pairs. Always retrieve at least one key, that ensures we have a
         * valid key when we copy it to start the next run.
         */
        next = mmrand(NULL, 0, 1) == 1;
        for (i = mmrand(NULL, 1, 1000); i > 0; --i) {
            if ((ret = (next ? cursor->next(cursor) : cursor->prev(cursor))) == 0) {
                testutil_check(
                  cursor->get_key(cursor, &hs_btree_id, &hs_key, &hs_start_ts, &hs_counter));
                testutil_check(cursor->get_value(
                  cursor, &hs_stop_durable_ts, &hs_durable_timestamp, &hs_upd_type, &hs_value));
                continue;
            }
            testutil_assert(ret == WT_NOTFOUND || ret == WT_ROLLBACK || ret == WT_CACHE_FULL);
            break;
        }

        /*
         * If we didn't hit the end of the store, save the current key to continue in the next run.
         * Otherwise, reset so we'll start over.
         */
        if (ret == 0) {
            testutil_check(
              __wt_buf_set((WT_SESSION_IMPL *)session, &key, hs_key.data, hs_key.size));
            restart = false;
        } else
            restart = true;

        testutil_check(cursor->close(cursor));

        /* Sleep for some number of seconds, in short intervals so we don't make the run wait. */
        for (period = mmrand(NULL, 1, 10); period > 0 && !g.workers_finished; --period)
            __wt_sleep(1, 0);
        if (g.workers_finished)
            break;
    }

    __wt_buf_free((WT_SESSION_IMPL *)session, &key);
    testutil_check(session->close(session, NULL));
#endif

    return (WT_THREAD_RET_VALUE);
}
