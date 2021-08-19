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
 * range_stat --
 *     Periodically do a range-statistics query.
 */
WT_THREAD_RET
range_stat(void *arg)
{
    WT_CONNECTION *conn;
    WT_CURSOR *start, *stop;
    WT_DECL_RET;
    WT_ITEM kstart, kstop;
    WT_SESSION *session;
    uint64_t byte_count, keyno, row_count;
    u_int period;

    (void)(arg);

    /* Open a session and a pair of stop. */
    conn = g.wts_conn;
    testutil_check(conn->open_session(conn, NULL, NULL, &session));
    while ((ret = session->open_cursor(session, g.uri, NULL, NULL, &start)) == EBUSY)
        __wt_yield();
    testutil_check(ret);
    while ((ret = session->open_cursor(session, g.uri, NULL, NULL, &stop)) == EBUSY)
        __wt_yield();
    testutil_check(ret);

    /* Set up the key buffers. */
    key_gen_init(&kstart);
    key_gen_init(&kstop);

    /*
     * Make a call at somewhere under 15 seconds (so we get at least one done), and then at regular
     * intervals.
     */
    for (period = mmrand(NULL, 1, 15);; period = 17) {
        /* Sleep for short periods so we don't make the run wait. */
        while (period > 0 && !g.workers_finished) {
            --period;
            __wt_sleep(1, 0);
        }
        if (g.workers_finished)
            break;

        /* 10% of the time stat the object, otherwise, stat a cursor range. */
        if (mmrand(NULL, 1, 10) == 1) {
            testutil_check(
              session->range_stat(session, g.uri, NULL, NULL, NULL, &row_count, &byte_count));
        } else {
            keyno = mmrand(NULL, 1, (u_int)(g.rows - g.rows / 10));
            switch (g.type) {
            case FIX:
            case VAR:
                start->set_key(start, keyno);
                break;
            case ROW:
                key_gen(&kstart, keyno);
                start->set_key(start, &kstart);
                break;
            }
            keyno = mmrand(NULL, (u_int)(keyno + 1), (u_int)g.rows);
            switch (g.type) {
            case FIX:
            case VAR:
                stop->set_key(stop, keyno);
                break;
            case ROW:
                key_gen(&kstop, keyno);
                stop->set_key(stop, &kstop);
                break;
            }
            ret = session->range_stat(session, NULL, start, stop, NULL, &row_count, &byte_count);
            testutil_assert(ret == 0 || ret == WT_NOTFOUND);
        }
    }

    testutil_check(session->close(session, NULL));

    return (WT_THREAD_RET_VALUE);
}
