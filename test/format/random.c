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
 * position_kv --
 *     Position a cursor at a random fraction of the table with random options and check the
 *     outcome. Returns false if the transaction was rolled back.
 */
static bool
position_kv(WT_CURSOR *cursor)
{
    WT_DECL_RET;
    WT_ITEM key, value;
    WT_POSITION position;
    double pos;
    int exact;
    bool key_only;

    memset(&position, 0, sizeof(position));
    position.pos = mmrand(&g.extra_rnd, 0, WT_THOUSAND) / (double)WT_THOUSAND;
    position.flags = mmrand(&g.extra_rnd, 0, 3) << 4;
    if (mmrand(&g.extra_rnd, 1, 3) == 1)
        position.flags |= WT_POSITION_PREV;
    if (mmrand(&g.extra_rnd, 1, 10) == 1)
        position.flags |= WT_POSITION_CACHE_ONLY;
    key_only = mmrand(&g.extra_rnd, 1, 4) == 1;
    if (key_only)
        position.flags |= WT_POSITION_KEY_ONLY;

    switch (ret = cursor->set_position(cursor, &position)) {
    case 0:
        break;
    case WT_NOTFOUND:
    case WT_CACHE_FULL:
    case WT_PREPARE_CONFLICT:
        return (true);
    case WT_ROLLBACK:
        return (false);
    default:
        testutil_check(ret);
    }

    if (key_only) {
        /* The boundary key is usable as a search key. */
        testutil_assert(position.pages_skipped == 0);
        testutil_check(cursor->get_key(cursor, &key));
        switch (ret = cursor->search_near(cursor, &exact)) {
        case 0:
            break;
        case WT_NOTFOUND:
        case WT_CACHE_FULL:
        case WT_PREPARE_CONFLICT:
            return (true);
        case WT_ROLLBACK:
            return (false);
        default:
            testutil_check(ret);
        }
    }

    testutil_check(cursor->get_key(cursor, &key));
    testutil_check(cursor->get_value(cursor, &value));
    testutil_check(cursor->get_position(cursor, &pos));
    testutil_assert(pos >= 0. && pos <= 1.);
    return (true);
}

/*
 * random_kv --
 *     Do random cursor operations.
 */
WT_THREAD_RET
random_kv(void *arg)
{
    SAP sap;
    TABLE *table;
    WT_CONNECTION *conn;
    WT_CURSOR *cursor;
    WT_DECL_RET;
    WT_ITEM key, value;
    WT_SESSION *session;
    uint32_t i;
    u_int period;
    const char *config;
    bool rollback, simple;

    (void)(arg); /* Unused parameter */

    conn = g.wts_conn;

    /* Random cursor ops are only supported on row-store, make sure there's a row-store table. */
    if (ntables == 0 && tables[0]->type != ROW)
        return (WT_THREAD_RET_VALUE);
    else {
        for (i = 1; i < ntables; ++i)
            if (tables[i]->type == ROW)
                break;
        if (i == ntables)
            return (WT_THREAD_RET_VALUE);
    }

    /* Open a session. */
    memset(&sap, 0, sizeof(sap));
    wt_wrap_open_session(conn, &sap, NULL, session_prefetch_cfg(), &session);

    for (simple = false;;) {
        /* Alternate between simple random cursors and sample-size random cursors. */
        config = simple ? "next_random=true" : "next_random=true,next_random_sample_size=37";
        simple = !simple;

        /* Select a table. */
        table = table_select_type(ROW, false);

        /* Read inside a snapshot transaction so the reads observe a single consistent state. */
        wt_wrap_begin_transaction(session, "isolation=snapshot");
        wt_wrap_open_cursor(session, table->uri, config, &cursor);

        /* This is just a smoke-test, get some key/value pairs. */
        rollback = false;
        for (i = mmrand(&g.extra_rnd, 0, WT_THOUSAND); i > 0 && !rollback && !g.workers_finished;
          --i) {
            switch (ret = cursor->next(cursor)) {
            case 0:
                break;
            case WT_NOTFOUND:
            case WT_CACHE_FULL:
            case WT_PREPARE_CONFLICT:
                continue;
            case WT_ROLLBACK:
                /* The snapshot can no longer be used; abandon this round and start a new one. */
                rollback = true;
                continue;
            default:
                testutil_check(ret);
            }
            testutil_check(cursor->get_key(cursor, &key));
            testutil_check(cursor->get_value(cursor, &value));
        }

        testutil_check(cursor->close(cursor));

        /* Position a plain cursor at random fractions of the same table. */
        wt_wrap_open_cursor(session, table->uri, NULL, &cursor);
        for (i = mmrand(&g.extra_rnd, 0, 100); i > 0 && !rollback && !g.workers_finished; --i)
            rollback = !position_kv(cursor);
        testutil_check(cursor->close(cursor));

        /*
         * End the transaction; required even after WT_ROLLBACK, which only marks the error and
         * leaves it running.
         */
        testutil_check(session->rollback_transaction(session, NULL));

        /* Sleep for some number of seconds. */
        period = mmrand(&g.extra_rnd, 1, 10);

        /* Sleep for short periods so we don't make the run wait. */
        while (period > 0 && !g.workers_finished) {
            --period;
            __wt_sleep(1, 0);
        }
        if (g.workers_finished)
            break;
    }

    wt_wrap_close_session(session);

    return (WT_THREAD_RET_VALUE);
}
