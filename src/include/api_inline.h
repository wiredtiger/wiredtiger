/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *  All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

/*
 * __wt_api_track_cursor_start --
 *     Start tracking a cursor API entry point for statistics.
 */
static WT_INLINE void
__wt_api_track_cursor_start(WT_SESSION_IMPL *session)
{
    /*
     * Track cursor API calls, so we can know how many are in the library at a point in time. These
     * need to be balanced. If the api call counter is zero, it means these have been used in the
     * wrong order compared to the other enter/end macros.
     */
    WT_ASSERT(session, session->id == 0 || session->api_call_counter != 0);
    if (session->api_call_counter == 1) {
        if (F_ISSET(session, WT_SESSION_INTERNAL))
            (void)__wt_atomic_add64(&S2C(session)->api_count_cursor_internal_in, 1);
        else
            (void)__wt_atomic_add64(&S2C(session)->api_count_cursor_in, 1);
    }
}

/*
 * __wt_api_track_cursor_end --
 *     Finish tracking a cursor API entry point for statistics.
 */
static WT_INLINE void
__wt_api_track_cursor_end(WT_SESSION_IMPL *session)
{
    WT_CONNECTION_IMPL *conn;

    conn = S2C(session);

    WT_ASSERT(session, session->id == 0 || session->api_call_counter != 0);
    if ((session)->api_call_counter == 1) {
        if (F_ISSET(session, WT_SESSION_INTERNAL)) {
            (void)__wt_atomic_add64(&conn->api_count_cursor_internal_out, 1);
            WT_API_COUNTER_CHECK(session, api_count_cursor_internal);
        } else {
            (void)__wt_atomic_add64(&conn->api_count_cursor_out, 1);
            WT_API_COUNTER_CHECK(session, api_count_cursor);
        }
    }
}
