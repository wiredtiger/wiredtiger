/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <cstdio>

#include "wt_internal.h"
#include "../wrappers/mock_session.h"

struct layered_orphan_remove_fixture {
    layered_orphan_remove_fixture() : mock(mock_session::build_test_mock_session())
    {
        session = mock->get_wt_session_impl();
        session->event_handler->handle_error = [](WT_EVENT_HANDLER *, WT_SESSION *, int,
                                                 const char *message) {
            (void)std::fprintf(stderr, "%s\n", message);
            return (0);
        };
        REQUIRE(__wt_calloc(session, 1, sizeof(WT_TXN), &session->txn) == 0);
        WT_CONNECTION_IMPL *conn = S2C(session);
        /* Let the tombstone's transaction ID pass the global visibility check. */
        conn->txn_global.oldest_id = 100;
        /* Enable the timestamp retention boundary used by the global visibility check. */
        conn->txn_global.has_pinned_timestamp = true;
        /* Older timestamps qualify for the exception; newer timestamps exercise the assertion. */
        conn->txn_global.pinned_timestamp = 10;

        /* Simulate a stable-table search with no matching key. */
        stable_cursor.compare = 1;
        REQUIRE(__wt_upd_alloc_tombstone(session, &tombstone, nullptr) == 0);
        tombstone->txnid = 50;
    }

    ~layered_orphan_remove_fixture()
    {
        __wt_free(session, tombstone);
        __wt_free(session, session->txn);
    }

    std::shared_ptr<mock_session> mock;
    WT_SESSION_IMPL *session;
    WT_CURSOR_BTREE stable_cursor = {};
    WT_UPDATE *tombstone = nullptr;
};

TEST_CASE_METHOD(layered_orphan_remove_fixture,
  "Ingest drain does not report a globally visible tombstone without a stable value",
  "[layered][layered_orphan_remove]")
{
    /*
     * This delete is old enough for the cleanup exception: its previous value may have been
     * removed, so a missing value is expected and is not an orphan.
     */
    tombstone->upd_start_ts = tombstone->upd_durable_ts = 5;
    REQUIRE(__wt_txn_upd_visible_all(session, tombstone));

    REQUIRE_FALSE(__ut_layered_check_stable_btree_state(session, &stable_cursor, tombstone));
}

TEST_CASE_METHOD(layered_orphan_remove_fixture,
  "Ingest drain reports a timestamped orphan tombstone", "[layered][layered_orphan_remove]")
{
    /*
     * This delete is too new for the cleanup exception, so nothing explains the missing value: the
     * key may never have existed. Blind remove allows that, so the drain reports it as an orphan
     * instead of aborting.
     */
    tombstone->upd_start_ts = tombstone->upd_durable_ts = 20;
    REQUIRE_FALSE(__wt_txn_upd_visible_all(session, tombstone));

    REQUIRE(__ut_layered_check_stable_btree_state(session, &stable_cursor, tombstone));
}
