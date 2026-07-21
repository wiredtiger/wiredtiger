/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"

#include "../utils.h"
#include "../wrappers/connection_wrapper.h"
#include "../../utility/test_util.h"

TEST_CASE(
  "txn oldest scan: tracks oldest session for active running transactions", "[txn][oldest_scan]")
{
    const std::string home = "WT_TEST.txn_oldest_scan";

    testutil_system("rm -rf %s && mkdir -p %s", home.c_str(), home.c_str());

    connection_wrapper conn(home, "create");
    WT_SESSION_IMPL *session1 = conn.create_session();
    WT_SESSION *wt_session1 = &session1->iface;

    REQUIRE(wt_session1->create(
              wt_session1, "table:test_oldest_scan", "key_format=S,value_format=S") == 0);

    WT_CURSOR *cursor1 = NULL;
    REQUIRE(
      wt_session1->open_cursor(wt_session1, "table:test_oldest_scan", NULL, NULL, &cursor1) == 0);
    REQUIRE(wt_session1->begin_transaction(wt_session1, "isolation=snapshot") == 0);
    cursor1->set_key(cursor1, "key0");
    cursor1->set_value(cursor1, "val1");
    REQUIRE(cursor1->insert(cursor1) == 0); /* Allocates s->id for Session 1 */

    /* Release Session 1's read snapshot so s->pinned_id becomes WT_TXN_NONE (0), while s->id
     * remains active. */
    __wt_txn_release_snapshot(session1);

    uint64_t oldest_id = 0;
    uint64_t last_running = 0;
    uint64_t metadata_pinned = 0;
    WT_SESSION_IMPL *oldest_session = NULL;

    /* Perform oldest transaction scan */
    __ut_txn_oldest_scan(session1, &oldest_id, &last_running, &metadata_pinned, &oldest_session);

    /*
     * Session 1's s->id is active and is the oldest transaction in the system. oldest_session MUST
     * be Session 1.
     */
    CHECK(oldest_id == session1->txn->time_point.id);
    CHECK(oldest_session == session1);

    REQUIRE(cursor1->close(cursor1) == 0);
    REQUIRE(wt_session1->rollback_transaction(wt_session1, NULL) == 0);
}
