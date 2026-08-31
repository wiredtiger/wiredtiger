/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef _WIN32

#include <string>

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"
#include "layered_disagg_utils.h"
#include "../../utility/test_util.h"

/*
 * test_layered_prepare_stepdown.cpp
 *
 * WT-18422: a transaction prepared before the step-down timestamp is set, but resolving (commit or
 * rollback) after it, straddles the boundary. Its write already sits in the stable constituent
 * under the pre-boundary routing rules, yet by the time it resolves its final timestamp can land
 * above the step-down timestamp -- and unlike an ordinary in-flight writer, a prepared transaction
 * cannot be rolled back to force a retry into ingest. These tests exercise the relocation
 * mechanism in src/txn/txn.c that resolves the straddle by duplicating the still-prepared update
 * onto ingest before the transaction resolves, so the caller's own resolution call (commit or
 * rollback) can apply to the clone the same way it applies to the original.
 *
 * Only a single (leader) connection is needed for these tests: the mechanism runs entirely inside
 * __wt_txn_commit / __wt_txn_rollback based on the step-down timestamp and the transaction's own
 * timestamps, with no dependency on an actual role change or checkpoint having happened.
 */

static const std::string TABLE_NAME = "test_layered_prepare_stepdown";
static const std::string TABLE_URI = "layered:" + TABLE_NAME;
static const std::string INGEST_URI = "file:" + TABLE_NAME + ".wt_ingest";
static const std::string HOME = "WT_TEST.layered_prepare_stepdown";
static const char *TABLE_CFG = "key_format=S,value_format=S,block_manager=disagg,type=layered";

/*
 * setup_leader --
 *     Create a fresh home directory and a disaggregated leader connection with one layered table,
 *     oldest and stable timestamps pinned at 1.
 */
static void
setup_leader(connection_wrapper **conn_wrap, WT_SESSION **sessionp)
{
    testutil_system("rm -rf %s && mkdir -p %s/kv_home", HOME.c_str(), HOME.c_str());

    *conn_wrap = new connection_wrapper(HOME, layered_disagg_build_cfg("leader").c_str());
    WT_CONNECTION *conn = (*conn_wrap)->get_wt_connection();
    WT_SESSION *session = (WT_SESSION *)(*conn_wrap)->create_session();

    REQUIRE(session->create(session, TABLE_URI.c_str(), TABLE_CFG) == 0);
    REQUIRE(conn->set_timestamp(conn, "oldest_timestamp=1,stable_timestamp=1") == 0);

    *sessionp = session;
}

TEST_CASE("Layered step-down: a prepared commit straddling the boundary lands in ingest",
  "[layered_prepare_stepdown]")
{
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);

    /*
     * Sid's straddler scenario: prepare while there is no step-down boundary yet, then the boundary
     * is set, then commit lands above it.
     */
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "straddler");
    cursor->set_value(cursor, "straddler-value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(session->prepare_transaction(session, "prepare_timestamp=10") == 0);

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);

    /* This must not be rejected: the coordinator already committed to this outcome at prepare. */
    REQUIRE(session->commit_transaction(session, "commit_timestamp=12,durable_timestamp=12") == 0);
    REQUIRE(cursor->close(cursor) == 0);

    /* The value is visible through the layered table above the boundary. */
    WT_SESSION *check_session = (WT_SESSION *)conn_wrap->create_session();
    REQUIRE(
      check_session->open_cursor(check_session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) ==
      0);
    REQUIRE(check_session->begin_transaction(check_session, "read_timestamp=20") == 0);
    cursor->set_key(cursor, "straddler");
    REQUIRE(cursor->search(cursor) == 0);
    const char *value;
    REQUIRE(cursor->get_value(cursor, &value) == 0);
    CHECK(std::string(value) == "straddler-value");
    REQUIRE(check_session->rollback_transaction(check_session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    /* It physically lives on ingest: that is what the step-down checkpoint at ts=11 will omit. */
    WT_CURSOR *ingest_cursor;
    REQUIRE(check_session->open_cursor(
              check_session, INGEST_URI.c_str(), nullptr, nullptr, &ingest_cursor) == 0);
    REQUIRE(check_session->begin_transaction(check_session, "read_timestamp=20") == 0);
    ingest_cursor->set_key(ingest_cursor, "straddler");
    REQUIRE(ingest_cursor->search(ingest_cursor) == 0);
    REQUIRE(ingest_cursor->get_value(ingest_cursor, &value) == 0);
    CHECK(std::string(value) == "straddler-value");
    REQUIRE(check_session->rollback_transaction(check_session, nullptr) == 0);
    REQUIRE(ingest_cursor->close(ingest_cursor) == 0);

    delete conn_wrap;
}

TEST_CASE(
  "Layered step-down: an ordinary post-boundary prepared transaction is unaffected",
  "[layered_prepare_stepdown]")
{
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);

    /*
     * A transaction that begins after the boundary is already ingest-routed and was never a
     * candidate for duplication: this is a regression guard that the new code leaves it alone.
     */
    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "post-boundary");
    cursor->set_value(cursor, "post-boundary-value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(session->prepare_transaction(session, "prepare_timestamp=15") == 0);
    REQUIRE(session->commit_transaction(session, "commit_timestamp=16,durable_timestamp=16") == 0);
    REQUIRE(cursor->close(cursor) == 0);

    WT_SESSION *check_session = (WT_SESSION *)conn_wrap->create_session();
    REQUIRE(
      check_session->open_cursor(check_session, INGEST_URI.c_str(), nullptr, nullptr, &cursor) ==
      0);
    REQUIRE(check_session->begin_transaction(check_session, "read_timestamp=20") == 0);
    cursor->set_key(cursor, "post-boundary");
    REQUIRE(cursor->search(cursor) == 0);
    const char *value;
    REQUIRE(cursor->get_value(cursor, &value) == 0);
    CHECK(std::string(value) == "post-boundary-value");
    REQUIRE(check_session->rollback_transaction(check_session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    delete conn_wrap;
}

#endif /* !_WIN32 */
