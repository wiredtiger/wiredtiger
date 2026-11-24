/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#ifndef _WIN32

#include <cstring>
#include <string>

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"
#include "layered_disagg_utils.h"
#include "../../utility/test_util.h"

/*
 * A transaction prepared before the step-down timestamp is set, and resolved after it, can no
 * longer be rolled back and retried. These tests check that its commit or rollback is copied to the
 * ingest table. A single leader connection is enough: no role change or checkpoint is needed.
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
setup_leader(connection_wrapper **conn_wrap, WT_SESSION **sessionp, bool preserve_prepared = false)
{
    testutil_system("rm -rf %s && mkdir -p %s/kv_home", HOME.c_str(), HOME.c_str());

    std::string cfg = layered_disagg_build_cfg("leader");
    if (preserve_prepared) {
        cfg += ",preserve_prepared=true";
        cfg += ",precise_checkpoint=true";
    }
    *conn_wrap = new connection_wrapper(HOME, cfg.c_str());
    WT_CONNECTION *conn = (*conn_wrap)->get_wt_connection();
    WT_SESSION *session = (WT_SESSION *)(*conn_wrap)->create_session();

    REQUIRE(session->create(session, TABLE_URI.c_str(), TABLE_CFG) == 0);
    REQUIRE(conn->set_timestamp(conn, "oldest_timestamp=1,stable_timestamp=1") == 0);

    *sessionp = session;
}

TEST_CASE("Layered step-down: a prepared commit straddling the boundary lands in ingest",
  "[layered_prepare_stepdown]")
{
    const bool reserve = GENERATE(false, true);
    CAPTURE(reserve);
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);

    /* Prepare, then set the step-down timestamp, then commit after it. */
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "straddler");
    cursor->set_value(cursor, "straddler-value");
    REQUIRE(cursor->insert(cursor) == 0);
    if (reserve) {
        cursor->set_key(cursor, "straddler");
        REQUIRE(cursor->reserve(cursor) == 0);
    }
    REQUIRE(session->prepare_transaction(session, "prepare_timestamp=10") == 0);

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);

    /* A prepared transaction must be allowed to commit. */
    REQUIRE(session->commit_transaction(session, "commit_timestamp=12,durable_timestamp=12") == 0);
    REQUIRE(cursor->close(cursor) == 0);

    /* The value is visible through the layered table. */
    WT_SESSION *check_session = (WT_SESSION *)conn_wrap->create_session();
    REQUIRE(
      check_session->open_cursor(check_session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(check_session->begin_transaction(check_session, "read_timestamp=20") == 0);
    cursor->set_key(cursor, "straddler");
    REQUIRE(cursor->search(cursor) == 0);
    const char *value;
    REQUIRE(cursor->get_value(cursor, &value) == 0);
    CHECK(std::string(value) == "straddler-value");
    REQUIRE(check_session->rollback_transaction(check_session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    /* The value is in the ingest table, so it survives the step-down. */
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

TEST_CASE("Layered step-down: an ordinary post-boundary prepared transaction is unaffected",
  "[layered_prepare_stepdown]")
{
    const bool commit_below_boundary = GENERATE(false, true);
    CAPTURE(commit_below_boundary);
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);

    /* A transaction that begins after the step-down timestamp is set writes to ingest directly. */
    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "post-boundary");
    cursor->set_value(cursor, "post-boundary-value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(session->prepare_transaction(session,
              commit_below_boundary ? "prepare_timestamp=5" : "prepare_timestamp=15") == 0);
    REQUIRE(session->timestamp_transaction(session,
              commit_below_boundary ? "commit_timestamp=10,durable_timestamp=12" :
                                      "commit_timestamp=16,durable_timestamp=16") == 0);
    REQUIRE(session->commit_transaction(session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    WT_SESSION *check_session = (WT_SESSION *)conn_wrap->create_session();
    REQUIRE(check_session->open_cursor(
              check_session, INGEST_URI.c_str(), nullptr, nullptr, &cursor) == 0);
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

TEST_CASE(
  "Layered step-down: preparing a transaction that began before the boundary is rolled back",
  "[layered_prepare_stepdown]")
{
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);

    /*
     * A transaction that wrote before the step-down timestamp was set cannot prepare after it: the
     * prepare is rejected while the transaction can still be rolled back.
     */
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "too-late");
    cursor->set_value(cursor, "too-late-value");
    REQUIRE(cursor->insert(cursor) == 0);

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);

    CHECK(session->prepare_transaction(session, "prepare_timestamp=12") == WT_ROLLBACK);
    REQUIRE(cursor->close(cursor) == 0);
    REQUIRE(session->rollback_transaction(session, nullptr) == 0);

    delete conn_wrap;
}

TEST_CASE(
  "Layered step-down: a post-boundary prepared commit's durable timestamp must be after "
  "the boundary",
  "[layered_prepare_stepdown]")
{
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "low-durable");
    cursor->set_value(cursor, "low-durable-value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(session->prepare_transaction(session, "prepare_timestamp=5") == 0);

    /*
     * A low commit timestamp is allowed; a durable timestamp at or before the step-down timestamp
     * is not. Set it before commit, because a prepared commit that fails is fatal.
     */
    REQUIRE(session->timestamp_transaction(session, "commit_timestamp=10") == 0);
    CHECK(session->timestamp_transaction(session, "durable_timestamp=11") == EINVAL);
    REQUIRE(cursor->close(cursor) == 0);
    REQUIRE(session->rollback_transaction(session, nullptr) == 0);

    delete conn_wrap;
}

TEST_CASE(
  "Layered step-down: a post-boundary prepared rollback's rollback timestamp must be "
  "after the boundary",
  "[layered_prepare_stepdown]")
{
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "low-rollback");
    cursor->set_value(cursor, "low-rollback-value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(session->prepare_transaction(session, "prepare_timestamp=5") == 0);

    /* Set it before rollback, because a prepared rollback that fails is fatal. */
    CHECK(session->timestamp_transaction(session, "rollback_timestamp=11") == EINVAL);
    REQUIRE(cursor->close(cursor) == 0);
    REQUIRE(session->rollback_transaction(session, nullptr) == 0);

    delete conn_wrap;
}

TEST_CASE(
  "Layered step-down: a prepared commit straddler with multiple updates to the same key "
  "relocates the latest one",
  "[layered_prepare_stepdown]")
{
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);

    /* Two writes to the same key: ingest gets the latest value. */
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "repeated-key");
    cursor->set_value(cursor, "first-value");
    REQUIRE(cursor->insert(cursor) == 0);
    cursor->set_key(cursor, "repeated-key");
    cursor->set_value(cursor, "second-value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(session->prepare_transaction(session, "prepare_timestamp=10") == 0);

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);
    REQUIRE(session->commit_transaction(session, "commit_timestamp=12,durable_timestamp=12") == 0);
    REQUIRE(cursor->close(cursor) == 0);

    WT_SESSION *check_session = (WT_SESSION *)conn_wrap->create_session();
    REQUIRE(check_session->open_cursor(
              check_session, INGEST_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(check_session->begin_transaction(check_session, "read_timestamp=20") == 0);
    cursor->set_key(cursor, "repeated-key");
    REQUIRE(cursor->search(cursor) == 0);
    const char *value;
    REQUIRE(cursor->get_value(cursor, &value) == 0);
    CHECK(std::string(value) == "second-value");
    REQUIRE(check_session->rollback_transaction(check_session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    delete conn_wrap;
}

TEST_CASE(
  "Layered step-down: a prepared commit straddler that inserts then removes the same key "
  "relocates the removal",
  "[layered_prepare_stepdown]")
{
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);

    /* The last write to the key is a remove, so ingest gets a remove. */
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "inserted-then-removed");
    cursor->set_value(cursor, "value");
    REQUIRE(cursor->insert(cursor) == 0);
    cursor->set_key(cursor, "inserted-then-removed");
    REQUIRE(cursor->remove(cursor) == 0);
    REQUIRE(session->prepare_transaction(session, "prepare_timestamp=10") == 0);

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);
    REQUIRE(session->commit_transaction(session, "commit_timestamp=12,durable_timestamp=12") == 0);
    REQUIRE(cursor->close(cursor) == 0);

    WT_SESSION *check_session = (WT_SESSION *)conn_wrap->create_session();
    REQUIRE(check_session->open_cursor(
              check_session, INGEST_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(check_session->begin_transaction(check_session, "read_timestamp=20") == 0);
    cursor->set_key(cursor, "inserted-then-removed");
    /*
     * Ingest stores a remove as a marker value, which a cursor opened directly on ingest returns as
     * an ordinary value.
     */
    REQUIRE(cursor->search(cursor) == 0);
    const char *value;
    REQUIRE(cursor->get_value(cursor, &value) == 0);
    /* The marker is not null-terminated, so compare by its length. */
    CHECK(memcmp(value, __wt_tombstone.data, __wt_tombstone.size) == 0);
    REQUIRE(check_session->rollback_transaction(check_session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    delete conn_wrap;
}

TEST_CASE(
  "Layered step-down: a prepared commit straddler that inserts then modifies the same key "
  "relocates the reconstructed modified value",
  "[layered_prepare_stepdown]")
{
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);

    /* The last write to the key is a modify, so ingest gets the full modified value. */
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "insert-then-modify");
    cursor->set_value(cursor, "initial");
    REQUIRE(cursor->insert(cursor) == 0);

    cursor->set_key(cursor, "insert-then-modify");
    WT_MODIFY mods[1];
    mods[0].data.data = "reconstructed";
    mods[0].data.size = strlen("reconstructed");
    mods[0].offset = 0;
    mods[0].size = mods[0].data.size;
    REQUIRE(cursor->modify(cursor, mods, 1) == 0);
    REQUIRE(session->prepare_transaction(session, "prepare_timestamp=10") == 0);

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);
    REQUIRE(session->commit_transaction(session, "commit_timestamp=12,durable_timestamp=12") == 0);
    REQUIRE(cursor->close(cursor) == 0);

    WT_SESSION *check_session = (WT_SESSION *)conn_wrap->create_session();
    REQUIRE(check_session->open_cursor(
              check_session, INGEST_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(check_session->begin_transaction(check_session, "read_timestamp=20") == 0);
    cursor->set_key(cursor, "insert-then-modify");
    REQUIRE(cursor->search(cursor) == 0);
    const char *value;
    REQUIRE(cursor->get_value(cursor, &value) == 0);
    CHECK(std::string(value) == "reconstructed");
    REQUIRE(check_session->rollback_transaction(check_session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    delete conn_wrap;
}

TEST_CASE("Layered step-down: a prepared rollback straddling the boundary is correctly relocated",
  "[layered_prepare_stepdown]")
{
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session, /*preserve_prepared=*/true);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);

    /*
     * Prepare, then set the step-down timestamp, then roll back after it. The key must be absent
     * from both the layered table and ingest.
     */
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "straddler-rollback");
    cursor->set_value(cursor, "straddler-rollback-value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(session->prepare_transaction(session, "prepare_timestamp=10,prepared_id=1") == 0);

    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);

    REQUIRE(session->rollback_transaction(session, "rollback_timestamp=12") == 0);
    REQUIRE(cursor->close(cursor) == 0);

    /* The key is not visible through the layered table. */
    WT_SESSION *check_session = (WT_SESSION *)conn_wrap->create_session();
    REQUIRE(
      check_session->open_cursor(check_session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(check_session->begin_transaction(check_session, "read_timestamp=20") == 0);
    cursor->set_key(cursor, "straddler-rollback");
    CHECK(cursor->search(cursor) == WT_NOTFOUND);
    REQUIRE(check_session->rollback_transaction(check_session, nullptr) == 0);
    REQUIRE(cursor->close(cursor) == 0);

    /* It is also absent from ingest. */
    WT_CURSOR *ingest_cursor;
    REQUIRE(check_session->open_cursor(
              check_session, INGEST_URI.c_str(), nullptr, nullptr, &ingest_cursor) == 0);
    REQUIRE(check_session->begin_transaction(check_session, "read_timestamp=20") == 0);
    ingest_cursor->set_key(ingest_cursor, "straddler-rollback");
    CHECK(ingest_cursor->search(ingest_cursor) == WT_NOTFOUND);
    REQUIRE(check_session->rollback_transaction(check_session, nullptr) == 0);
    REQUIRE(ingest_cursor->close(ingest_cursor) == 0);

    delete conn_wrap;
}

TEST_CASE("Layered step-down: prepared resolution can replace an existing ingest tombstone",
  "[layered_prepare_stepdown]")
{
    const bool commit = GENERATE(true, false);
    connection_wrapper *conn_wrap;
    WT_SESSION *session;
    setup_leader(&conn_wrap, &session, /*preserve_prepared=*/true);
    WT_CONNECTION *conn = conn_wrap->get_wt_connection();

    WT_CURSOR *cursor;
    REQUIRE(session->open_cursor(session, INGEST_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "straddler");
    cursor->set_value(cursor, "old-value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(session->commit_transaction(session, "commit_timestamp=2") == 0);

    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "straddler");
    REQUIRE(cursor->remove(cursor) == 0);
    REQUIRE(session->commit_transaction(session, "commit_timestamp=3") == 0);
    REQUIRE(cursor->close(cursor) == 0);

    REQUIRE(session->open_cursor(session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
    REQUIRE(session->begin_transaction(session, nullptr) == 0);
    cursor->set_key(cursor, "straddler");
    cursor->set_value(cursor, "straddler-value");
    REQUIRE(cursor->insert(cursor) == 0);
    REQUIRE(session->prepare_transaction(session, "prepare_timestamp=10,prepared_id=1") == 0);
    REQUIRE(conn->set_timestamp(conn, "step_down_timestamp=11") == 0);

    if (commit)
        REQUIRE(
          session->commit_transaction(session, "commit_timestamp=12,durable_timestamp=12") == 0);
    else
        REQUIRE(session->rollback_transaction(session, "rollback_timestamp=12") == 0);
    REQUIRE(cursor->close(cursor) == 0);

    for (const auto &uri : {TABLE_URI, INGEST_URI}) {
        REQUIRE(session->open_cursor(session, uri.c_str(), nullptr, nullptr, &cursor) == 0);
        REQUIRE(session->begin_transaction(session, "read_timestamp=20") == 0);
        cursor->set_key(cursor, "straddler");
        if (commit) {
            REQUIRE(cursor->search(cursor) == 0);
            const char *value;
            REQUIRE(cursor->get_value(cursor, &value) == 0);
            CHECK(std::string(value) == "straddler-value");
        } else
            CHECK(cursor->search(cursor) == WT_NOTFOUND);
        REQUIRE(session->rollback_transaction(session, nullptr) == 0);
        REQUIRE(cursor->close(cursor) == 0);
    }

    delete conn_wrap;
}

#endif /* !_WIN32 */
