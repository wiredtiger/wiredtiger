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
 * A transaction prepared before the step-down timestamp is set, but resolving (commit or rollback)
 * after it, straddles the boundary. Its write already sits in the stable constituent under the
 * pre-boundary routing rules, yet by the time it resolves its final timestamp can land above the
 * step-down timestamp -- and unlike an ordinary in-flight writer, a prepared transaction cannot be
 * rolled back to force a retry into ingest. These tests exercise the relocation mechanism that
 * resolves the straddle by duplicating the still-prepared update onto ingest before the transaction
 * resolves, so the caller's own resolution call (commit or rollback) can apply to the clone the
 * same way it applies to the original.
 *
 * Only a single (leader) connection is needed for these tests: the mechanism runs entirely inside
 * transaction commit and rollback based on the step-down timestamp and the transaction's own
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
      check_session->open_cursor(check_session, TABLE_URI.c_str(), nullptr, nullptr, &cursor) == 0);
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

TEST_CASE("Layered step-down: an ordinary post-boundary prepared transaction is unaffected",
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
     * The transaction begins with no boundary set, so it routes its write to stable; the boundary
     * only appears afterward. Preparing it now would freeze it as an unresolvable straddler, so it
     * must be rejected here, while rollback is still legal, rather than at commit.
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
     * Only the durable timestamp determines what a prepared transaction's commit actually makes
     * durable; a prepared commit's own commit timestamp can be set independently and is no longer
     * checked against the boundary (a low commit timestamp here, on its own, is not an error). The
     * durable timestamp landing at or below the boundary is what must be rejected.
     *
     * Set it through a separate timestamp_transaction call rather than passing it directly to
     * commit_transaction: once a transaction is prepared, any error returned from inside
     * commit_transaction itself is treated as fatal (there is no safe way to reject a commit the
     * coordinator already expects to succeed), so this check has to be caught here instead, while
     * it is still just a configuration error and not an attempted commit.
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

    /* Same reasoning as the durable-timestamp case: catch this before rollback_transaction itself
     * is called, since a prepared transaction cannot tolerate an error from that call either. */
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

    /*
     * Two writes to the same key before prepare: WiredTiger chains them on one page and marks only
     * the first as the op to resolve (WT_TXN_OP_KEY_REPEATED on the second), so duplication must
     * fire once, on the primary op, and clone whatever the chain currently resolves to -- the
     * second (latest) value -- not the first.
     */
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

    /* The chain's head is a tombstone here, not a standard update; the clone must preserve that. */
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
     * A plain file cursor has no concept of the tombstone marker ingest uses in place of a real
     * tombstone, so it finds the key rather than reporting it deleted; a layered-aware reader is
     * the one that turns this value back into WT_NOTFOUND.
     */
    REQUIRE(cursor->search(cursor) == 0);
    const char *value;
    REQUIRE(cursor->get_value(cursor, &value) == 0);
    CHECK(
      std::string(value) == std::string((const char *)__wt_tombstone.data, __wt_tombstone.size));
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

    /*
     * A modify against the transaction's own prior insert makes the chain's head a delta, not a
     * full value; the clone must reconstruct the full value rather than copy the delta bytes.
     */
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
     * A prepared transaction that begins before the step-down boundary, with a rollback timestamp
     * landing after it, straddles the boundary on the rollback path. The relocation mechanism
     * clones the update onto ingest before rollback applies, then rollback deletes both the
     * original and the clone, leaving the key correctly absent.
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

    /* It is also correctly absent from the ingest constituent. */
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

#endif /* !_WIN32 */
