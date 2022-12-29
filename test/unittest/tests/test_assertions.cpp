/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "utils.h"
#include "wiredtiger.h"
#include "wrappers/connection_wrapper.h"
#include "wt_internal.h"

/* Assert that a WT assertion fired with the expected message, then clear the flag and message. */
void
expect_assertion(WT_SESSION_IMPL *session, std::string expected_message)
{
    REQUIRE(session->unittest_assert_hit);
    REQUIRE(std::string(session->unittest_assert_msg) == expected_message);

    // Clear the assertion flag and message for the next test step
    session->unittest_assert_hit = false;
    memset(session->unittest_assert_msg, 0, WT_SESSION_UNITTEST_BUF_LEN);
}

void
expect_no_assertion(WT_SESSION_IMPL *session)
{
    REQUIRE_FALSE(session->unittest_assert_hit);
    REQUIRE(std::string(session->unittest_assert_msg).empty());
}

int
call_wt_ret(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    if (assert_should_pass)
        WT_RET_ASSERT(session, 1 == 1, -1, "WT_RET raised assert");
    else
        WT_RET_ASSERT(session, 1 == 2, -1, "WT_RET raised assert");

    return 14;
}

int
call_wt_err(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    WT_DECL_RET;

    if (assert_should_pass)
        WT_ERR_ASSERT(session, 1 == 1, -1, "WT_ERR raised assert");
    else
        WT_RET_ASSERT(session, 1 == 2, -1, "WT_ERR raised assert");

    ret = 14;

    if (0) {
err:
        ret = 13;
    }
    return ret;
}

int
call_wt_panic(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    WT_DECL_RET;

    if (assert_should_pass)
        WT_RET_PANIC_ASSERT(session, 1 == 1, -1, "WT_PANIC raised assert");
    else
        WT_RET_PANIC_ASSERT(session, 1 == 2, -1, "WT_PANIC raised assert");

    ret = 14;

    return ret;
}

int
call_wt_optional(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    if (assert_should_pass)
        WT_ASSERT_OPTIONAL(session, 1 == 1, "WT_PANIC raised assert");
    else
        WT_ASSERT_OPTIONAL(session, 1 == 2, "WT_PANIC raised assert");

    return 14;
}

int
assert_always_aborts(WT_SESSION_IMPL *session)
{
    // Regardless of connection configuration WT_ASSERT is disabled when HAVE_DIAGNOSTIC=0
    // WT_ASSERT_ALWAYS is always enabled.

    // WT_ASSERT does nothing.
    WT_ASSERT(session, 1 == 2);
    expect_no_assertion(session);

    // WT_ASSERT_ALWAYS aborts.
    WT_ASSERT_ALWAYS(session, 1 == 2, "Values are not equal!");
    expect_assertion(session, "Assertion '1 == 2' failed: Values are not equal!");

    return 0;
}

int
all_asserts_abort(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    int ret = 0;

    REQUIRE(assert_always_aborts(session) == 0);

    REQUIRE(call_wt_optional(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_OPTIONAL raised assert");

    REQUIRE(call_wt_ret(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_RET raised assert");

    REQUIRE(call_wt_err(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_ERR raised assert");

    REQUIRE(call_wt_panic(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_PANIC raised assert");

    return ret;
}

int
configured_asserts_abort(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    int ret = 0;

    REQUIRE(call_wt_optional(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_OPTIONAL raised assert");

    REQUIRE(call_wt_ret(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_RET raised assert");

    REQUIRE(call_wt_err(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_ERR raised assert");

    REQUIRE(call_wt_panic(session, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_PANIC raised assert");

    return ret;
}

int
configured_asserts_off(WT_SESSION_IMPL *session, bool assert_should_pass)
{
    int ret = 0;

    REQUIRE(call_wt_optional(session, 1 == 2) == 14);
    expect_no_assertion(session);

    REQUIRE(call_wt_ret(session, 1 == 2) == 14);
    expect_no_assertion(session);

    REQUIRE(call_wt_err(session, 1 == 2) == 14);
    expect_no_assertion(session);

    REQUIRE(call_wt_panic(session, 1 == 2) == 14);
    expect_no_assertion(session);

    return ret;
}

TEST_CASE("2a1", "[assertions]")
{
    // Regardless of connection configuration, asserts always disabled/enabled
    ConnectionWrapper conn(DB_HOME, "create, diagnostic_asserts=off");
    auto connection = conn.getWtConnection();
    // Configure the session deafult configuration.
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
    REQUIRE(assert_always_aborts(session) == 0);
    // Reconfigure the connection with diagnostic_asserts = on.
    connection->reconfigure(connection, "diagnostic_asserts=on");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == true);
    REQUIRE(assert_always_aborts(session) == 0);
}

TEST_CASE("2a2", "[assertions]")
{
    // WT_DIAG_ALL category
    ConnectionWrapper conn(DB_HOME, "create, diagnostic_asserts= [WT_DIAG_ALL]");
    auto connection = conn.getWtConnection();

    // Configure the session deafult configuration.
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == true);
    REQUIRE(configured_asserts_abort(session, 1 == 2) == 0);
}

TEST_CASE("2a3i", "[assertions]")
{
    // Connection takes C as an option
    ConnectionWrapper conn(DB_HOME, "create, diagnostic_asserts= [visibility]");
    auto connection = conn.getWtConnection();
    WT_SESSION_IMPL *session = conn.createSession();

    if (DIAGNOSTIC_ASSERTS_ENABLED(session, visibility))
        REQUIRE(configured_asserts_abort(session, 1 == 2) == 0);
}

TEST_CASE("2a3ii", "[assertions]")
{
    // Connection takes C as one of the options
    ConnectionWrapper conn(DB_HOME, "create, diagnostic_asserts= [visibility, concurrent_access]");
    auto connection = conn.getWtConnection();

    // Configure the session deafult configuration.
    WT_SESSION_IMPL *session = conn.createSession();

    // Reconfigure the connection with diagnostic_asserts = WT_DIAG_ALL.
    if (DIAGNOSTIC_ASSERTS_ENABLED(session, visibility))
        REQUIRE(configured_asserts_abort(session, 1 == 2) == 0);
}

TEST_CASE("2a3iii", "[assertions]")
{
    // Connection does not take C or DIAG_ALL as one of the options
    ConnectionWrapper conn(DB_HOME, "create, diagnostic_asserts= [visibility, concurrent_access]");
    auto connection = conn.getWtConnection();
    // Configure the session deafult configuration.
    WT_SESSION_IMPL *session = conn.createSession();

    if ((DIAGNOSTIC_ASSERTS_ENABLED(session, out_of_order))
        REQUIRE(configured_asserts_off(session, 1 == 2) == 0);
}

TEST_CASE("2b1", "[assertions]")
{
    // Reconfigure the connection with diagnostic_asserts not provided
    ConnectionWrapper conn(DB_HOME, "create");
    auto connection = conn.getWtConnection();
    // Configure the session deafult configuration.
    WT_SESSION_IMPL *session = conn.createSession();

    onnection->reconfigure(connection, "");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
    REQUIRE(configured_asserts_off(session, 1 == 2) == 0);
}

TEST_CASE("2b2", "[assertions]")
{
    // Reconfigure the connection with diagnostic_asserts as an empty list
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();

    // Configure the session deafult configuration.
    WT_SESSION_IMPL *session = conn.createSession();

    connection->reconfigure(connection, "diagnostic_asserts=[]");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
    REQUIRE(configured_asserts_off(session, 1 == 2) == 0);
}

TEST_CASE("2b3i", "[assertions]")
{
    // Reconfigure the connection with diagnostic_asserts as a list with invalid item
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();

    // Configure the session deafult configuration.
    WT_SESSION_IMPL *session = conn.createSession();
    REQUIRE(WT_ERR(connection->reconfigure(connection, "diagnostic_asserts=[slow_operation, panic, INVALID]"));

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session)) == false);
}

TEST_CASE("2b3i", "[assertions]")
{
    // Reconfigure the connection with diagnostic_asserts as a list of valid items
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();

    // Configure the session deafult configuration.
    WT_SESSION_IMPL *session = conn.createSession();
    connection->reconfigure(connection, "diagnostic_asserts=[data_validation, invalid_op, panic]");

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, data_validation)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, invalid_op)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, panic)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, concurrent_access)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, out_of_order)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, slow_operation)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, visibility)) == false);
}

TEST_CASE("Transition", "[assertions]")
{
    // Reconfigure with different assert categories configured
    ConnectionWrapper conn(
      DB_HOME, "create, diagnostic_asserts= [concurrent_access, out_of_order]");
    auto connection = conn.getWtConnection();

    // Configure the session deafult configuration.
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, concurrent_access)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, out_of_order)) == true);

    connection->reconfigure(
      connection, "diagnostic_asserts=[data_validation, slow_operation, out_of_order]");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, out_of_order)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, data_validation)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, slow_operation)) == true);

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, concurrent_access)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, visibility)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, invalid_op)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, panic)) == false);
}
