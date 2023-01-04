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

/* Assert that WT assertion fired with the expected message, then clear the flag and message. */
void
expect_assertion(WT_SESSION_IMPL *session, std::string expected_message)
{
    REQUIRE(session->unittest_assert_hit);
    REQUIRE(std::string(session->unittest_assert_msg) == expected_message);

    // Clear the assertion flag and message for the next test step.
    session->unittest_assert_hit = false;
    memset(session->unittest_assert_msg, 0, WT_SESSION_UNITTEST_BUF_LEN);
}

/* Assert that no WT assertion fired. */
void
expect_no_assertion(WT_SESSION_IMPL *session)
{
    REQUIRE_FALSE(session->unittest_assert_hit);
    REQUIRE(std::string(session->unittest_assert_msg).empty());
}

/* Wrapper to call WT_RET_ASSERT. */
int
call_wt_ret(WT_SESSION_IMPL *session, uint16_t category, bool assert_should_pass)
{
    if (assert_should_pass)
        WT_RET_ASSERT(session, category, 1 == 1, -1, "WT_RET raised assert");
    else
        WT_RET_ASSERT(session, category, 1 == 2, -1, "WT_RET raised assert");

    return 14;
}

/* Wrapper to call WT_ERR_ASSERT. */
int
call_wt_err(WT_SESSION_IMPL *session, uint16_t category, bool assert_should_pass)
{
    WT_DECL_RET;

    if (assert_should_pass)
        WT_ERR_ASSERT(session, category, 1 == 1, -1, "WT_ERR raised assert");
    else
        WT_RET_ASSERT(session, category, 1 == 2, -1, "WT_ERR raised assert");

    ret = 14;

    if (0) {
err:
        ret = 13;
    }
    return ret;
}

/* Wrapper to call WT_RET_PANIC_ASSERT. */
int
call_wt_panic(WT_SESSION_IMPL *session, uint16_t category, bool assert_should_pass)
{
    WT_DECL_RET;

    if (assert_should_pass)
        WT_RET_PANIC_ASSERT(session, category, 1 == 1, -1, "WT_PANIC raised assert");
    else
        WT_RET_PANIC_ASSERT(session, category, 1 == 2, -1, "WT_PANIC raised assert");

    ret = 14;

    return ret;
}

/* Wrapper to call WT_ASSERT_OPTIONAL. */
int
call_wt_optional(WT_SESSION_IMPL *session, uint16_t category, bool assert_should_pass)
{
    if (assert_should_pass)
        WT_ASSERT_OPTIONAL(session, category, 1 == 1, "WT_OPTIONAL raised assert");
    else
        WT_ASSERT_OPTIONAL(session, category, 1 == 2, "WT_OPTIONAL raised assert");

    return 14;
}

/* Assert that WT_ASSERT and WT_ASSERT_ALWAYS behave consistently regardless of the HAVE_DIAGNOSTIC
 * configuration. */
int
assert_always_aborts(WT_SESSION_IMPL *session)
{
    // WT_ASSERT does nothing.
    WT_ASSERT(session, 1 == 2);
    expect_no_assertion(session);

    // WT_ASSERT_ALWAYS aborts.
    WT_ASSERT_ALWAYS(session, 1 == 2, "Values are not equal!");
    expect_assertion(session, "Assertion '1 == 2' failed: Values are not equal!");

    return 0;
}

/* Assert that all diagnostic assert categories are off. */
int
all_diag_asserts_off(WT_SESSION_IMPL *session)
{
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_DATA_VALIDATION)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_INVALID_OP)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_PANIC)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_CONCURRENT_ACCESS)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_OUT_OF_ORDER)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_SLOW_OPERATION)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_VISIBILITY)) == false);

    return 0;
}

/* Assert that all diagnostic assert categories are on. */
int
all_diag_asserts_on(WT_SESSION_IMPL *session)
{
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_DATA_VALIDATION)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_INVALID_OP)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_PANIC)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_CONCURRENT_ACCESS)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_OUT_OF_ORDER)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_SLOW_OPERATION)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_VISIBILITY)) == true);

    return 0;
}

/* Assert that all asserts fire. */
int
all_asserts_abort(WT_SESSION_IMPL *session, uint16_t category, bool assert_should_pass)
{
    int ret = 0;

    REQUIRE(assert_always_aborts(session) == 0);

    REQUIRE(call_wt_optional(session, category, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_OPTIONAL raised assert");

    REQUIRE(call_wt_ret(session, category, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_RET raised assert");

    REQUIRE(call_wt_err(session, category, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_ERR raised assert");

    REQUIRE(call_wt_panic(session, category, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_PANIC raised assert");

    return ret;
}

/* Assert that the expected asserts fire. */
int
configured_asserts_abort(WT_SESSION_IMPL *session, uint16_t category, bool assert_should_pass)
{
    int ret = 0;

    REQUIRE(call_wt_optional(session, category, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_OPTIONAL raised assert");

    REQUIRE(call_wt_ret(session, category, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_RET raised assert");

    REQUIRE(call_wt_err(session, category, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_ERR raised assert");

    REQUIRE(call_wt_panic(session, category, 1 == 2) == 14);
    expect_assertion(session, "Assertion '1 == 2' failed: WT_PANIC raised assert");

    return ret;
}

/* Assert that the expected asserts don't fire. */
int
configured_asserts_off(WT_SESSION_IMPL *session, u_int16_t category, bool assert_should_pass)
{
    int ret = 0;

    REQUIRE(call_wt_optional(session, category, 1 == 2) == 14);
    expect_no_assertion(session);

    REQUIRE(call_wt_ret(session, category, 1 == 2) == -1);
    expect_no_assertion(session);

    REQUIRE(call_wt_err(session, category, 1 == 2) == -1);
    expect_no_assertion(session);

    REQUIRE(call_wt_panic(session, category, 1 == 2) == -31804);
    expect_no_assertion(session);

    return ret;
}

/* Assert that regardless of connection configuration, asserts always disabled/enabled */
TEST_CASE("Connection config: off", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME, "create");
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE(all_diag_asserts_off(session) == 0);
}

/* Assert that regardless of connection configuration, asserts always disabled/enabled */
TEST_CASE("Connection config: on", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME, "create, diagnostic_asserts=[all]");
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE(assert_always_aborts(session) == 0);
    REQUIRE(all_diag_asserts_on(session) == 0);
}

/* When WT_DIAG_ALL is enabled, all asserts are enabled. */
TEST_CASE("Connection config: WT_DIAG_ALL", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME, "create, diagnostic_asserts= [all]");
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE(configured_asserts_abort(session, WT_DIAG_ALL, 1 == 2) == 0);

    // Checking state.
    REQUIRE(all_diag_asserts_on(session) == 0);
}

/* When a category is enabled, all asserts for that category are enabled. */
TEST_CASE("Connection config: check one enabled category", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME, "create, diagnostic_asserts=[out_of_order]");
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_OUT_OF_ORDER)) == true);
    REQUIRE(configured_asserts_abort(session, WT_DIAG_OUT_OF_ORDER, 1 == 2) == 0);
}

/* Asserts that categories are enabled/disabled following the connection configuration. */
TEST_CASE("Connection config: check multiple enabled categories", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME, "create, diagnostic_asserts= [visibility, concurrent_access]");
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE(configured_asserts_abort(session, WT_DIAG_VISIBILITY, 1 == 2) == 0);

    // Checking state.
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_VISIBILITY)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_CONCURRENT_ACCESS)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_SLOW_OPERATION)) == false);
}

/* Asserts that categories are enabled/disabled following the connection configuration. */
TEST_CASE("Connection config: check disabled category", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME, "create, diagnostic_asserts = [invalid_op]");
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_SLOW_OPERATION)) == false);
    REQUIRE(configured_asserts_off(session, WT_DIAG_SLOW_OPERATION, 1 == 2) == 0);
}

/* Reconfigure with diagnostic_asserts not provided. */
TEST_CASE("Reconfigure: diagnostic_asserts not provided", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME, "create");
    auto connection = conn.getWtConnection();
    WT_SESSION_IMPL *session = conn.createSession();

    connection->reconfigure(connection, "");
    REQUIRE(all_diag_asserts_off(session) == 0);
}

/* Reconfigure the connection with diagnostic_asserts as an empty list. */
TEST_CASE("Reconfigure: diagnostic_asserts empty list", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE(all_diag_asserts_off(session) == 0);
    connection->reconfigure(connection, "diagnostic_asserts=[]");
    REQUIRE(all_diag_asserts_off(session) == 0);
}

/* Reconfigure the connection with diagnostic_asserts as a list with invalid item. */
TEST_CASE("Reconfigure: diagnostic_asserts with invalid item", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE(all_diag_asserts_off(session) == 0);
    REQUIRE(
      connection->reconfigure(connection, "diagnostic_asserts=[slow_operation, panic, INVALID]"));
    REQUIRE(all_diag_asserts_off(session) == 0);
}

/* Reconfigure the connection with diagnostic_asserts as a list of valid items. */
TEST_CASE("Reconfigure: diagnostic_asserts with valid items", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME);
    auto connection = conn.getWtConnection();
    WT_SESSION_IMPL *session = conn.createSession();

    connection->reconfigure(connection, "diagnostic_asserts=[data_validation, invalid_op, panic]");

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_DATA_VALIDATION)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_INVALID_OP)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_PANIC)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_CONCURRENT_ACCESS)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_OUT_OF_ORDER)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_SLOW_OPERATION)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_VISIBILITY)) == false);
}

/* Reconfigure with assertion categories changed from enabled->disabled and vice-versa. */
TEST_CASE("Reconfigure: Transition cases", "[assertions]")
{
    ConnectionWrapper conn(
      DB_HOME, "create, diagnostic_asserts= [concurrent_access, out_of_order]");
    auto connection = conn.getWtConnection();
    WT_SESSION_IMPL *session = conn.createSession();

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_CONCURRENT_ACCESS)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_OUT_OF_ORDER)) == true);

    connection->reconfigure(
      connection, "diagnostic_asserts=[data_validation, slow_operation, out_of_order]");
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_OUT_OF_ORDER)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_DATA_VALIDATION)) == true);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_SLOW_OPERATION)) == true);

    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_CONCURRENT_ACCESS)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_VISIBILITY)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_INVALID_OP)) == false);
    REQUIRE((DIAGNOSTIC_ASSERTS_ENABLED(session, WT_DIAG_PANIC)) == false);
}
