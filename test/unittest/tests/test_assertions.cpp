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

TEST_CASE("Simple implementation of unit testing WT_ASSERT_ALWAYS", "[assertions]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.createSession();

    // Check that the new session has set up our test fields correctly.
    expect_no_assertion(session);

    SECTION("Basic WT_ASSERT_ALWAYS tests")
    {
        WT_ASSERT_ALWAYS(session, 1 == 2, "Values are not equal!");
        expect_assertion(session, "Assertion '1 == 2' failed: Values are not equal!");

        WT_ASSERT_ALWAYS(session, 1 == 1, "Values are not equal!");
        expect_no_assertion(session);
    }
}
