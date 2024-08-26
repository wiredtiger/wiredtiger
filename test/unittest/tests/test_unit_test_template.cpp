/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "../wrappers/mock_session.h"
#include "../wrappers/block_mods.h"
/*
 * [test_template]: source_file.c
 * Add comment describing the what the unit test file tests.
 */

/*
 * Each test case should be testing one wiredtiger function. The test case description should be in
 * format "[Module]: Function [Tag]".
 */
TEST_CASE("Boilerplate: test function", "[test_template]")
{
    /*
     * Build Mock session, this will automatically create a mock connection. Remove if not
     * necessary.
     */
    std::shared_ptr<MockSession> session = MockSession::buildTestMockSession();

    /*
     * Sections are a great way to separate the different edge cases for a particular function. In
     * sections description, describe what edge case is being tested. Remove if not necessary.
     */
    SECTION("Boilerplate test section") {}
}
