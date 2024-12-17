/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include <string>

static void
check_error_code(int error, std::string expected)
{
    std::string result = wiredtiger_strerror(error);
    CHECK(result == expected);
}

TEST_CASE("Test generation of sub-level error codes when strerror is called")
{
    /* Basic default sub-level error code */
    check_error_code(-32000, "WT_NONE: last API call was successful");

    SECTION("Unique sub-level error codes")
    {
        std::vector<std::pair<int, std::string>> errors = {
          {-32001,
            "WT_COMPACTION_ALREADY_RUNNING: Cannot reconfigure background compaction while it's "
            "already running"},
          {-32002, "WT_SESSION_MAX: out of sessions (including internal sessions)"},
        };

        for (auto const [code, expected] : errors)
            check_error_code(code, expected);
    }
}
