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

TEST_CASE("Test generation of sub-level error codes when strerror is called")
{
    /* Basic default sub-level error code */
    std::string result = wiredtiger_strerror(-32000);
    std::string expected("WT_NONE: last API call was successful");
    CHECK(result == expected);

    SECTION("Unique sub-level error codes")
    {
        CHECK(true);
        /*
        WT_COMPACTION_ALREADY_RUNNING
        WT_SESSION_MAX
        WT_CACHE_OVERFLOW
        WT_WRITE_CONFLICT
        WT_OLDEST_FOR_EVICTION
        WT_CONFLICT_BACKUP
        WT_CONFLICT_DHANDLE
        WT_CONFLICT_SCHEMA_LOCK
        WT_UNCOMMITTED_DATA
        WT_DIRTY_DATA
        WT_CONFLICT_TABLE_LOCK
        */
    }
}
