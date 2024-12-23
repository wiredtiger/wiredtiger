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

TEST_CASE("Test generation of sub-level error codes when strerror is called", "[strerror]")
{
    SECTION("Unique sub-level error codes")
    {
        std::vector<std::pair<int, std::string>> errors = {
          {-32000, "WT_NONE: No additional context"},
          {-32001, "WT_COMPACTION_ALREADY_RUNNING: Compaction is already running"},
          {-32002, "WT_SESSION_MAX: Max capacity of configured sessions reached"},
          {-32003, "WT_CACHE_OVERFLOW: Cache capacity has overflown"},
          {-32004, "WT_WRITE_CONFLICT: Write conflict between concurrent operations"},
          {-32005, "WT_OLDEST_FOR_EVICTION: Transaction has the oldest pinned transaction ID"},
          {-32006, "WT_CONFLICT_BACKUP: Conflict performing operation due to running backup"},
          {-32007,
            "WT_CONFLICT_DHANDLE: Another thread currently holds the data handle of the table"},
          {-32008, "WT_CONFLICT_SCHEMA_LOCK: Conflict grabbing WiredTiger schema lock"},
          {-32009, "WT_UNCOMMITTED_DATA: Table has uncommitted data"},
          {-32010, "WT_DIRTY_DATA: Table has dirty data"},
          {-32011, "WT_CONFLICT_TABLE_LOCK: Another thread currently holds the table lock"},
        };

        for (auto const [code, expected] : errors)
            check_error_code(code, expected);
    }
}
