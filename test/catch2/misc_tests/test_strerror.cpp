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
          {-32000, "WT_NONE: last API call was successful"},
          {-32001,
            "WT_COMPACTION_ALREADY_RUNNING: cannot reconfigure background compaction while it's "
            "already running"},
          {-32002, "WT_SESSION_MAX: out of sessions (including internal sessions)"},
          {-32003, "WT_CACHE_OVERFLOW: transaction rolled back because of cache overflow"},
          {-32004, "WT_WRITE_CONFLICT: conflict between concurrent operations"},
          {-32005, "WT_OLDEST_FOR_EVICTION: oldest pinned transaction ID rolled back for eviction"},
          {-32006, "WT_CONFLICT_BACKUP: the table is currently performing backup"},
          {-32007, "WT_CONFLICT_DHANDLE: another thread is accessing the table"},
          {-32008, "WT_CONFLICT_SCHEMA_LOCK: another thread is performing a schema operation"},
          {-32009,
            "WT_UNCOMMITTED_DATA: the table has uncommitted data and can not be dropped yet"},
          {-32010, "WT_DIRTY_DATA: the table has dirty data and can not be dropped yet"},
          {-32011,
            "WT_CONFLICT_TABLE_LOCK: another thread is currently reading or writing on the "
            "table"},
        };

        for (auto const [code, expected] : errors)
            check_error_code(code, expected);
    }
}
