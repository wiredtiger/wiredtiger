/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * FIXME-WT-13871 - Expand on this comment.
 * [live_restore_extent_list]
 */

#include <catch2/catch.hpp>

#include "../utils_live_restore_extent_list.h"
#include "wt_internal.h"

using namespace utils;


TEST_CASE("Live Restore Extent Lists: XXXXXXX", "[live_restore_extent_list]")
{
    SECTION("Test 1")
    {
        REQUIRE(1 == 1);
    }

    SECTION("Test 2")
    {
        REQUIRE(2 == 2);
    }
}
