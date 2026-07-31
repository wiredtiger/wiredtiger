/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"

TEST_CASE("Eviction bounded wait remaining time", "[evict]")
{
    REQUIRE(__evict_bounded_wait_remaining_us(0) == WTI_EVICT_BOUNDED_WAIT_US);
    REQUIRE(__evict_bounded_wait_remaining_us(WTI_EVICT_BOUNDED_WAIT_US - 1) == 1);
    REQUIRE(__evict_bounded_wait_remaining_us(WTI_EVICT_BOUNDED_WAIT_US) == 0);
    REQUIRE(__evict_bounded_wait_remaining_us(WTI_EVICT_BOUNDED_WAIT_US + 1) == 0);
}
