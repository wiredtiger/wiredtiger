/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"

TEST_CASE("Ordinary eviction exposes a lone entry", "[evict_lru_candidate_count]")
{
    REQUIRE(__evict_lru_candidate_count(1, 0) == 1);
}

TEST_CASE("Ordinary eviction caps larger queues at half", "[evict_lru_candidate_count]")
{
    REQUIRE(__evict_lru_candidate_count(3, 1) == 1);
}
