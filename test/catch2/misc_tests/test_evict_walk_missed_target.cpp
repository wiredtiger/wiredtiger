/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"

/*
 * __wti_evict_walk_missed_target decides whether an eviction walk found too few pages and the tree
 * should therefore be walked less often. It is pure arithmetic, so it is tested directly rather
 * than through a live tree whose eviction server would race the test.
 */
TEST_CASE("Eviction walk backs off when it queues nothing for a one-page target", "[evict]")
{
    /* A walk that queued nothing must back off, however small the remaining target is. */
    REQUIRE(__wti_evict_walk_missed_target(0, 1));
    REQUIRE(__wti_evict_walk_missed_target(0, 2));
    REQUIRE(__wti_evict_walk_missed_target(0, 3));
    REQUIRE(__wti_evict_walk_missed_target(0, 100));
}

TEST_CASE("Eviction walk does not back off when it meets the target", "[evict]")
{
    /* Meeting the target in full is never a miss. */
    for (uint32_t target = 1; target <= 256; ++target)
        REQUIRE_FALSE(__wti_evict_walk_missed_target(target, target));

    /* Queuing more than asked for is not a miss either. */
    REQUIRE_FALSE(__wti_evict_walk_missed_target(2, 1));
}

TEST_CASE("Eviction walk backs off only below half of the target", "[evict]")
{
    /* Half the target, rounded up, is the boundary: strictly below it is a miss. */
    struct {
        uint64_t queued;
        uint32_t target;
        bool expected_miss;
    } cases[] = {
      {0, 4, true},          /* Nothing of four. */
      {1, 4, true},          /* One of four: below half. */
      {2, 4, false},         /* Exactly half of four. */
      {1, 3, true},          /* One of three: below half rounded up. */
      {2, 3, false},         /* Two of three. */
      {49, 100, true},       /* Just below half. */
      {50, 100, false},      /* Exactly half. */
      {0, UINT32_MAX, true}, /* The rounding must not overflow a 32-bit target. */
      {UINT32_MAX, UINT32_MAX, false},
    };

    for (auto &c : cases) {
        INFO("queued " << c.queued << " target " << c.target);
        REQUIRE(__wti_evict_walk_missed_target(c.queued, c.target) == c.expected_miss);
    }
}
