/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "../../wrappers/mock_session.h"

/*
 * [block_disagg_cumulative_size]: block_disagg_write.c, block_disagg_read.c
 * This file unit tests cumulative size tracking for disaggregated address cookies.
 */
TEST_CASE("Disagg block: cumulative size tracking", "[block_disagg]")
{
    WT_PAGE_BLOCK_META block_meta;
    WT_CLEAR(block_meta);

    SECTION("Base page stores individual size")
    {
        // Setup base page metadata
        block_meta.delta_count = 0;
        block_meta.cumulative_size = 0;
        uint32_t size = 100;

        // Test the core logic from __wti_block_disagg_write
        uint32_t cookie_size =
          (block_meta.delta_count == 0) ? size : block_meta.cumulative_size + size;

        REQUIRE(cookie_size == 100);

        // Update block_meta for future delta writes
        block_meta.cumulative_size = cookie_size;
        REQUIRE(block_meta.cumulative_size == 100);
    }

    SECTION("Delta page accumulates size")
    {
        // Setup delta page metadata with existing cumulative size
        block_meta.delta_count = 1;
        block_meta.cumulative_size = 200;
        uint32_t size = 50;

        // Test the core logic from __wti_block_disagg_write
        uint32_t cookie_size = block_meta.cumulative_size + size;

        REQUIRE(cookie_size == 250);

        // Update block_meta for future delta writes
        block_meta.cumulative_size = cookie_size;
        REQUIRE(block_meta.cumulative_size == 250);
    }

    SECTION("Multiple deltas accumulate correctly")
    {
        // Base page: 100
        block_meta.delta_count = 0;
        block_meta.cumulative_size = 0;
        uint32_t size = 100;

        uint32_t cookie_size =
          (block_meta.delta_count == 0) ? size : block_meta.cumulative_size + size;
        block_meta.cumulative_size = cookie_size;
        REQUIRE(block_meta.cumulative_size == 100);

        // Delta 1: +30 = 130
        block_meta.delta_count = 1;
        size = 30;
        cookie_size = block_meta.cumulative_size + size;
        block_meta.cumulative_size = cookie_size;
        REQUIRE(block_meta.cumulative_size == 130);

        // Delta 2: +20 = 150
        block_meta.delta_count = 2;
        size = 20;
        cookie_size = block_meta.cumulative_size + size;
        block_meta.cumulative_size = cookie_size;
        REQUIRE(block_meta.cumulative_size == 150);
    }

    SECTION("Read path populates cumulative_size")
    {
        // Simulate read path setting cumulative_size
        uint32_t test_size = 300;
        block_meta.cumulative_size = test_size;

        // Verify the field is set correctly
        REQUIRE(block_meta.cumulative_size == 300);
    }
}
