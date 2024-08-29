/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * [block_other]: block.h
 * This file unit tests the __wt_block_header_byteswap_copy and __wt_block_eligible_for_sweep
 * functions.
 */

#include "wt_internal.h"
#include <catch2/catch.hpp>

static void
test_block_header_byteswap_copy(WT_BLOCK_HEADER *from, WT_BLOCK_HEADER *to)
{
    WT_BLOCK_HEADER expected, prev_from, prev_to;

    // Save the original values before any potential byte re-orderings.
    prev_from.disk_size = from->disk_size;
    prev_from.checksum = from->checksum;
    prev_to.disk_size = to->disk_size;
    prev_to.checksum = to->checksum;

#ifdef WORDS_BIGENDIAN
    // Swap and copy the relevant block header bytes.
    expected.checksum = __wt_bswap32(prev_from.checksum);
    expected.disk_size = __wt_bswap32(prev_from.disk_size);

    // Test that the block manager's byteswap copy function yields the same results.
    __wt_block_header_byteswap_copy(from, to);
    REQUIRE(to->checksum == expected.checksum);
    REQUIRE(to->disk_size == expected.disk_size);
#else
    // Test that the byte orderings remain the same in both block headers.
    WT_UNUSED(expected);
    REQUIRE(from->disk_size == prev_from.disk_size);
    REQUIRE(from->checksum == prev_from.checksum);
    REQUIRE(to->disk_size == prev_to.disk_size);
    REQUIRE(to->checksum == prev_to.checksum);
#endif
}

TEST_CASE("Block header byteswap copy", "[block_other]")
{
    WT_BLOCK_HEADER from, to;
    from.disk_size = 12121;
    from.checksum = 24358;
    to.disk_size = 0;
    to.checksum = 0;

    test_block_header_byteswap_copy(&from, &to);
}

TEST_CASE("Block eligible for sweep", "[block_other]")
{
    WT_BLOCK block;
    WT_BM bm;

    SECTION("Block is local")
    {
        block.remote = false;
        block.objectid = 0;
        bm.max_flushed_objectid = 0;

        // Test that blocks that have been flushed are eligible for sweep.
        REQUIRE(__wt_block_eligible_for_sweep(&bm, &block) == true);

        // Test that blocks that haven't been flushed should not be eligible for sweep.
        block.objectid = 1;
        REQUIRE(__wt_block_eligible_for_sweep(&bm, &block) == false);
    }

    SECTION("Block is remote")
    {
        block.remote = true;
        block.objectid = 0;
        bm.max_flushed_objectid = 0;

        // Only local blocks need to be swept.
        REQUIRE(__wt_block_eligible_for_sweep(&bm, &block) == false);
    }
}
