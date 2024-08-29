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
    WT_BLOCK_HEADER prev_from, prev_to;

    // Save the original values before any potential byte re-orderings.
    prev_from.disk_size = from->disk_size;
    prev_from.checksum = from->checksum;
    prev_to.disk_size = to->disk_size;
    prev_to.checksum = to->checksum;

#ifdef WORDS_BIGENDIAN
    __wt_block_header_byteswap_copy(from, to);
    REQUIRE(to->checksum == __wt_bswap32(prev_from.checksum));
    REQUIRE(to->disk_size == __wt_bswap32(prev_from.disk_size));
#else
    // Test that the byte orderings remain the same in both block headers.
    REQUIRE(from->disk_size == prev_from.disk_size);
    REQUIRE(from->checksum == prev_from.checksum);
    REQUIRE(to->disk_size == prev_to.disk_size);
    REQUIRE(to->checksum == prev_to.checksum);
#endif
}

TEST_CASE("Block header byteswap copy", "[block_other]")
{
    WT_BLOCK_HEADER from, to;

    SECTION("Test case 1")
    {
        from.disk_size = 12121;
        from.checksum = 24358;
        to.disk_size = to.checksum = 0;

        // Test using WiredTiger byte swap functions.
        test_block_header_byteswap_copy(&from, &to);

        // Test manually against known results.
#ifdef WORDS_BIGENDIAN
        // 12121 (59 2F 00 00) -> 1496252416 (00 00 2F 59).
        // 24358 (26 5F 00 00) -> 643760128 (00 00 5F 26).
        REQUIRE(to.disk_size == 1496252416);
        REQUIRE(to.checksum == 643760128);
#endif
    }

    SECTION("Test case 2")
    {
        from.disk_size = from.checksum = to.disk_size = to.checksum = 0;

        // Test all zero values using WiredTiger byte swap functions.
        test_block_header_byteswap_copy(&from, &to);

        // Test manually against known results.
#ifdef WORDS_BIGENDIAN
        REQUIRE(to.disk_size == 0);
        REQUIRE(to.checksum == 0);
#endif
    }

    SECTION("Test case 3")
    {
        from.disk_size = 28;
        from.checksum = 66666;
        to.disk_size = to.checksum = 0;

        // Test using WiredTiger byte swap functions.
        test_block_header_byteswap_copy(&from, &to);

        // Test manually against known results.
#ifdef WORDS_BIGENDIAN
        // 28 (00 00 00 1C) -> 469762048 (1C 00 00 00).
        // 66666 (00 01 04 6A) -> 1778647296 (6A 04 01 00).
        REQUIRE(to.disk_size == 469762048);
        REQUIRE(to.checksum == 1778647296);
#endif
    }
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
