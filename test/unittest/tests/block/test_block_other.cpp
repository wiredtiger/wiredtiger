/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * This file unit tests the following block manager functions:
 *    - __wt_page_header_byteswap
 *    - __wt_block_header_byteswap_copy
 *    _ __wt_block_eligible_for_sweep
 */

#include "wt_internal.h"
#include <catch2/catch.hpp>

uint16_t
swap_bytes16(uint16_t i)
{
    // byte 2 | byte1.
    uint16_t byte1 = i & 0x000000ff;
    uint16_t byte2 = (i & 0x0000ff00) >> 8;

    // byte 1 | byte2.
    return (byte1 << 8 | byte2);
}

uint32_t
swap_bytes32(uint32_t i)
{
    // byte4 | byte3 | byte2 | byte1.
    uint32_t byte1 = i & 0x000000ff;
    uint32_t byte2 = (i & 0x0000ff00) >> 8;
    uint32_t byte3 = (i & 0x00ff0000) >> 16;
    uint32_t byte4 = (i & 0xff000000) >> 24;

    // byte 1 | byte2 | byte3 | byte4.
    return (byte1 << 24 | byte2 << 16 | byte3 << 8 | byte4);
}

uint64_t
swap_bytes64(uint64_t i)
{
    // byte8 | byte7 | byte6 | byte5 | byte4 | byte3 | byte2 | byte1.
    uint64_t byte1 = i & 0x00000000000000ffUL;
    uint64_t byte2 = i & 0x000000000000ff00UL;
    uint64_t byte3 = i & 0x0000000000ff0000UL;
    uint64_t byte4 = i & 0x00000000ff000000UL;
    uint64_t byte5 = i & 0x000000ff00000000UL;
    uint64_t byte6 = i & 0x0000ff0000000000UL;
    uint64_t byte7 = i & 0x00ff000000000000UL;
    uint64_t byte8 = i & 0xff00000000000000UL;

    // byte1 | byte2 | byte3 | byte4 | byte5 | byte6 | byte7 | byte8.
    return (byte1 << 56 | byte2 << 40 | byte3 << 24 | byte4 << 8 | byte5 >> 16 | byte6 >> 24 |
      byte7 >> 40 | byte8 >> 56);
}

static void
test_block_header_byteswap_copy(WT_BLOCK_HEADER *from)
{
    WT_BLOCK_HEADER expected, to;

#ifdef WORDS_BIGENDIAN
    // Swap and copy the relevant block header bytes.
    expected.checksum = swap_bytes32(from->checksum);
    expected.disk_size = swap_bytes32(from->disk_size);

    // Test that the block manager's byteswap copy function yields the same results.
    __wt_block_header_byteswap_copy(from, &to);
    REQUIRE(to.checksum == expected.checksum);
    REQUIRE(to.disk_size == expected.disk_size);
#else
    WT_UNUSED(expected);
    WT_UNUSED(from);
    WT_UNUSED(to);
#endif
}

TEST_CASE("Block header byteswap copy", "[block_other]")
{
    WT_BLOCK_HEADER from;

    from.disk_size = 12121;
    from.checksum = 24358;

    test_block_header_byteswap_copy(&from);
}

TEST_CASE("Page header byteswap", "[block_other]")
{
    WT_PAGE_HEADER dsk, expected;
    dsk.recno = 123456;
    dsk.write_gen = 666;
    dsk.mem_size = 100;
    dsk.u.entries = 88;

    // Test that the block manager's page header byteswap function yields the same results.
#ifdef WORDS_BIGENDIAN
    // Swap the bytes in the page header's fields.
    expected.recno = swap_bytes64(123456);
    expected.write_gen = swap_bytes64(666);
    expected.mem_size = swap_bytes32(100);
    expected.u.entries = swap_bytes32(88);

    __wt_page_header_byteswap(&dsk);
    REQUIRE(dsk.recno == expected.recno);
    REQUIRE(dsk.write_gen == expected.write_gen);
    REQUIRE(dsk.mem_size == expected.mem_size);
    REQUIRE(dsk.u.entries == expected.u.entries);
#else
    WT_UNUSED(dsk);
    WT_UNUSED(expected);
#endif
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
        REQUIRE(__wt_block_eligible_for_sweep(&bm, &block) == true);

        block.objectid = 1;
        REQUIRE(__wt_block_eligible_for_sweep(&bm, &block) == false);
    }

    SECTION("Block is non-local")
    {
        block.remote = true;
        block.objectid = 0;
        bm.max_flushed_objectid = 0;
        REQUIRE(__wt_block_eligible_for_sweep(&bm, &block) == false);
    }
}
