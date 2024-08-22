/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

// This file unit tests block manager functions relating to packing and unpacking address cookies.

#include "wt_internal.h"
#include <catch2/catch.hpp>
#include <iostream>
#include <vector>

void
unpack_addr_cookie_and_check(const uint8_t *packed, uint32_t block_allocsize, wt_off_t pack_offset,
  uint32_t pack_size, uint32_t pack_checksum)
{
    uint64_t o = 0, s = 0, c = 0;
    REQUIRE(__wt_vunpack_uint(&packed, 0, &o) == 0);
    REQUIRE(__wt_vunpack_uint(&packed, 0, &s) == 0);
    REQUIRE(__wt_vunpack_uint(&packed, 0, &c) == 0);

    // Adjust the unpacked values as the block manager also does this to avoid storing large
    // offsets.
    wt_off_t unpacked_offset = (s == 0) ? (wt_off_t)o : (wt_off_t)(o + 1) * block_allocsize;
    uint32_t unpacked_size = (s == 0) ? (uint32_t)s : (uint32_t)s * block_allocsize;
    uint32_t unpacked_checksum = (uint32_t)c;

    if (pack_size != 0) {
        CHECK(unpacked_offset == pack_offset);
        CHECK(unpacked_size == pack_size);
        CHECK(unpacked_checksum == pack_checksum);
    } else {
        CHECK(unpacked_offset == 0);
        CHECK(unpacked_size == 0);
        CHECK(unpacked_checksum == 0);
    }
}

// Test the block manager's pack function.
static void
test_pack_addr_cookie(uint8_t *pp, WT_BLOCK *block, size_t *addr_size, wt_off_t pack_offset,
  uint32_t pack_size, uint32_t pack_checksum)
{
    const uint8_t *begin = (const uint8_t *)pp;
    REQUIRE(__wt_block_addr_pack(
              block, &pp, WT_TIERED_OBJECTID_NONE, pack_offset, pack_size, pack_checksum) == 0);
    *addr_size = WT_PTRDIFF(pp, begin);
    unpack_addr_cookie_and_check(begin, block->allocsize, pack_offset, pack_size, pack_checksum);
}

// Test the block manager's unpack function.
static void
test_unpack_addr_cookie(const uint8_t *begin, WT_BLOCK *block, size_t addr_size,
  wt_off_t pack_offset, uint32_t pack_size, uint32_t pack_checksum)
{
    uint32_t checksum, obj_id, size;
    wt_off_t offset;
    REQUIRE(__wt_block_addr_unpack(
              NULL, block, begin, addr_size, &obj_id, &offset, &size, &checksum) == 0);
    if (size != 0) {
        CHECK(offset == pack_offset);
        CHECK(size == pack_size);
        CHECK(checksum == pack_checksum);
    } else {
        CHECK(offset == 0);
        CHECK(size == 0);
        CHECK(checksum == 0);
    }
}

static void
test_pack_and_unpack_addr_cookie(
  WT_BLOCK *block, wt_off_t pack_offset, uint32_t pack_size, uint32_t pack_checksum)
{
    uint8_t p[WT_BTREE_MAX_ADDR_COOKIE], *pp;
    pp = p;

    const uint8_t *begin = (const uint8_t *)pp;
    size_t addr_size;

    test_pack_addr_cookie(pp, block, &addr_size, pack_offset, pack_size, pack_checksum);
    test_unpack_addr_cookie(begin, block, addr_size, pack_offset, pack_size, pack_checksum);
}

static void
test_pack_and_unpack_addr_cookie_manual(
  WT_BLOCK *block, std::vector<int64_t> cookie_vals, std::vector<uint8_t> &expected_packed_vals)
{
    std::vector<uint8_t> packed(24, 0);
    uint8_t *p = packed.data();
    const uint8_t *begin = (const uint8_t *)p;
    REQUIRE(__wt_block_addr_pack(block, &p, WT_TIERED_OBJECTID_NONE, (wt_off_t)cookie_vals[0],
              (uint32_t)cookie_vals[1], (uint32_t)cookie_vals[2]) == 0);
    CHECK(packed[0] == expected_packed_vals[0]);
    CHECK(packed[1] == expected_packed_vals[1]);
    CHECK(packed[2] == expected_packed_vals[2]);

    unpack_addr_cookie_and_check(begin, block->allocsize, (wt_off_t)cookie_vals[0],
      (uint32_t)cookie_vals[1], (uint32_t)cookie_vals[2]);
}

TEST_CASE("Block addr pack and unpack", "[block_addr]")
{
    WT_BLOCK b;
    WT_BM bm, *bmp;

    b.allocsize = 1;
    bmp = &bm;
    bmp->block = &b;

    // Test the block manager's pack function with an address cookie containing all zero fields.
    SECTION("Pack and unpack address cookie 1")
    {
        uint32_t pack_checksum = 0, pack_size = 0;
        wt_off_t pack_offset = 0;

        test_pack_and_unpack_addr_cookie(bmp->block, pack_offset, pack_size, pack_checksum);
    }

    /*
     * Test that packing an address cookie of size 0 just packs 0 into all the fields. The pack
     * values will differ from the expected values (which are all 0), and the function checks for
     * this.
     */
    SECTION("Pack and unpack address cookie 2")
    {
        uint32_t pack_checksum = 1, pack_size = 0;
        wt_off_t pack_offset = 1;

        test_pack_and_unpack_addr_cookie(bmp->block, pack_offset, pack_size, pack_checksum);
    }

    // Test packing an address cookie with mostly non-zero fields.
    SECTION("Pack and unpack address cookie 3")
    {
        uint32_t pack_checksum = 12345, pack_size = 4;
        wt_off_t pack_offset = 10;

        test_pack_and_unpack_addr_cookie(bmp->block, pack_offset, pack_size, pack_checksum);
    }

    // Test the block manager's packing function against hardcoded values rather than relying on
    // the integer pack function.
    SECTION("Manually pack and unpack address cookie 4")
    {
        /*
         * The block manager will modify these values due to its logic that accounts for large
         * offsets. The address cookie values that will actually be packed with an allocsize of 1:
         * {7, 7, 42}.
         */
        std::vector<int64_t> cookie_vals = {8, 7, 42};
        std::vector<uint8_t> expected_packed_vals = {0x87, 0x87, 0xaa};
        test_pack_and_unpack_addr_cookie_manual(bmp->block, cookie_vals, expected_packed_vals);
    }

    // Test that trying to pack an address cookie with negative values exhibits weird behavior.
    SECTION("Pack and unpack address cookie with negative values")
    {
        std::vector<int64_t> cookie_vals = {-6, -42, -256};
        std::vector<uint8_t> expected_packed_vals = {0x79, 0x56, 0x3f, 0x40};

        std::vector<uint8_t> packed(24, 0);
        uint8_t *p = packed.data();
        REQUIRE(
          __wt_block_addr_pack(bmp->block, &p, WT_TIERED_OBJECTID_NONE, (wt_off_t)cookie_vals[0],
            (uint32_t)cookie_vals[1], (uint32_t)cookie_vals[2]) == 0);
        CHECK(packed[0] != expected_packed_vals[0]);
        CHECK(packed[1] != expected_packed_vals[1]);
        CHECK(packed[2] != expected_packed_vals[2]);
    }
}
