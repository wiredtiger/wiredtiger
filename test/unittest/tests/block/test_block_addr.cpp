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

static void
unpack_int_and_check(std::vector<uint8_t> const &packed, int64_t expectedValue)
{
    uint8_t const *p = packed.data();
    int64_t unpackedValue = 0;
    REQUIRE(__wt_vunpack_int(&p, packed.size(), &unpackedValue) == 0);
    REQUIRE(unpackedValue == expectedValue);
}

static void
test_pack_and_unpack_ints(
  std::vector<int64_t> values, std::vector<std::vector<uint8_t>> &expectedPacked)
{
    std::vector<std::vector<uint8_t>> packed;
    packed.push_back({0, 0, 0, 0, 0, 0, 0, 0});
    packed.push_back({0, 0, 0, 0, 0, 0, 0, 0});
    packed.push_back({0, 0, 0, 0, 0, 0, 0, 0});
    uint8_t *p0 = packed[0].data();
    uint8_t *p1 = packed[1].data();
    uint8_t *p2 = packed[2].data();
    REQUIRE(__wt_vpack_int(&p0, packed[0].size(), values[0]) == 0);
    REQUIRE(__wt_vpack_int(&p1, packed[1].size(), values[1]) == 0);
    REQUIRE(__wt_vpack_int(&p2, packed[2].size(), values[2]) == 0);
    CHECK(packed[0] == expectedPacked[0]);
    CHECK(packed[1] == expectedPacked[1]);
    CHECK(packed[2] == expectedPacked[2]);
    unpack_int_and_check(packed[0], values[0]);
    unpack_int_and_check(packed[1], values[1]);
    unpack_int_and_check(packed[2], values[2]);
}

void
unpack_addr_cookie_and_check(const uint8_t *packed, uint32_t block_allocsize,
  wt_off_t expected_offset, uint32_t expected_size, uint32_t expected_checksum)
{
    uint32_t unpacked_size, unpacked_checksum;
    uint64_t o, s, c;
    wt_off_t unpacked_offset;
    c = o = s = 0;
    REQUIRE(__wt_vunpack_uint(&packed, 0, &o) == 0);
    REQUIRE(__wt_vunpack_uint(&packed, 0, &s) == 0);
    REQUIRE(__wt_vunpack_uint(&packed, 0, &c) == 0);

    unpacked_offset = (s > 0) ? (wt_off_t)(o + 1) * block_allocsize : 0;
    unpacked_size = (s > 0) ? (uint32_t)s * block_allocsize : 0;
    unpacked_checksum = (s > 0) ? (uint32_t)c : 0;

    CHECK(unpacked_offset == expected_offset);
    CHECK(unpacked_size == expected_size);
    CHECK(unpacked_checksum == expected_checksum);
}

TEST_CASE("Block addr pack and unpack", "[block_addr]")
{
    WT_BLOCK b;
    WT_BM bm, *bmp;

    b.allocsize = 1;
    bmp = &bm;
    bmp->block = &b;

    // Address cookie 1 object id 0 offset 0 size 0 checksum 0
    SECTION("Pack and unpack address cookie 1")
    {
        const uint8_t *begin;
        uint8_t p[WT_BTREE_MAX_ADDR_COOKIE], *pp;
        uint32_t checksum, obj_id, size;
        uint32_t expected_checksum, expected_size;
        size_t addr_size;
        wt_off_t offset, expected_offset;
        pp = p;
        expected_checksum = expected_size = 0;
        expected_offset = 0;

        // Test the block manager's pack function with an address cookie containing all zero fields.
        begin = (const uint8_t *)pp;
        REQUIRE(__wt_block_addr_pack(bmp->block, &pp, WT_TIERED_OBJECTID_NONE, 0, 0, 0) == 0);
        addr_size = WT_PTRDIFF(pp, begin);
        unpack_addr_cookie_and_check(
          begin, b.allocsize, expected_offset, expected_size, expected_checksum);

        // Test the block manager's unpack function.
        REQUIRE(__wt_block_addr_unpack(
                  NULL, bmp->block, begin, addr_size, &obj_id, &offset, &size, &checksum) == 0);
        CHECK(offset == expected_offset);
        CHECK(size == expected_size);
        CHECK(checksum == expected_checksum);
    }

    // Address cookie 2 object id 0 offset 1 size 0 checksum 1
    SECTION("Pack and unpack address cookie 2")
    {
        const uint8_t *begin;
        uint8_t p[WT_BTREE_MAX_ADDR_COOKIE], *pp;
        uint32_t checksum, obj_id, size;
        uint32_t expected_checksum, expected_size;
        size_t addr_size;
        wt_off_t offset, expected_offset;
        pp = p;
        expected_checksum = expected_size = 0;
        expected_offset = 0;

        // Test that packing an address cookie of size 0 just packs 0 into all the fields.
        begin = (const uint8_t *)pp;
        REQUIRE(__wt_block_addr_pack(bmp->block, &pp, WT_TIERED_OBJECTID_NONE, expected_offset,
                  expected_size, expected_checksum) == 0);
        addr_size = WT_PTRDIFF(pp, begin);
        unpack_addr_cookie_and_check(
          begin, b.allocsize, expected_offset, expected_size, expected_checksum);

        // Test the block manager's unpack function.
        REQUIRE(__wt_block_addr_unpack(
                  NULL, bmp->block, begin, addr_size, &obj_id, &offset, &size, &checksum) == 0);
        CHECK(offset == expected_offset);
        CHECK(size == expected_size);
        CHECK(checksum == expected_checksum);
    }

    // Address cookie 3 object id 0 offset 10 size 4 checksum 12345
    SECTION("Pack and unpack address cookie 3")
    {
        const uint8_t *begin;
        uint8_t p[WT_BTREE_MAX_ADDR_COOKIE], *pp;
        uint32_t checksum, obj_id, size;
        uint32_t expected_checksum, expected_size;
        size_t addr_size;
        wt_off_t offset, expected_offset;
        pp = p;
        expected_checksum = 12345;
        expected_offset = 10;
        expected_size = 4;

        // Test packing an address cookie with mostly non-zero fields.
        begin = (const uint8_t *)pp;
        REQUIRE(__wt_block_addr_pack(bmp->block, &pp, WT_TIERED_OBJECTID_NONE, expected_offset,
                  expected_size, expected_checksum) == 0);
        addr_size = WT_PTRDIFF(pp, begin);
        unpack_addr_cookie_and_check(
          begin, b.allocsize, expected_offset, expected_size, expected_checksum);

        // Test the block manager's unpack function.
        REQUIRE(__wt_block_addr_unpack(
                  NULL, bmp->block, begin, addr_size, &obj_id, &offset, &size, &checksum) == 0);
        CHECK(offset == expected_offset);
        CHECK(size == expected_size);
        CHECK(checksum == expected_checksum);
    }

    SECTION("Manually pack and unpack address cookie 4")
    {
        std::vector<int64_t> cookie_values = {7, 7, 42};
        std::vector<std::vector<uint8_t>> expected_packed_vals;
        expected_packed_vals.push_back({0x87, 0, 0, 0, 0, 0, 0, 0});
        expected_packed_vals.push_back({0x87, 0, 0, 0, 0, 0, 0, 0});
        expected_packed_vals.push_back({0xaa, 0, 0, 0, 0, 0, 0, 0});
        test_pack_and_unpack_ints(bmp->block, cookie_values, expected_packed_vals);
    }
}
