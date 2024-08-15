/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"

TEST_CASE("Block addr pack", "[block_pack]")
{
    WT_BLOCK b;
    WT_BM bm, *bmp;

    b.allocsize = 2;
    bmp = &bm;
    bmp->block = &b;

    /*
     * Address cookie 1
     * objectid 0
     * offset 0
     * size 0
     * checksum 0
     */
    SECTION("Pack address cookie 1")
    {
        uint8_t p, *pp;
        const uint8_t *begin;
        uint64_t checksum, offset, size;
        uint64_t expected_checksum, expected_offset, expected_size;
        pp = &p;
        expected_checksum = expected_offset = expected_size = 0;

        begin = (const uint8_t *)pp;
        REQUIRE(__wt_block_addr_pack(bmp->block, &pp, WT_TIERED_OBJECTID_NONE, 0, 0, 0) == 0);

        REQUIRE(__wt_vunpack_uint(&begin, 0, &offset) == 0);
        CHECK(offset == expected_offset);
        REQUIRE(__wt_vunpack_uint(&begin, 0, &size) == 0);
        CHECK(size == expected_size);
        REQUIRE(__wt_vunpack_uint(&begin, 0, &checksum) == 0);
        CHECK(checksum == expected_checksum);
    }

    /*
     * Address cookie 2
     * objectid 0
     * offset 1
     * size 0
     * checksum 1
     */
    SECTION("Pack address cookie 2")
    {
        uint8_t p, *pp;
        const uint8_t *begin;
        uint64_t checksum, offset, size;
        uint64_t expected_checksum, expected_offset, expected_size;
        pp = &p;
        expected_checksum = expected_offset = expected_size = 0;

        // Test that packing an address cookie of size 0 just packs 0 into all the fields.
        begin = (const uint8_t *)pp;
        REQUIRE(__wt_block_addr_pack(bmp->block, &pp, WT_TIERED_OBJECTID_NONE, 1, 0, 1) == 0);

        REQUIRE(__wt_vunpack_uint(&begin, 0, &offset) == 0);
        CHECK(offset == expected_offset);
        REQUIRE(__wt_vunpack_uint(&begin, 0, &size) == 0);
        CHECK(size == expected_size);
        REQUIRE(__wt_vunpack_uint(&begin, 0, &checksum) == 0);
        CHECK(checksum == expected_checksum);
    }

    /*
     * Address cookie 3
     * objectid 0
     * offset 10
     * size 4
     * checksum 12345
     */
    SECTION("Pack address cookie 3")
    {
        uint8_t p, *pp;
        const uint8_t *begin;
        uint64_t checksum, offset, size;
        uint64_t expected_checksum, expected_offset, expected_size;
        pp = &p;
        checksum = 12345;
        offset = 10;
        size = 4;
        expected_checksum = 12345;
        expected_offset = offset/b.allocsize - 1;
        expected_size = size/b.allocsize;

        // Test packing an address cookie with mostly non-zero fields.
        begin = (const uint8_t *)pp;
        REQUIRE(__wt_block_addr_pack(bmp->block, &pp, WT_TIERED_OBJECTID_NONE, offset, size, checksum) == 0);

        REQUIRE(__wt_vunpack_uint(&begin, 0, &offset) == 0);
        CHECK(offset == expected_offset);
        REQUIRE(__wt_vunpack_uint(&begin, 0, &size) == 0);
        CHECK(size == expected_size);
        REQUIRE(__wt_vunpack_uint(&begin, 0, &checksum) == 0);
        CHECK(checksum == expected_checksum);
    }
}

TEST_CASE("Block addr unpack", "[block_unpack]")
{
    WT_BLOCK b;
    WT_BM bm, *bmp;

    b.allocsize = 2;
    bmp = &bm;
    bmp->block = &b;

    SECTION("Unpack address cookie 1")
    {
        uint8_t p, *pp;
        uint32_t checksum, obj_id, size;
        uint32_t expected_checksum, expected_size;
        wt_off_t offset, expected_offset;

        pp = &p;
        expected_checksum = expected_offset = expected_size = 0;

        // Manually generate an address cookie.
        REQUIRE(__wt_vpack_uint(&pp, 0, expected_offset) == 0);
        REQUIRE(__wt_vpack_uint(&pp, 0, expected_size) == 0);
        REQUIRE(__wt_vpack_uint(&pp, 0, expected_checksum) == 0);

        // Check that the block manager unpack function generates the expected results.
        REQUIRE(__wt_block_addr_unpack(NULL, bmp->block, &p, 3, &obj_id, &offset, &size, &checksum) == 0); 
    }
    
    SECTION("Unpack address cookie 2")
    {
        uint8_t p, *pp;
        uint32_t checksum, obj_id, size;
        uint32_t expected_checksum, expected_size;
        wt_off_t offset, expected_offset;
        pp = &p;
        expected_checksum = 12345;
        expected_offset = 10; 
        expected_size = 4;

        // Manually generate an address cookie.
        REQUIRE(__wt_vpack_uint(&pp, 10, expected_offset) == 0);
        REQUIRE(__wt_vpack_uint(&pp, 4, expected_size) == 0);
        REQUIRE(__wt_vpack_uint(&pp, 12345, expected_checksum) == 0);

        // Check that the block manager unpack function generates the expected results.
        REQUIRE(__wt_block_addr_unpack(NULL, bmp->block, &p, 5, &obj_id, &offset, &size, &checksum) == 0); 
    }
}
