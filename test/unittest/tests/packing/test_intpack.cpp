/*-
* Copyright (c) 2014-present MongoDB, Inc.
* Copyright (c) 2008-2014 WiredTiger, Inc.
*	All rights reserved.
*
* See the file LICENSE for redistribution information.
*/

/*
* This file unit tests the macros and functions contained in intpack_inline.h
*/

#include "wt_internal.h"
#include <catch2/catch.hpp>

/*
 * wt_size_check_pack_wrapper --
 *    The WT_SIZE_CHECK_PACK() macro which will directly call return on failure.
 *    Creating a wrapper function thereby ensures that the macro's return call is restricted to
 *    this function's scope.
 */
static int wt_size_check_pack_wrapper(int value, size_t maxValue)
{
    WT_SIZE_CHECK_PACK(value, maxValue);
    return 0;
}

/*
 * wt_size_check_unpack_wrapper --
 *    The WT_SIZE_CHECK_UNPACK() macro which will directly call return on failure.
 *    Creating a wrapper function thereby ensures that the macro's return call is restricted to
 *    this function's scope.
 */
static int wt_size_check_unpack_wrapper(int value, size_t maxValue)
{
    WT_SIZE_CHECK_UNPACK(value, maxValue);
    return 0;
}

/*
 * wt_leading_zeros_wrapper --
 *    This function wraps WT_LEADING_ZEROS() to create a function that returns the
 *    number of leading zeros, rather than requiring a result variable to be passed in
 */
template<class T>
static int wt_leading_zeros_wrapper(T value)
{
    int result = 0;
    WT_LEADING_ZEROS(value, result);
    return result;
}

static void unpack_and_check(std::vector<uint8_t> const& packed, uint64_t expectedValue)
{
    uint8_t const *p = packed.data();
    uint64_t unpackedValue = 0;
    REQUIRE(__wt_vunpack_posint(&p, packed.size(), &unpackedValue) == 0);
    REQUIRE(unpackedValue == expectedValue);
}

TEST_CASE("Integer packing macros: byte min/max", "[intpack]")
{
    /*
     * These macros have no type, so assign macros into variables to give them a type
     */
    uint16_t neg_1byte_min_16 = NEG_1BYTE_MIN;
    uint16_t neg_2byte_min_16 = NEG_2BYTE_MIN;
    uint16_t pos_1byte_max_16 = POS_1BYTE_MAX;
    uint16_t pos_2byte_max_16 = POS_2BYTE_MAX;

    uint32_t neg_1byte_min_32 = NEG_1BYTE_MIN;
    uint32_t neg_2byte_min_32 = NEG_2BYTE_MIN;
    uint32_t pos_1byte_max_32 = POS_1BYTE_MAX;
    uint32_t pos_2byte_max_32 = POS_2BYTE_MAX;

    uint64_t neg_1byte_min_64 = NEG_1BYTE_MIN;
    uint64_t neg_2byte_min_64 = NEG_2BYTE_MIN;
    uint64_t pos_1byte_max_64 = POS_1BYTE_MAX;
    uint64_t pos_2byte_max_64 = POS_2BYTE_MAX;

    CHECK(neg_1byte_min_16 == 0xffc0u);
    CHECK(neg_2byte_min_16 == 0xdfc0u);
    CHECK(pos_1byte_max_16 == 0x003fu);
    CHECK(pos_2byte_max_16 == 0x203fu);

    CHECK(neg_1byte_min_32 == 0xffffffc0lu);
    CHECK(neg_2byte_min_32 == 0xffffdfc0lu);
    CHECK(pos_1byte_max_32 == 0x0000003flu);
    CHECK(pos_2byte_max_32 == 0x0000203flu);

    CHECK(neg_1byte_min_64 == 0xffffffffffffffc0llu);
    CHECK(neg_2byte_min_64 == 0xffffffffffffdfc0llu);
    CHECK(pos_1byte_max_64 == 0x000000000000003fllu);
    CHECK(pos_2byte_max_64 == 0x000000000000203fllu);
};


TEST_CASE("Integer packing macros: calculations", "[intpack]")
{
    REQUIRE(GET_BITS(0x01, 8, 0) == 0x1ll);

    CHECK(wt_size_check_pack_wrapper(100, 0) == 0);
    CHECK(wt_size_check_pack_wrapper(100, 256) == 0);
    CHECK(wt_size_check_pack_wrapper(100, 4) == ENOMEM);
    CHECK(wt_size_check_pack_wrapper(300, 8) == ENOMEM);

    CHECK(wt_size_check_unpack_wrapper(100, 0) == 0);
    CHECK(wt_size_check_unpack_wrapper(100, 256) == 0);
    CHECK(wt_size_check_unpack_wrapper(100, 4) == EINVAL);
    CHECK(wt_size_check_unpack_wrapper(300, 8) == EINVAL);

    CHECK(wt_leading_zeros_wrapper<uint64_t>(0) == sizeof(uint64_t));
    CHECK(wt_leading_zeros_wrapper<uint64_t>(0x1) == 7);
    CHECK(wt_leading_zeros_wrapper<uint64_t>(0x100) == 6);
    CHECK(wt_leading_zeros_wrapper<uint64_t>(0x1ff) == 6);
    CHECK(wt_leading_zeros_wrapper<uint64_t>(0x10100) == 5);
    CHECK(wt_leading_zeros_wrapper<uint64_t>(0x101ff) == 5);

    /*
     * WT_LEADING_ZEROS uses sizeof(type) if the value is 0,
     * but assumes uint64_t if non-zero, given odd results
     */
    CHECK(wt_leading_zeros_wrapper<uint8_t>(0) == sizeof(uint8_t));
    CHECK(wt_leading_zeros_wrapper<uint8_t>(0x1) == 7);
    CHECK(wt_leading_zeros_wrapper<uint32_t>(0) == sizeof(uint32_t));
    CHECK(wt_leading_zeros_wrapper<uint32_t>(0x1) == 7);
}


TEST_CASE("Integer packing functions: __wt_vpack_posint and __wt_vunpack_posint", "[intpack]")
{
    std::vector<uint8_t> packed(8, 0);

    SECTION("pack and unpack 7")
    {
        uint8_t *p = packed.data();
        uint64_t value = 7;
        REQUIRE(__wt_vpack_posint(&p, packed.size(), value) == 0);
        REQUIRE(packed[0] == 1);
        REQUIRE(packed[1] == 7);
        REQUIRE(packed[2] == 0);
        REQUIRE(packed[3] == 0);
        REQUIRE(packed[4] == 0);
        REQUIRE(packed[5] == 0);
        REQUIRE(packed[6] == 0);
        REQUIRE(packed[7] == 0);
        unpack_and_check(packed, value);
    }

    SECTION("pack and unpack  42")
    {
        uint8_t *p = packed.data();
        uint64_t value = 42;
        REQUIRE(__wt_vpack_posint(&p, packed.size(), value) == 0);
        REQUIRE(packed[0] == 1);
        REQUIRE(packed[1] == 42);
        REQUIRE(packed[2] == 0);
        REQUIRE(packed[3] == 0);
        REQUIRE(packed[4] == 0);
        REQUIRE(packed[5] == 0);
        REQUIRE(packed[6] == 0);
        REQUIRE(packed[7] == 0);
        unpack_and_check(packed, value);
    }

    SECTION("pack and unpack  0x1234")
    {
        uint8_t *p = packed.data();
        uint64_t value = 0x1234;
        REQUIRE(__wt_vpack_posint(&p, packed.size(), value) == 0);
        REQUIRE(packed[0] == 2);
        REQUIRE(packed[1] == 0x12);
        REQUIRE(packed[2] == 0x34);
        REQUIRE(packed[3] == 0);
        REQUIRE(packed[4] == 0);
        REQUIRE(packed[5] == 0);
        REQUIRE(packed[6] == 0);
        REQUIRE(packed[7] == 0);
        unpack_and_check(packed, value);
    }

    SECTION("pack and unpack  0x123456789")
    {
        uint8_t *p = packed.data();
        uint64_t value = 0x123456789;
        REQUIRE(__wt_vpack_posint(&p, 2, 0x123456789) == ENOMEM);
        REQUIRE(__wt_vpack_posint(&p, packed.size(), 0x123456789) == 0);
        REQUIRE(packed[0] == 5);
        REQUIRE(packed[1] == 0x01);
        REQUIRE(packed[2] == 0x23);
        REQUIRE(packed[3] == 0x45);
        REQUIRE(packed[4] == 0x67);
        REQUIRE(packed[5] == 0x89);
        REQUIRE(packed[6] == 0);
        REQUIRE(packed[7] == 0);
        unpack_and_check(packed, value);
    }
}