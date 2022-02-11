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

static int wt_size_check_pack_wrapper(int value, size_t maxValue)
{
    WT_SIZE_CHECK_PACK(value, maxValue);
    return 0;
}

static int wt_size_check_unpack_wrapper(int value, size_t maxValue)
{
    WT_SIZE_CHECK_UNPACK(value, maxValue);
    return 0;
}

template<class T>
static int wt_leading_zeros_wrapper(T value)
{
    int result = 0;
    WT_LEADING_ZEROS(value, result);
    return result;
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
