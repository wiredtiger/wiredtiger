/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Test the live restore bitmap filling bit range functionality.
 * [live_restore_bitmap_filling_bit_range].
 */

#include "../utils_live_restore.h"
#include "../../wrappers/mock_session.h"

using namespace utils;

struct test_data {
    test_data(uint32_t allocsize, uint64_t nbits, std::vector<std::pair<uint64_t, uint64_t>> ranges)
        : allocsize(allocsize), nbits(nbits), ranges(std::move(ranges))
    {
        bitmap_len = (nbits + 7) >> 3;
        bitmap = new uint8_t[bitmap_len];
        memset(bitmap, 0x0, bitmap_len);
    }

    uint32_t allocsize;
    uint64_t nbits;
    uint8_t *bitmap;
    uint64_t bitmap_len;
    // range.first represents filling offset, range.second represents filling length.
    std::vector<std::pair<uint64_t, uint64_t>> ranges;
};

static bool
is_bit_in_range(uint64_t bit_offset, const test_data &test)
{
    for (const auto &range : test.ranges) {
        uint64_t range_bit_start = range.first / test.allocsize;
        uint64_t range_bit_end = (range.first + range.second - 1) / test.allocsize;
        if (range_bit_start <= bit_offset && bit_offset <= range_bit_end)
            return true;
    }
    return false;
}

/*
 * Iterate through all bits in the bitmap. For each bit, check if it is as expected by verifying --
 * if the bit is set, its bit_offset must fall within one of the filling ranges.
 */
static bool
is_valid_bitmap(const test_data &test)
{
    for (uint64_t i = 0; i < test.bitmap_len; i++) {
        uint64_t bitmap_i = test.bitmap[i];
        for (uint64_t j = 0; j < 8; j++) {
            bool bit_set = bitmap_i & (1 << j);
            bool bit_in_range = is_bit_in_range((i << 3) | j, test);
            if (bit_set != bit_in_range)
                return false;
        }
    }
    return true;
}

TEST_CASE("Test various bitmap filling bit ranges",
  "[live_restore_bitmap], [live_restore_bitmap_filling_bit_range]")
{
    std::shared_ptr<mock_session> mock_session = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *session = mock_session->get_wt_session_impl();

    WTI_LIVE_RESTORE_FILE_HANDLE lr_fh;
    WT_CLEAR(lr_fh);
    // We need to have a non NULL pointer here for the encoding to take place.
    lr_fh.source = reinterpret_cast<WT_FILE_HANDLE *>(0xab);

    // Filling one range that fits within a single bit slot.
    test_data test1 = test_data(4, 16, {{16, 4}});
    // Filling one range that spans multiple bit slots and fits entirely within them.
    test_data test2 = test_data(4, 16, {{16, 16}});
    // Filling one range that partially overlaps a bit slot on the left.
    test_data test3 = test_data(4, 16, {{15, 5}});
    // Filling one range that partially overlaps a bit slot on the right.
    test_data test4 = test_data(4, 16, {{16, 5}});
    // Filling one range that partially overlaps multiple bit slots on both the left and right.
    test_data test5 = test_data(4, 16, {{13, 13}});
    // Filling one range that partially overlaps the last bit slot.
    test_data test6 = test_data(4, 16, {{63, 3}});
    // Filling one range that is not tracked by the bitmap.
    test_data test7 = test_data(4, 16, {{64, 4}});
    // Filling one range that fits the entire bitmap.
    test_data test8 = test_data(4, 16, {{0, 64}});
    // Filling one range that spans the entire bitmap and extends beyond the last slot.
    test_data test9 = test_data(4, 16, {{0, 81}});
    // Filling multiple ranges that each range fits within a bit slot.
    test_data test10 = test_data(4, 16, {{16, 4}, {24, 4}, {32, 4}});
    // Filling multiple ranges that each range partially overlaps some bit slots.
    test_data test11 = test_data(4, 16, {{15, 7}, {23, 8}, {36, 5}});
    // Filling multiple ranges that overlaps with each other.
    test_data test12 = test_data(4, 16, {{0, 7}, {5, 9}, {13, 15}});
    // Filling with some random allocsize, nbits, and ranges.
    test_data test13 = test_data(8, 128, {{3, 16}, {80, 50}, {96, 124}, {137, 169}, {17, 82}});
    test_data test14 = test_data(16, 64, {{0, 79}, {123, 40}, {172, 9}, {193, 17}, {196, 15}});
    test_data test15 = test_data(32, 256, {{3, 169}, {500, 500}, {876, 678}, {1135, 2321}});

    std::vector<test_data> tests = {test1, test2, test3, test4, test5, test6, test7, test8, test9,
      test10, test11, test12, test13, test14, test15};

    REQUIRE(__wt_rwlock_init(session, &lr_fh.lock) == 0);
    for (const auto &test : tests) {
        lr_fh.allocsize = test.allocsize;
        lr_fh.bitmap = test.bitmap;
        lr_fh.nbits = test.nbits;

        __wt_writelock(session, &lr_fh.lock);
        for (const auto &range : test.ranges)
            __ut_live_restore_fh_fill_bit_range(
              &lr_fh, session, (wt_off_t)range.first, (size_t)range.second);
        __wt_writeunlock(session, &lr_fh.lock);

        REQUIRE(is_valid_bitmap(test));

        __wt_free(session, lr_fh.bitmap);
    }
    __wt_rwlock_destroy(session, &lr_fh.lock);
}
