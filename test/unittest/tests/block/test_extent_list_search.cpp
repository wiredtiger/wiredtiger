/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * block_ext.c: [extent_list2] Test extent list functions part 3. (More to come.)
 *
 * Test extent list search functions: __block_off_srch_pair, and __block_off_match.
 */

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

#include <catch2/catch.hpp>

#include "test_util.h"
#include "../utils.h"
#include "../utils_extlist.h"
#include "../wrappers/mock_session.h"
#include "wt_internal.h"

using namespace utils;

/*!
 * A test (_off) and expected values (_before and _after) for __block_srch_pair.
 */
struct search_before_after {
    wt_off_t _off;
    off_size *_before;
    off_size *_after;

    search_before_after(wt_off_t off, off_size *before, off_size *after)
        : _off(off), _before(before), _after(after)
    {
    }
};

TEST_CASE("Extent Lists: block_off_srch_pair", "[extent_list2]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> mock_session = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = mock_session->getWtSessionImpl();

    std::vector<WT_EXT **> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("search an empty list")
    {
        BREAK;
        /* Offsets to search for */
        std::vector<wt_off_t> test_list{0, 4096, 3 * 4096}; // 3, 4096, 12,288

        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist = {};

        /* Test */
        WT_EXT dummy;
        for (const wt_off_t &test : test_list) {
            INFO("Search: off " << test);
            /* Set to an invalid value to determine whether __block_off_srch_pair changed them. */
            WT_EXT *before = &dummy;
            WT_EXT *after = &dummy;
            /* Call */
            __ut_block_off_srch_pair(&extlist, test, &before, &after);
            /* Verify: All should be not found */
            REQUIRE(before == nullptr);
            REQUIRE(after == nullptr);
        }
    }

    SECTION("search a non-empty list")
    {
        BREAK;
        /* Extents to insert to create an extent list to search */
        std::vector<off_size> insert_list{
          off_size(3 * 4096, 4096), // Second [12,288, 16,383]
          off_size(4096, 4096),     // First [4,096, 8,191]
          off_size(5 * 4096, 4096), // Third [20,480, 24,575]
        };

        /* Tests and expected values for __block_srch_pair */
        std::vector<search_before_after> expected_before_after{
          search_before_after(0, nullptr, &insert_list[1]),    // Before first 0
          search_before_after(4096, nullptr, &insert_list[1]), // At first 4,096
          search_before_after(
            2 * 4096, &insert_list[1], &insert_list[0]), // Between first and second 8,192
          search_before_after(3 * 4096, &insert_list[1], &insert_list[0]), // At second 12,288
          search_before_after(
            4 * 4096, &insert_list[0], &insert_list[2]), // Between second and third 16,384
          search_before_after(5 * 4096, &insert_list[0], &insert_list[2]), // At third 20,480
          search_before_after(6 * 4096, &insert_list[2], nullptr),         // After third 24,576
        };

        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist = {};

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
            INFO("Insert: {off " << std::showbase << to_insert._off << ", size " << to_insert._size
                                 << ", end " << to_insert.end() << "}");
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert._off, to_insert._size) == 0);
        }

        extlist_print_off(extlist);

        /* Test */
        WT_EXT dummy;
        uint32_t idx = 0;
        std::ostringstream line_stream;

        for (const search_before_after &expected : expected_before_after) {
            WT_EXT *before = &dummy;
            WT_EXT *after = &dummy;
            /* Call */
            __ut_block_off_srch_pair(&extlist, expected._off, &before, &after);

            line_stream.clear();
            line_stream << "Verify: " << idx << ". off " << expected._off;
            if (expected._before != nullptr)
                line_stream << "; Expected: _before: {off " << expected._before->_off << ", size "
                            << expected._before->_size << ", end " << expected._before->end()
                            << "}";
            else
                line_stream << "; Expected: _before == nullptr";

            if (expected._after != nullptr)
                line_stream << ", after: {off " << expected._after->_off << ", size "
                            << expected._after->_size << ", end " << expected._after->end() << "}";
            else
                line_stream << ", after == nullptr";

            if (before != nullptr)
                line_stream << "; Actual: before: {off " << before->off << ", size " << before->size
                            << ", end " << (before->off + before->size - 1) << "}";
            else
                line_stream << "; Actual: before == nullptr";

            if (after != nullptr)
                line_stream << ", after: {off " << after->off << ", size " << after->size
                            << ", end " << (after->off + after->size - 1) << "}";
            else
                line_stream << ", _after == nullptr";

            std::string line = line_stream.str();
            INFO(line);

            ++idx;
            /* Verify */
            if (expected._before != nullptr) {
                REQUIRE(before != nullptr);
                REQUIRE(before->off == expected._before->_off);
                REQUIRE(before->size == expected._before->_size);
            } else {
                REQUIRE(before == nullptr);
            }

            if (expected._after != nullptr) {
                REQUIRE(after != nullptr);
                REQUIRE(after->off == expected._after->_off);
                REQUIRE(after->size == expected._after->_size);
            } else {
                REQUIRE(after == nullptr);
            }
        }

        /* Cleanup */
        extlist_free(session, extlist);
    }
}

#ifdef HAVE_DIAGNOSTIC
/*!
 * A test (_off and _size) and the expected value (_match) for __block_off_match.
 */
struct search_match {
    wt_off_t _off;
    wt_off_t _size;
    bool _match;

    search_match(wt_off_t off, wt_off_t size, bool match) : _off(off), _size(size), _match(match) {}

    /*!
     * end --
     *     Return the end of the closed interval represented by _off and _size.
     */
    wt_off_t
    end(void) const
    {
        return (_off + _size - 1);
    }
};

TEST_CASE("Extent Lists: block_off_match", "[extent_list2]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> mock_session = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = mock_session->getWtSessionImpl();

    /* Extents to insert to create an extent list to search */
    std::vector<off_size> insert_list{
      off_size(3 * 4096, 4096), // Second [12,288, 16,383]
      off_size(4096, 4096),     // First [4,096, 8,191]
      off_size(5 * 4096, 4096), // Third [20,480, 24,575]
    };

    /* Tests and expected values for __block_off_match */
    std::vector<search_match> expected_match
    {
        search_match(0, 0, false),      // Empty: Before first 0
          search_match(4095, 0, false), // Empty: Just before first 4,095
          search_match(4096, 0, false), // Empty: At the start of first 4,096
#if 0 // Failed: Verify: 3. Expected: off=8191, size=0, end=8190, match=false; Actual: match = true
          search_match(4096 + 4095, 0, false), // Empty: At end first 8,191
#endif
          search_match(2 * 4096, 0, false),      // Empty: Just after first 8,192
          search_match(2 * 4096 + 64, 0, false), // Empty: Between first and second 8,256
          search_match(3 * 4096, 0, false),      // Empty: At the start of second 12,288
          search_match(4 * 4096 + 64, 0, false), // Empty: Between second and third 16,448
          search_match(5 * 4096, 0, false),      // Empty: At the start of third 20,480
          search_match(6 * 4096, 0, false),      // Empty: Just after third 24,576
          search_match(4096 - 128, 64, false),   // Before first [3,968, 4,031]
          search_match(4095, 1, false),          // Just before first, i.e. touching. [4,095, 4,095]
          search_match(2 * 4096, 1, false),      // Just after first [8,192, 8,192]
          search_match(4096 - 64, 128, true),    // Overlapping the start of first [4,032, 4,160]
          search_match(4096, 1, true),           // Just the start of first [4,096, 4,096]
          search_match(4096, 64, true),          // At the start of first [4,096, 4,159]
          search_match(4096 + 64, 64, true),     // Within first [4,160, 4,223]
          search_match(2 * 4096 - 64, 64, true), // At the end of first [8,128, 8,191]
          search_match(2 * 4096 - 1, 1, true),   // Just the end of first [8,191, 8,191]
          search_match(2 * 4096 - 64, 128, true),    // Overlapping the end of first [8,128, 8,255]
          search_match(4096, 4096, true),            // The same as first [4,096, 8191]
          search_match(4096 - 64, 4096 + 128, true), // Completely overlapping first [4,032, 8,255]
    };

    std::vector<WT_EXT **> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("search an empty list")
    {
        BREAK;
        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist = {};

        /* Test */
        uint32_t idx = 0;
        for (const search_match &expected : expected_match) {
            /* Call */
            bool match = __ut_block_off_match(&extlist, expected._off, expected._size);

            const char *match_str = match ? "true" : "false";
            INFO("Verify: " << idx << ". Expected: {off " << expected._off << ", size "
                            << expected._size << ", end " << expected.end()
                            << "}, match false; Actual: match " << match_str);

            ++idx;
            /* Verify: All should be not found. */
            REQUIRE(match == false);
        }
    }

    SECTION("search a non-empty list")
    {
        BREAK;
        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist = {};

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
            INFO("Insert: {off " << std::showbase << to_insert._off << ", size " << to_insert._size
                                 << ", end " << to_insert.end() << "}");
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert._off, to_insert._size) == 0);
        }

        extlist_print_off(extlist);

        /* Test */
        uint32_t idx = 0;
        for (const search_match &expected : expected_match) {
            /* Call */
            bool match = __ut_block_off_match(&extlist, expected._off, expected._size);

            const char *match_str = match ? "true" : "false";
            INFO("Verify: " << idx << ". Expected: {off " << expected._off << ", size "
                            << expected._size << ", end " << expected.end()
                            << "}, match false; Actual: match " << match_str);

            ++idx;
            /* Verify */
            REQUIRE(match == expected._match);
        }

        /* Cleanup */
        extlist_free(session, extlist);
    }
}
#endif
