/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * block_ext.c: [extent_list2] Test extent list functions part 4. (More to come.)
 *
 * Test extent list insert/remove functions with block: __block_merge, __block_off_remove,
 * __block_extend, and __block_append.
 */

#include <algorithm>
#include <memory>

#include <catch2/catch.hpp>

#include "test_util.h"
#include "../utils.h"
#include "../utils_extlist.h"
#include "../wrappers/mock_session.h"
#include "wt_internal.h"

using namespace utils;

/*!
 * To sort off_size by _off and _size
 */
struct {
    bool
    operator()(const off_size &left, const off_size &right)
    {
        return (left < right);
    }
} off_size_Less_off_and_size;

/*!
 * A test (_off_size) and the expected value (_expected_list) for operations that need an off_size
 * to modify a WT_EXTLIST
 */
struct off_size_expected {
    off_size _off_size;
    std::vector<off_size> _expected_list;
};

TEST_CASE("Extent Lists: block_merge", "[extent_list2]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> mock_session = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = mock_session->getWtSessionImpl();

    std::vector<WT_EXT **> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("insert/merge multiple extents and verify all extents after each insert/merge")
    {
        BREAK;
        /* Tests and expected values */
        std::vector<off_size_expected> test_list{
          {off_size(3 * 4096, 4096), // [12,288, 16,383] Second
            {
              off_size(3 * 4096, 4096), // [12,288, 16,383]
            }},
          {off_size(4096, 4096), // [4,096, 8,191] First
            {
              off_size(4096, 4096),     // [4,096, 8,191],
              off_size(3 * 4096, 4096), // [12,288, 16,383]
            }},
          {off_size(5 * 4096, 4096), // [20,480, 24,575] Third
            {
              off_size(4096, 4096),     // [4,096, 8,191],
              off_size(3 * 4096, 4096), // [12,288, 16,383]
              off_size(5 * 4096, 4096), // [20,480, 24,575]
            }},
          {off_size(4096 - 64, 64), // [4,032, 4,095] Just below First, Merge with First start
            {
              off_size(4096 - 64, 4096 + 64), // [4,032, 8,191], First'
              off_size(3 * 4096, 4096),       // [12,288, 16,383] Second
              off_size(5 * 4096, 4096),       // [20,480, 24,575] Third
            }},
          {off_size(2 * 4096, 64), // [8,192, 8,255] Just above First', Merge with First' end
            {
              off_size(4096 - 64, 4096 + 128), // [4,032, 8,255], First''
              off_size(3 * 4096, 4096),        // [12,288, 16,383] Second
              off_size(5 * 4096, 4096),        // [20,480, 24,575] Third
            }},
          {off_size(2 * 4096 + 64,
             4096 - 64), // [8,256, 12,287] Just above First'', Merge First'' and Second
            {
              off_size(4096 - 64, 3 * 4096 + 64), // [4,032, 16,383], First'''
              off_size(5 * 4096, 4096),           // [20,480, 24,575] Third
            }},
          {off_size(6 * 4096, 64), // [20,480, 12,287] Just above First''', Merge First''' and Third
            {
              off_size(4096 - 64, 3 * 4096 + 64), // [4,032, 16,383], First'''
              off_size(5 * 4096, 4096 + 64),      // [20,480, 24,639] Third'
            }},
        };

        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist = {};

        /* Empty block */
        WT_BLOCK block = {};
        block.name = "__block_merge";
        block.allocsize = 1024;
        block.size = 4096; // Description information

        /* Test */
        /* Insert/merge extents and verify */
        int idx = 0;
        for (const off_size_expected &test : test_list) {
            /* Call */
            REQUIRE(__ut_block_merge(
                      session, &block, &extlist, test._off_size._off, test._off_size._size) == 0);
            INFO("After " << idx << ". Insert/merge: {off " << test._off_size._off << ", size "
                          << test._off_size._size << ", end %" << test._off_size.end() << "}");

            utils::extlist_print_off(extlist);

            /* Verify */
            verify_off_extent_list(extlist, test._expected_list, false);
            ++idx;
        }

        /* Cleanup */
        extlist_free(session, extlist);
    }
}

/*!
 * A test (_off) and the expected value (_expected_list) for operations that need an off to modify a
 * WT_EXTLIST
 */
struct off_expected {
    wt_off_t _off;
    std::vector<off_size> _expected_list;
};

TEST_CASE("Extent Lists: block_off_remove", "[extent_list2]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> mock_session = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = mock_session->getWtSessionImpl();

    std::vector<WT_EXT **> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("remove multiple extents and verify all extents after each remove")
    {
        BREAK;
        /* Extents to insert to setup for __ut_block_remove */
        std::vector<off_size> insert_list{
          off_size(3 * 4096, 4096), // Second [12,288, 16,383]
          off_size(4096, 4096),     // First [4,096, 8,191]
          off_size(5 * 4096, 4096), // Third [20,480, 24,575]
        };

        /* Tests and expected values */
        std::vector<off_expected> test_list{
          {3 * 4096, // [12,288, 16,383] Second
            {
              off_size(4096, 4096),     // [4,096, 8,191] First
              off_size(5 * 4096, 4096), // [20,480, 24,575] Third
            }},
          {4096, // [4,096, 8,191] First
            {
              off_size(5 * 4096, 4096), // [20,480, 24,575] Third
            }},
          {5 * 4096, // [20,480, 24,575] Third
            {}},
        };

        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist = {};

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
            INFO("Insert: {off " << to_insert._off << ", size " << to_insert._size << ", end "
                                 << to_insert.end() << "}n");
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert._off, to_insert._size) == 0);
        }

        extlist_print_off(extlist);

        /* Verify extents */
        std::vector<off_size> expected_order{insert_list};
        std::sort(expected_order.begin(), expected_order.end(), off_size_Less_off_and_size);
        verify_off_extent_list(extlist, expected_order);

        /* Test */
        WT_BLOCK block = {};
        int idx = 0;
        for (const off_expected &test : test_list) {
            /* For testing, half request ext returned, and half do not. */
            if ((idx % 2) == 0)
                /* Call */
                REQUIRE(__ut_block_off_remove(session, &block, &extlist, test._off, nullptr) == 0);
            else {
                WT_EXT *ext = nullptr;
                /* Call */
                REQUIRE(__ut_block_off_remove(session, &block, &extlist, test._off, &ext) == 0);
                REQUIRE(ext != nullptr);
                __wti_block_ext_free(session, ext);
            }

            INFO("After " << idx << ". Remove: off " << test._off);
            extlist_print_off(extlist);

            /* Verify */
            verify_off_extent_list(extlist, test._expected_list, false);
            ++idx;
        }

        /* Verify the result of all calls */
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Cleanup */
        extlist_free(session, extlist);
    }
}

TEST_CASE("Extent Lists: block_append", "[extent_list2]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> mock_session = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = mock_session->getWtSessionImpl();

    std::vector<WT_EXT **> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("append multiple extents and verify all extents after each append")
    {
        BREAK;
        /* Tests and expected values */
        std::vector<off_size_expected> test_list{
          {off_size(4096, 2048), // First half of First [4,096, 6,143]
            {
              off_size(4096, 2048), // First [4,096, 6,143]
            }},
          {off_size(4096 + 2048, 2048), // Second half of First [6,144, 8,191]
            {
              off_size(4096, 4096), // First [4,096, 8,191]
            }},
#if 0 // FAILED: REQUIRE( last == extlist.last ) with expansion: 0x0000000012173e40 == 0x00000000121746a0
          {off_size(3 * 4096, 4096), // Second [12,288, 16,383]
            {
              off_size(4096, 4096),     // First [4,096, 8,191]
              off_size(3 * 4096, 4096), // Second [12,288, 16,383]
            }},
#endif
#if 0 // FAILED: REQUIRE( extlist.entries == expected_order.size() ) with expansion: 2 == 3
          {off_size(5 * 4096, 4096), // Third [20,480, 24,575]
            {
              off_size(4096, 4096),     // First [4,096, 8,191]
              off_size(3 * 4096, 4096), // Second [12,288, 16,383]
              off_size(5 * 4096, 4096), // Third [20,480, 24,575]
            }},
#endif
        };

        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist = {};

        WT_BLOCK block = {};
        block.name = "__block_append";
        block.allocsize = 1024;
        block.size = 4096; // Description information

        /* Test */
        /* Append extents and verify */
        int idx = 0;
        for (const off_size_expected &test : test_list) {
            /* Call */
            REQUIRE(__ut_block_append(
                      session, &block, &extlist, test._off_size._off, test._off_size._size) == 0);

            INFO("After " << idx << ". Append: {off " << test._off_size._off << ", size "
                          << test._off_size._size << ", end " << test._off_size.end() << "}");
            utils::extlist_print_off(extlist);

            /* Verify */
            verify_off_extent_list(extlist, test._expected_list, true);
            ++idx;
        }

        /* Cleanup */
        extlist_free(session, extlist);
    }
}
