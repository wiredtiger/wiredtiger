/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * block_ext.c: [extent_list2] Test extent list functions part 2. (More to come.)
 *
 * Test insert functions without block: __block_ext_insert, and __block_off_insert.
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

TEST_CASE("Extent Lists: block_ext_insert", "[extent_list2]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> mock_session = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = mock_session->getWtSessionImpl();

    std::vector<WT_EXT **> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("insert into an empty list has one element")
    {
        BREAK;
        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Test */
        /* Insert one extent */
        WT_EXT *first = alloc_new_ext(session, 4096, 4096);
        /* Call */
        REQUIRE(__ut_block_ext_insert(session, &extlist, first) == 0);

#ifdef DEBUG
        extlist_print_off(extlist);
        fflush(stdout);
#endif

        /* Verify */
        REQUIRE(__ut_block_off_srch_last(&extlist.off[0], &stack[0]) == extlist.off[0]);

        /* Cleanup */
        extlist_free(session, extlist);
    }

    SECTION("insert multiple extents and retrieve in correct order")
    {
        BREAK;
        /* Extents to insert */
        std::vector<off_size> insert_list{
          off_size(3 * 4096, 4096), // Second [12,288, 16,383]
          off_size(4096, 4096),     // First [4,096, 8,191]
          off_size(5 * 4096, 4096), // Third [20,480, 24,575]
        };

        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Test */
        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
            INFO("Insert: {off " << std::showbase << to_insert._off << ", size " << to_insert._size
                                 << ", end " << to_insert.end() << "}");
            WT_EXT *insert_ext = alloc_new_ext(session, to_insert);
            /* Call */
            REQUIRE(__ut_block_ext_insert(session, &extlist, insert_ext) == 0);
        }

#ifdef DEBUG
        extlist_print_off(extlist);
        fflush(stdout);
#endif

        /* Verify the result of all calls */
        std::vector<off_size> expected_order{insert_list};
        std::sort(expected_order.begin(), expected_order.end(), off_size_Less_off_and_size);
        verify_off_extent_list(extlist, expected_order);

        /* Cleanup */
        extlist_free(session, extlist);
    }
}

TEST_CASE("Extent Lists: block_off_insert", "[extent_list2]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> mock_session = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = mock_session->getWtSessionImpl();

    std::vector<WT_EXT **> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("insert into an empty list has one element")
    {
        BREAK;
        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Test */
        /* Insert one extent */
        /* Call */
        REQUIRE(__ut_block_off_insert(session, &extlist, 4096, 4096) == 0);

#ifdef DEBUG
        extlist_print_off(extlist);
        fflush(stdout);
#endif

        /* Verify */
        REQUIRE(__ut_block_off_srch_last(&extlist.off[0], &stack[0]) == extlist.off[0]);

        /* Cleanup */
        extlist_free(session, extlist);
    }

    SECTION("insert multiple extents and retrieve in correct order")
    {
        BREAK;
        /* Extents to insert */
        std::vector<off_size> insert_list{
          off_size(3 * 4096, 4096), // Second [12,288, 16,383]
          off_size(4096, 4096),     // First [4,096, 8,191]
          off_size(5 * 4096, 4096), // Third [20,480, 24,575]
        };

        /* Setup */
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Test */
        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
            INFO("Insert: {off " << std::showbase << to_insert._off << ", size " << to_insert._size
                                 << ", end " << to_insert.end() << "}");
            /* Call */
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert._off, to_insert._size) == 0);
        }

#ifdef DEBUG
        extlist_print_off(extlist);
        fflush(stdout);
#endif

        /* Verify the result of all calls */
        std::vector<off_size> expected_order{insert_list};
        std::sort(expected_order.begin(), expected_order.end(), off_size_Less_off_and_size);
        verify_off_extent_list(extlist, expected_order);

        /* Cleanup */
        extlist_free(session, extlist);
    }
}
