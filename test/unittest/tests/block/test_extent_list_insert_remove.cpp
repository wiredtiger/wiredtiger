/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * block_ext.c: [extent_list2] Test extent list insert/remove functions: __block_ext_insert,
 * __block_merge, __block_off_remove.
 */

#include <algorithm>
#include <memory>

#include <catch2/catch.hpp>

#include "test_util.h"
#include "../wrappers/mock_session.h"
#include "wt_internal.h"

extern void print_list(WT_EXT **head);

// Wrappers and Utilities
struct off_size {
    wt_off_t off;
    wt_off_t size;

    off_size(wt_off_t off = 0, wt_off_t size = 0)
        : off(off), size(size)
        {}
};

int operator<(const off_size & left, const off_size right) {
    return ((left.off < right.off) ||
            ((left.off == right.off) && (left.size < right.size)));
};

struct
{
    bool operator()(const off_size& left, const off_size& right) {
    return ((left.off < right.off) ||
            ((left.off == right.off) && (left.size < right.size)));
    }
} off_sizeLess;


WT_EXT *
alloc_new_ext(wt_off_t off = 0, wt_off_t size = 0)
{
    /*
     * Manually alloc enough extra space for the zero-length array to encode two skip lists.
     */
    constexpr auto sz = sizeof(WT_EXT) + 2 * WT_SKIP_MAXDEPTH * sizeof(WT_EXT *);

    auto raw = (WT_EXT *)malloc(sz);
    memset(raw, 0, sz);
    raw->off = off;
    raw->size = size;

    return raw;
}

WT_EXT *
alloc_new_ext(const off_size & one) {
    return alloc_new_ext(one.off, one.size);
}

void
break_here(const char * file, const char * func, int line) {
    printf(">> %s line %d: %s\n", file, line, func);
    fflush(stdout);
}
#define BREAK break_here(__FILE__, __func__, __LINE__)

// Test specific
/*
 * Verify an extent list is empty.
 */
void
verify_empty_extent_list(WT_EXT **head, WT_EXT ***stack)
{
    REQUIRE(__ut_block_off_srch_last(&head[0], &stack[0]) == nullptr);
    for (int i = 0; i < WT_SKIP_MAXDEPTH; i++) {
        REQUIRE(stack[i] == &head[i]);
    }
}

TEST_CASE("Extent Lists: block_ext_insert", "[extent_list2]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> mock_session = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = mock_session->getWtSessionImpl();

    std::vector<WT_EXT **> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("insert into an empty list has one element")
    {
        BREAK;
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert one extent */
        WT_EXT * first = alloc_new_ext(4096, 4096);
        REQUIRE(__ut_block_ext_insert(session, &extlist, first) == 0);

        print_list(extlist.off);
        fflush(stdout);

        /* Verify */
        REQUIRE(__ut_block_off_srch_last(&extlist.off[0], &stack[0]) == extlist.off[0]);
    }

    SECTION("insert multiple extents and retrieve in correct order")
    {
        BREAK;
        /* Extents to insert */
        std::vector<off_size> insert_list {
            off_size(3*4096, 4096),
            off_size(4096, 4096),
            off_size(5*4096, 4096),
        };
            
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert extents */
        for (const off_size & to_insert: insert_list) {
            WT_EXT * insert_ext = alloc_new_ext(to_insert);
            REQUIRE(__ut_block_ext_insert(session, &extlist, insert_ext) == 0);
        }

        print_list(extlist.off);
        fflush(stdout);

        /* Verify extents */
        std::vector<off_size> expected_order { insert_list };
        std::sort(expected_order.begin(), expected_order.end(), off_sizeLess);

        int idx = 0;
        for (const off_size & expected: expected_order) {
            __ut_block_off_srch(&extlist.off[0], expected.off, &stack[0], false);

            for (int i = 0; i < WT_SKIP_MAXDEPTH; i++)
                REQUIRE(stack[i] == &extlist.off[i]);

            if (extlist.off[0] != nullptr) {
                REQUIRE(extlist.off[0]->off == expected.off);
                REQUIRE(extlist.off[0]->size == expected.size);
            }
            ++idx;
        }
    }
}
