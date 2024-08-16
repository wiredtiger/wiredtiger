/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * block_ext.c: [extent_list2] Test extent list insert/remove functions: __block_ext_insert,
 * __block_off_insert,
 * __block_merge, and __block_off_remove. Test extent list search functions: __block_off_srch_pair,
 * and __block_off_match. Not yet: __wti_block_alloc, __block_extend, and __block_append.
 */

/* Choose one */
#define DEBUG /* Print debugging output */
//#undef DEBUG

#include <algorithm>
#include <memory>

#include <catch2/catch.hpp>

#include "test_util.h"
#include "../wrappers/mock_session.h"
#include "wt_internal.h"

extern void print_list(WT_EXT **head);

// Wrappers and Utilities
// For expected values
struct off_size {
    wt_off_t off;
    wt_off_t size;

    off_size(wt_off_t off = 0, wt_off_t size = 0) : off(off), size(size) {}
};

// To sort off_size by off and size.
struct {
    bool
    operator()(const off_size &left, const off_size &right)
    {
        return ((left.off < right.off) || ((left.off == right.off) && (left.size < right.size)));
    }
} off_size_Less_off_and_size;

#if 0
// To sort off_size by just off
struct
{
    bool operator()(const off_size& left, const off_size& right) {
    return (left.off < right.off);
    }
} off_size_Less_off;
#endif

// Allocate WT_EXT
WT_EXT *
alloc_new_ext(WT_SESSION_IMPL *session, wt_off_t off = 0, wt_off_t size = 0)
{
    WT_EXT *ext;
    REQUIRE(__wti_block_ext_alloc(session, &ext) == 0);
    ext->off = off;
    ext->size = size;

#ifdef DEBUG
    printf("Allocated WT_EXT %p: off=%" PRId64 ", size=%" PRId64 ", depth=%" PRIu8 ", next[0]=%p\n",
      ext, ext->off, ext->size, ext->depth, ext->next[0]);
    fflush(stdout);
#endif

    return ext;
}

WT_EXT *
alloc_new_ext(WT_SESSION_IMPL *session, const off_size &one)
{
    return alloc_new_ext(session, one.off, one.size);
}

// WT_EXTLIST operations
WT_EXT *
get_off_n(const WT_EXTLIST &extlist, uint32_t idx)
{
    REQUIRE(idx < extlist.entries);
    if ((extlist.last != nullptr) && (idx == (extlist.entries - 1)))
        return extlist.last;
    return extlist.off[idx];
}

void
verify_off_extent_list(const WT_EXTLIST &extlist, const std::vector<off_size> &expected_order)
{
    REQUIRE(extlist.entries == expected_order.size());
    uint32_t idx = 0;
    for (const off_size &expected : expected_order) {
        WT_EXT *ext = get_off_n(extlist, idx);
#ifdef DEBUG
        printf("Verify: %" PRIu32 ". Expected: off=%" PRId64 ", size=%" PRId64
               "; Actual: off=%" PRId64 ", size=%" PRId64 "\n",
          idx, expected.off, expected.size, ext->off, ext->size);
        fflush(stdout);
#endif
        REQUIRE(ext->off == expected.off);
        REQUIRE(ext->size == expected.size);
        ++idx;
    }
}

// break break_here for debugging.
void
break_here(const char *file, const char *func, int line)
{
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
        WT_EXT *first = alloc_new_ext(session, 4096, 4096);
        REQUIRE(__ut_block_ext_insert(session, &extlist, first) == 0);

#ifdef DEBUG
        print_list(extlist.off);
        fflush(stdout);
#endif

        /* Verify */
        REQUIRE(__ut_block_off_srch_last(&extlist.off[0], &stack[0]) == extlist.off[0]);
    }

    SECTION("insert multiple extents and retrieve in correct order")
    {
        BREAK;
        /* Extents to insert */
        std::vector<off_size> insert_list{
          off_size(3 * 4096, 4096),
          off_size(4096, 4096),
          off_size(5 * 4096, 4096),
        };

        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
#ifdef DEBUG
            printf("Insert: off=%" PRId64 ", size=%" PRId64 "\n", to_insert.off, to_insert.size);
            fflush(stdout);
#endif
            WT_EXT *insert_ext = alloc_new_ext(session, to_insert);
            REQUIRE(__ut_block_ext_insert(session, &extlist, insert_ext) == 0);
        }

#ifdef DEBUG
        print_list(extlist.off);
        fflush(stdout);
#endif

        /* Verify extents */
        std::vector<off_size> expected_order{insert_list};
        std::sort(expected_order.begin(), expected_order.end(), off_size_Less_off_and_size);
        verify_off_extent_list(extlist, expected_order);
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
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert one extent */
        REQUIRE(__ut_block_off_insert(session, &extlist, 4096, 4096) == 0);

#ifdef DEBUG
        print_list(extlist.off);
        fflush(stdout);
#endif

        /* Verify */
        REQUIRE(__ut_block_off_srch_last(&extlist.off[0], &stack[0]) == extlist.off[0]);
    }

    SECTION("insert multiple extents and retrieve in correct order")
    {
        BREAK;
        /* Extents to insert */
        std::vector<off_size> insert_list{
          off_size(3 * 4096, 4096),
          off_size(4096, 4096),
          off_size(5 * 4096, 4096),
        };

        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
#ifdef DEBUG
            printf("Insert: off=%" PRId64 ", size=%" PRId64 "\n", to_insert.off, to_insert.size);
            fflush(stdout);
#endif
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert.off, to_insert.size) == 0);
        }

#ifdef DEBUG
        print_list(extlist.off);
        fflush(stdout);
#endif

        /* Verify extents */
        std::vector<off_size> expected_order{insert_list};
        std::sort(expected_order.begin(), expected_order.end(), off_size_Less_off_and_size);
        verify_off_extent_list(extlist, expected_order);
    }
}

/* Expected before/after */
struct search_before_after {
    wt_off_t off;
    off_size *before;
    off_size *after;

    search_before_after(wt_off_t off, off_size *before, off_size *after)
        : off(off), before(before), after(after)
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
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Verify */
        std::vector<wt_off_t> expected_list{0, 4096, 3 * 4096};
        WT_EXT dummy;
        for (const wt_off_t &expected : expected_list) {
#ifdef DEBUG
            printf("Search: off=%" PRId64 "\n", expected);
            fflush(stdout);
#endif
            WT_EXT *before = &dummy;
            WT_EXT *after = &dummy;
            __ut_block_off_srch_pair(&extlist, expected, &before, &after);
            REQUIRE(before == nullptr);
            REQUIRE(after == nullptr);
        }
    }

    SECTION("search a non-empty list")
    {
        BREAK;
        /* Extents to insert */
        std::vector<off_size> insert_list{
          off_size(3 * 4096, 4096), // Second
          off_size(4096, 4096),     // First
          off_size(5 * 4096, 4096), // Third
        };

        std::vector<search_before_after> expected_before_after{
          search_before_after(0, nullptr, &insert_list[1]),    // Before first
          search_before_after(4096, nullptr, &insert_list[1]), // At first
          search_before_after(
            2 * 4096, &insert_list[1], &insert_list[0]), // Between first and second
          search_before_after(3 * 4096, &insert_list[1], &insert_list[0]), // At second
          search_before_after(
            4 * 4096, &insert_list[0], &insert_list[2]), // Between second and third
          search_before_after(5 * 4096, &insert_list[0], &insert_list[2]), // At third
          search_before_after(6 * 4096, &insert_list[2], nullptr),         // After third
        };

        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
#ifdef DEBUG
            printf("Insert: off=%" PRId64 ", size=%" PRId64 "\n", to_insert.off, to_insert.size);
            fflush(stdout);
#endif
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert.off, to_insert.size) == 0);
        }

#ifdef DEBUG
        print_list(extlist.off);
        fflush(stdout);
#endif

        /* Verify __block_srch_pair() versus expected_before_after */
        WT_EXT dummy;
        uint32_t idx = 0;
        for (const search_before_after &expected : expected_before_after) {
            WT_EXT *before = &dummy;
            WT_EXT *after = &dummy;
            __ut_block_off_srch_pair(&extlist, expected.off, &before, &after);
#ifdef DEBUG
            printf("Verify: %" PRIu32 ". Expected: off=%" PRId64, idx, expected.off);
            if (expected.before != nullptr)
                printf("; before: off=%" PRId64 ", size=%" PRId64, expected.before->off,
                  expected.before->size);
            else
                printf("; before == nullptr");
            if (expected.after != nullptr)
                printf("; after: off=%" PRId64 ", size=%" PRId64, expected.after->off,
                  expected.after->size);
            else
                printf("; after == nullptr");

            if (before != nullptr)
                printf(
                  "; Actual: before: off=%" PRId64 ", size=%" PRId64, before->off, before->size);
            else
                printf("; Actual: before == nullptr");
            if (after != nullptr)
                printf("; after: off=%" PRId64 ", size=%" PRId64 "\n", after->off, after->size);
            else
                printf("; after == nullptr\n");
            fflush(stdout);
            ++idx;
#endif
            if (expected.before != nullptr) {
                REQUIRE(before != nullptr);
                REQUIRE(before->off == expected.before->off);
                REQUIRE(before->size == expected.before->size);
            } else {
                REQUIRE(before == nullptr);
            }

            if (expected.after != nullptr) {
                REQUIRE(after != nullptr);
                REQUIRE(after->off == expected.after->off);
                REQUIRE(after->size == expected.after->size);
            } else {
                REQUIRE(after == nullptr);
            }
        }
    }
}

/* Expected match */
struct search_match {
    wt_off_t off;
    wt_off_t size;
    bool match;

    search_match(wt_off_t off, wt_off_t size, bool match) : off(off), size(size), match(match) {}
};

TEST_CASE("Extent Lists: block_off_match", "[extent_list2]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> mock_session = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = mock_session->getWtSessionImpl();

    /* Extents to insert */
    std::vector<off_size> insert_list{
      off_size(3 * 4096, 4096), // Second
      off_size(4096, 4096),     // First
      off_size(5 * 4096, 4096), // Third
    };

    std::vector<search_match> expected_match{
      search_match(0, 0, false),                 // Empty: Before first
      search_match(4095, 0, false),              // Empty: Just before first
      search_match(4096, 0, false),              // Empty: At the start of first
      search_match(4096 + 4095, 0, false),       // Empty: At end first
      search_match(2 * 4096, 0, false),          // Empty: Just after first
      search_match(2 * 4096 + 64, 0, false),     // Empty: Between first and second
      search_match(3 * 4096, 0, false),          // Empty: At the start of second
      search_match(4 * 4096 + 64, 0, false),     // Empty: Between second and third
      search_match(5 * 4096, 0, false),          // Empty: At the start of third
      search_match(6 * 4096, 0, false),          // Empty: Just after third
      search_match(4096 - 128, 64, false),       // Before first
      search_match(4095, 1, false),              // Just before first, i.e. touching.
      search_match(4096 - 64, 128, true),        // Overlapping the start of first
      search_match(4096, 1, true),               // Just the start of first
      search_match(4096, 64, true),              // At the start of first
      search_match(4096 + 64, 64, true),         // Within first
      search_match(2 * 4096 - 64, 64, true),     // At the end of first
      search_match(2 * 4096 - 1, 1, true),       // Just the end of first
      search_match(2 * 4096 - 64, 128, true),    // Overlapping the end of first
      search_match(4096, 4096, true),            // The same as first
      search_match(4096 - 64, 4096 + 128, true), // Completely overlapping first
    };

    std::vector<WT_EXT **> stack(WT_SKIP_MAXDEPTH, nullptr);

    SECTION("search an empty list")
    {
        BREAK;
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Verify */
        uint32_t idx = 0;
        for (const search_match &expected : expected_match) {
            bool match = __ut_block_off_match(&extlist, expected.off, expected.size);
#ifdef DEBUG
            printf("Verify: %" PRIu32 ". Expected: off=%" PRId64 ", size=%" PRId64
                   ", match = false; Actual: match = %s\n",
              idx, expected.off, expected.size, match ? "true" : "false");
            fflush(stdout);
            ++idx;
#endif
            REQUIRE(match == false);
        }
    }

    SECTION("search a non-empty list")
    {
        BREAK;
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
#ifdef DEBUG
            printf("Insert: off=%" PRId64 ", size=%" PRId64 "\n", to_insert.off, to_insert.size);
            fflush(stdout);
#endif
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert.off, to_insert.size) == 0);
        }

#ifdef DEBUG
        print_list(extlist.off);
        fflush(stdout);
#endif

        /* Verify __block_srch_pair() versus expected_match */
        uint32_t idx = 0;
        for (const search_match &expected : expected_match) {
            bool match = __ut_block_off_match(&extlist, expected.off, expected.size);
#ifdef DEBUG
            printf("Verify: %" PRIu32 ". Expected: off=%" PRId64 ", size=%" PRId64
                   ", match=%s; Actual: match = %s\n",
              idx, expected.off, expected.size, expected.match ? "true" : "false",
              match ? "true" : "false");
            fflush(stdout);
            ++idx;
#endif
            REQUIRE(match == expected.match);
        }
    }
}
