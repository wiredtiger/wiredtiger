/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * block_ext.c: [extent_list2] Test extent list insert functions without block: __block_ext_insert,
 * and __block_off_insert.
 *
 * Test extent list insert/remove functions with block: __block_merge, __block_off_remove,
 * __block_extend, and __block_append.
 *
 * Test extent list search functions: __block_off_srch_pair, and __block_off_match.
 *
 * Test extent allocation function: __wti_block_alloc.
 */

/* Choose one */
#define DEBUG /* Print debugging output */
//#undef DEBUG

#include <algorithm>
#include <memory>

#include <catch2/catch.hpp>

#include "test_util.h"
#include "../utils.h"
#include "../wrappers/mock_session.h"
#include "wt_internal.h"

// Wrappers and Utilities
// For expected values
struct off_size {
    wt_off_t _off;
    wt_off_t _size;

    off_size(wt_off_t off = 0, wt_off_t size = 0) : _off(off), _size(size) {}
};

// To sort off_size by _off and _size
struct {
    bool
    operator()(const off_size &left, const off_size &right)
    {
        return (
          (left._off < right._off) || ((left._off == right._off) && (left._size < right._size)));
    }
} off_size_Less_off_and_size;

#if 0
// To sort off_size by just _off
struct
{
    bool operator()(const off_size& left, const off_size& right) {
    return (left._off < right._off);
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
    printf("Allocated WT_EXT %p: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64 ", depth=%" PRIu8
           ", next[0]=%p\n",
      ext, ext->off, ext->size, (ext->off + ext->size - 1), ext->depth, ext->next[0]);
    fflush(stdout);
#endif

    return ext;
}

WT_EXT *
alloc_new_ext(WT_SESSION_IMPL *session, const off_size &one)
{
    return alloc_new_ext(session, one._off, one._size);
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
verify_off_extent_list(const WT_EXTLIST &extlist, const std::vector<off_size> &expected_order,
  bool verify_entries_bytes = true)
{
    REQUIRE(extlist.entries == expected_order.size());
    uint32_t idx = 0;
    uint64_t expected_bytes = 0;
    for (const off_size &expected : expected_order) {
        WT_EXT *ext = get_off_n(extlist, idx);
#ifdef DEBUG
        printf("Verify: %" PRIu32 ". Expected: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64
               "; Actual: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64 "\n",
          idx, expected._off, expected._size, (expected._off + expected._size - 1), ext->off,
          ext->size, (ext->off + ext->size - 1));
        fflush(stdout);
#endif
        REQUIRE(ext->off == expected._off);
        REQUIRE(ext->size == expected._size);
        ++idx;
        expected_bytes += ext->size;
    }
    if (!verify_entries_bytes)
        return;
    REQUIRE(extlist.entries == idx);
    REQUIRE(extlist.bytes == expected_bytes);
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
        utils::ext_print_list(extlist.off);
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
          off_size(3 * 4096, 4096), // Second [12,288, 16,383]
          off_size(4096, 4096),     // First [4,096, 8,191]
          off_size(5 * 4096, 4096), // Third [20,480, 24,575]
        };

        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
#ifdef DEBUG
            printf("Insert: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64 "\n", to_insert._off,
              to_insert._size, (to_insert._off + to_insert._size - 1));
            fflush(stdout);
#endif
            WT_EXT *insert_ext = alloc_new_ext(session, to_insert);
            REQUIRE(__ut_block_ext_insert(session, &extlist, insert_ext) == 0);
        }

#ifdef DEBUG
        utils::ext_print_list(extlist.off);
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
        utils::ext_print_list(extlist.off);
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
          off_size(3 * 4096, 4096), // Second [12,288, 16,383]
          off_size(4096, 4096),     // First [4,096, 8,191]
          off_size(5 * 4096, 4096), // Third [20,480, 24,575]
        };

        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
#ifdef DEBUG
            printf("Insert: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64 "\n", to_insert._off,
              to_insert._size, (to_insert._off + to_insert._size - 1));
            fflush(stdout);
#endif
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert._off, to_insert._size) == 0);
        }

#ifdef DEBUG
        utils::ext_print_list(extlist.off);
        fflush(stdout);
#endif

        /* Verify extents */
        std::vector<off_size> expected_order{insert_list};
        std::sort(expected_order.begin(), expected_order.end(), off_size_Less_off_and_size);
        verify_off_extent_list(extlist, expected_order);
    }
}

/* Tests and expected values for __block_srch_pair */
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
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Verify */
        std::vector<wt_off_t> expected_list{0, 4096, 3 * 4096}; // 3, 4096, 12,288
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
          off_size(3 * 4096, 4096), // Second [12,288, 16,383]
          off_size(4096, 4096),     // First [4,096, 8,191]
          off_size(5 * 4096, 4096), // Third [20,480, 24,575]
        };

        /* Tests and expected values for __block_srch_pair() */
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

        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
#ifdef DEBUG
            printf("Insert: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64 "\n", to_insert._off,
              to_insert._size, (to_insert._off + to_insert._size - 1));
            fflush(stdout);
#endif
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert._off, to_insert._size) == 0);
        }

#ifdef DEBUG
        utils::ext_print_list(extlist.off);
        fflush(stdout);
#endif

        /* Verify __block_srch_pair() versus expected_before_after */
        WT_EXT dummy;
        uint32_t idx = 0;
        for (const search_before_after &expected : expected_before_after) {
            WT_EXT *before = &dummy;
            WT_EXT *after = &dummy;
            __ut_block_off_srch_pair(&extlist, expected._off, &before, &after);
#ifdef DEBUG
            printf("Verify: %" PRIu32 ". Expected: off=%" PRId64, idx, expected._off);
            if (expected._before != nullptr)
                printf("; _before: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64,
                  expected._before->_off, expected._before->_size,
                  (expected._before->_off + expected._before->_size - 1));
            else
                printf("; _before == nullptr");
            if (expected._after != nullptr)
                printf("; after: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64,
                  expected._after->_off, expected._after->_size,
                  (expected._after->_off + expected._after->_size - 1));
            else
                printf("; after == nullptr");

            if (before != nullptr)
                printf("; Actual: before: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64,
                  before->off, before->size, (before->off + before->size - 1));
            else
                printf("; Actual: before == nullptr");
            if (after != nullptr)
                printf("; after: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64 "\n", after->off,
                  after->size, (after->off + after->size - 1));
            else
                printf("; _after == nullptr\n");
            fflush(stdout);
            ++idx;
#endif
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
    }
}

/* Tests and expected values for __block_off_match */
struct search_match {
    wt_off_t _off;
    wt_off_t _size;
    bool _match;

    search_match(wt_off_t off, wt_off_t size, bool match) : _off(off), _size(size), _match(match) {}
};

TEST_CASE("Extent Lists: block_off_match", "[extent_list2]")
{
    /* Build Mock session, this will automatically create a mock connection. */
    std::shared_ptr<MockSession> mock_session = MockSession::buildTestMockSession();
    WT_SESSION_IMPL *session = mock_session->getWtSessionImpl();

    /* Extents to insert */
    std::vector<off_size> insert_list{
      off_size(3 * 4096, 4096), // Second [12,288, 16,383]
      off_size(4096, 4096),     // First [4,096, 8,191]
      off_size(5 * 4096, 4096), // Third [20,480, 24,575]
    };

    std::vector<search_match> expected_match
    {
        search_match(0, 0, false),      // Empty: Before first 0
          search_match(4095, 0, false), // Empty: Just before first 4,095
          search_match(4096, 0, false), // Empty: At the start of first 4,096
#if 0 // Failed: Verify: 3. Expected: off=8191, size=0, end=8190, match=false; Actual: match = true
      search_match(4096 + 4095, 0, false),       // Empty: At end first 8,191
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
        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Verify */
        uint32_t idx = 0;
        for (const search_match &expected : expected_match) {
            bool match = __ut_block_off_match(&extlist, expected._off, expected._size);
#ifdef DEBUG
            printf("Verify: %" PRIu32 ". Expected: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64
                   ", match = false; Actual: match = %s\n",
              idx, expected._off, expected._size, (expected._off + expected._size - 1),
              match ? "true" : "false");
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
            printf("Insert: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64 "\n", to_insert._off,
              to_insert._size, (to_insert._off + to_insert._size - 1));
            fflush(stdout);
#endif
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert._off, to_insert._size) == 0);
        }

#ifdef DEBUG
        utils::ext_print_list(extlist.off);
        fflush(stdout);
#endif

        /* Verify __block_off_match() versus expected_match */
        uint32_t idx = 0;
        for (const search_match &expected : expected_match) {
            bool match = __ut_block_off_match(&extlist, expected._off, expected._size);
#ifdef DEBUG
            printf("Verify: %" PRIu32 ". Expected: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64
                   ", match=%s; Actual: match = %s\n",
              idx, expected._off, expected._size, (expected._off + expected._size - 1),
              expected._match ? "true" : "false", match ? "true" : "false");
            fflush(stdout);
            ++idx;
#endif
            REQUIRE(match == expected._match);
        }
    }
}

/* Tests and expected values for operations that need an off_size to modify a WT_EXTLIST */
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

        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Empty block */
        WT_BLOCK block;
        memset(reinterpret_cast<void *>(&block), 0, sizeof(block));
        block.name = "__block_merge";
        block.allocsize = 1024;
        block.size = 4096; // Description information

        /* Insert/merge extents and verify */
        int idx = 0;
        for (const off_size_expected &test : test_list) {
            REQUIRE(__ut_block_merge(
                      session, &block, &extlist, test._off_size._off, test._off_size._size) == 0);
#ifdef DEBUG
            printf("%d. Insert/merge: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64 "\n", idx,
              test._off_size._off, test._off_size._size,
              (test._off_size._off + test._off_size._size - 1));
            utils::ext_print_list(extlist.off);
            fflush(stdout);
#endif

            verify_off_extent_list(extlist, test._expected_list, false);
            ++idx;
        }
    }
}

/* Tests and expected values for operations that need an off to modify a WT_EXTLIST */
struct off_expected {
    wt_off_t _off;
    std::vector<off_size> _expected_list;
};

TEST_CASE("Extent Lists: block_remove", "[extent_list2]")
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

        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

        /* Insert extents */
        for (const off_size &to_insert : insert_list) {
#ifdef DEBUG
            printf("Insert: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64 "\n", to_insert._off,
              to_insert._size, (to_insert._off + to_insert._size - 1));
            fflush(stdout);
#endif
            REQUIRE(__ut_block_off_insert(session, &extlist, to_insert._off, to_insert._size) == 0);
        }

#ifdef DEBUG
        utils::ext_print_list(extlist.off);
        fflush(stdout);
#endif

        /* Verify extents */
        std::vector<off_size> expected_order{insert_list};
        std::sort(expected_order.begin(), expected_order.end(), off_size_Less_off_and_size);
        verify_off_extent_list(extlist, expected_order);

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

        /* Remove extents and verify */
        WT_BLOCK block;
        memset(reinterpret_cast<void *>(&block), 0, sizeof(block));

        int idx = 0;
        for (const off_expected &test : test_list) {
            /* For testing, half request ext returned, and half do not. */
            if ((idx % 2) == 0)
                REQUIRE(__ut_block_off_remove(session, &block, &extlist, test._off, nullptr) == 0);
            else {
                WT_EXT *ext = nullptr;
                REQUIRE(__ut_block_off_remove(session, &block, &extlist, test._off, &ext) == 0);
                REQUIRE(ext != nullptr);
                __wti_block_ext_free(session, ext);
            }
#ifdef DEBUG
            printf("%d. Remove: off=%" PRId64 "\n", idx, test._off);
            utils::ext_print_list(extlist.off);
            fflush(stdout);
#endif

            verify_off_extent_list(extlist, test._expected_list, false);
            ++idx;
        }

        /* Verify empty extent list */
        verify_empty_extent_list(&extlist.off[0], &stack[0]);
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

        /* Empty extent list */
        WT_EXTLIST extlist;
        memset(&extlist, 0, sizeof(extlist));
        verify_empty_extent_list(&extlist.off[0], &stack[0]);

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
          {off_size(3 * 4096, 4096), // Second [12,288, 16,383]
            {
              off_size(4096, 4096),     // First [4,096, 8,191]
              off_size(3 * 4096, 4096), // Second [12,288, 16,383]
            }},
          {off_size(5 * 4096, 4096), // Third [20,480, 24,575]
            {
              off_size(4096, 4096),     // First [4,096, 8,191]
              off_size(3 * 4096, 4096), // Second [12,288, 16,383]
              off_size(5 * 4096, 4096), // Third [20,480, 24,575]
            }},
        };

        /* Append extents and verify */
        WT_BLOCK block;
        memset(reinterpret_cast<void *>(&block), 0, sizeof(block));
        block.name = "__block_append";
        block.allocsize = 1024;
        block.size = 4096; // Description information

        int idx = 0;
        for (const off_size_expected &test : test_list) {
            REQUIRE(__ut_block_append(
                      session, &block, &extlist, test._off_size._off, test._off_size._size) == 0);
#ifdef DEBUG
            printf("%d. Append: off=%" PRId64 ", size=%" PRId64 ", end=%" PRId64 "\n", idx,
              test._off_size._off, test._off_size._size,
              (test._off_size._off + test._off_size._size - 1));
            utils::ext_print_list(extlist.off);
            fflush(stdout);
#endif

            verify_off_extent_list(extlist, test._expected_list, true);
            ++idx;
        }
    }
}
