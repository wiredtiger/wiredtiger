/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"

struct MockCursor {
    uint64_t flags;
};

int
mock_cursor_bounds_save(MockCursor *cursor, WT_CURSOR_BOUNDS_STATE *state)
{
    state->bound_flags = F_MASK(cursor, WT_CURSTD_BOUND_ALL);
    return (0);
}

int
mock_cursor_bounds_restore(MockCursor *cursor, WT_CURSOR_BOUNDS_STATE *state)
{
    F_CLR(cursor, WT_CURSTD_BOUND_ALL);
    F_SET(cursor, state->bound_flags);
    return (0);
}

int
validate_mock_cursor_bounds_restore(MockCursor *cursor, uint64_t original_cursor_flags)
{
    assert(cursor->flags == original_cursor_flags);
    return (0);
}

TEST_CASE("Bounds save and restore flag logic", "[bounds_restore]")
{
    uint64_t original_cursor_flags;
    MockCursor *mock_cursor;
    WT_CURSOR_BOUNDS_STATE mock_state;

    // Save bounds flags and ensure that the restore logic correctly restores the desired flags.
    SECTION("Save non-empty non-inclusive bounds flags and restore")
    {
        mock_cursor = (MockCursor *)calloc(1, sizeof(MockCursor));
        F_SET(mock_cursor, WT_CURSTD_BOUND_UPPER);
        F_SET(mock_cursor, WT_CURSTD_BOUND_LOWER);
        original_cursor_flags = mock_cursor->flags;
        REQUIRE(mock_cursor_bounds_save(mock_cursor, &mock_state) == 0);
        REQUIRE(mock_cursor_bounds_restore(mock_cursor, &mock_state) == 0);
        REQUIRE(validate_mock_cursor_bounds_restore(mock_cursor, original_cursor_flags) == 0);
    }

    SECTION("Save non-empty inclusive bounds flags and restore")
    {
        mock_cursor = (MockCursor *)calloc(1, sizeof(MockCursor));
        F_SET(mock_cursor, WT_CURSTD_BOUND_UPPER_INCLUSIVE);
        F_SET(mock_cursor, WT_CURSTD_BOUND_LOWER_INCLUSIVE);
        original_cursor_flags = mock_cursor->flags;
        REQUIRE(mock_cursor_bounds_save(mock_cursor, &mock_state) == 0);
        REQUIRE(mock_cursor_bounds_restore(mock_cursor, &mock_state) == 0);
        REQUIRE(validate_mock_cursor_bounds_restore(mock_cursor, original_cursor_flags) == 0);
    }
}
