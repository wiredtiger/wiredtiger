/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <cstdint>

#include "wt_internal.h"

/*
 * A reader can read a disagg block header iff its own version is at least the header's
 * compatible_version. The read path used to compare against WT_BLOCK_DISAGG_COMPATIBLE_VERSION (the
 * reader's own oldest-compatible bound) instead of WT_BLOCK_DISAGG_VERSION (the reader's own
 * version), incorrectly rejecting readable blocks once the writer version diverges from the
 * compatible version.
 */
TEST_CASE("disagg block header version compatibility", "[block_disagg]")
{
    /* A reader can read a header that only requires an older or equal version. */
    REQUIRE(__ut_block_disagg_header_version_compatible(1, 1));
    REQUIRE(__ut_block_disagg_header_version_compatible(2, 1));
    REQUIRE(__ut_block_disagg_header_version_compatible(5, 3));

    /*
     * Reader version 2 reading a block that requires compatible_version 2: the case the buggy check
     * got wrong, rejecting a block the v2 reader is allowed to read.
     */
    REQUIRE(__ut_block_disagg_header_version_compatible(2, 2));

    /* A reader cannot read a header that requires a newer reader than the reader's own version. */
    REQUIRE_FALSE(__ut_block_disagg_header_version_compatible(1, 2));
    REQUIRE_FALSE(__ut_block_disagg_header_version_compatible(3, 5));

    /* The current build must be able to read everything it writes. */
    REQUIRE(__ut_block_disagg_header_version_compatible(
      WT_BLOCK_DISAGG_VERSION, WT_BLOCK_DISAGG_COMPATIBLE_VERSION));
    REQUIRE(__ut_block_disagg_header_version_compatible(
      WT_BLOCK_DISAGG_VERSION, WT_BLOCK_DISAGG_VERSION));
}
