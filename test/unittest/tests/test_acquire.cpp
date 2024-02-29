/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "wt_internal.h"

TEST_CASE("Acquire", "[acquire]")
{
    uint64_t a = 5;
    uint64_t *ap = &a;
    uint64_t a_result;
    WT_ACQUIRE_READ(a_result, *ap);
    REQUIRE(a == a_result);
}
