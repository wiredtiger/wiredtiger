/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "os_module.h"
#include <catch2/catch.hpp>

TEST_CASE("OS Module: test_mod", "[os_module]")
{
    CHECK(test_mod() == 0);

    char *tmp;
    CHECK(__wt_calloc(NULL, 1, 1024, &tmp) == 0);
}
