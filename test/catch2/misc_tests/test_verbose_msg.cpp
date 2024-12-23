/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include "wt_internal.h"
#include <string>

TEST_CASE("Test generation of sub-level error codes when using verbose message macros")
{
    WT_SESSION_IMPL *session;
    WT_RET_VERBOSE_MSG(session, EINVAL, WT_NONE, "original error");
}
