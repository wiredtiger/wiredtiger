/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>

#include "utils.h"
#include "wiredtiger.h"
#include "wrappers/connection_wrapper.h"
#include "wt_internal.h"

TEST_CASE("Reconciliation tracking: ovfl_discard_verbose", "[reconciliation]")
{
    ConnectionWrapper conn(DB_HOME);
    WT_SESSION_IMPL *session = conn.createSession();

    SECTION("handle null page and tag")
    {
        REQUIRE(__ut_ovfl_discard_verbose(session, nullptr, nullptr, nullptr) == EINVAL);
    }
}
