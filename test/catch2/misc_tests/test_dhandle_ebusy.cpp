/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * When attempting to lock a dhandle returns EBUSY because the btree has special flags set
 * (bulk/salvage/verify), the session's dhandle ref must be cleared to NULL. Leaving it set exposes
 * callers to a handle they never locked, which can corrupt the dhandle's lock state if they attempt
 * to release it.
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"
#include "../../utility/test_util.h"

TEST_CASE("session->dhandle is NULL after EBUSY from get_dhandle", "[dhandle]")
{
    const std::string home = "WT_TEST.dhandle_ebusy";
    testutil_system("rm -rf %s && mkdir -p %s", home.c_str(), home.c_str());

    {
        connection_wrapper conn(home);

        WT_SESSION_IMPL *s1 = conn.create_session();
        WT_SESSION *pub_s1 = &s1->iface;
        REQUIRE(pub_s1->create(pub_s1, "file:t.wt", "key_format=i,value_format=i") == 0);

        WT_CURSOR *bulk = nullptr;
        REQUIRE(pub_s1->open_cursor(pub_s1, "file:t.wt", nullptr, "bulk", &bulk) == 0);

        WT_SESSION_IMPL *s2 = conn.create_session();
        REQUIRE(s2->dhandle == nullptr);

        int ret = __wt_session_get_dhandle(s2, "file:t.wt", nullptr, nullptr, 0);
        REQUIRE(ret == EBUSY);
        CHECK(s2->dhandle == nullptr);

        REQUIRE(bulk->close(bulk) == 0);
    }

    utils::wiredtiger_cleanup(home);
}
