/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * WT-15078: session_get_dhandle returning EBUSY leaves hanging handle.
 *
 * When __wt_session_lock_dhandle returns EBUSY because the btree has
 * WT_BTREE_SPECIAL_FLAGS (bulk/salvage/verify), session->dhandle must be
 * cleared to NULL.  Leaving it set exposes callers to a handle they never
 * locked, which can corrupt the dhandle's lock state if they attempt to
 * release it.
 *
 * The public API wraps every call in API_SESSION_PUSH/POP, which saves and
 * restores session->dhandle, so the stale pointer never escapes to
 * application code.  The bug only affects internal callers that invoke
 * __wt_session_get_dhandle directly, such as the checkpoint and
 * checkpoint-cleanup paths.
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../utils.h"
#include "../wrappers/connection_wrapper.h"
#include "../../utility/test_util.h"

TEST_CASE("WT-15078: session->dhandle is NULL after EBUSY from get_dhandle", "[dhandle][WT-15078]")
{
    const std::string home = "WT_TEST.wt15078_dhandle_ebusy";
    testutil_system("rm -rf %s && mkdir -p %s", home.c_str(), home.c_str());

    {
        connection_wrapper conn(home);

        WT_SESSION_IMPL *s1 = conn.create_session();
        WT_SESSION *pub_s1 = &s1->iface;

        REQUIRE(pub_s1->create(pub_s1, "file:t.wt", "key_format=i,value_format=i") == 0);

        /*
         * Open a bulk cursor: this sets WT_BTREE_BULK on the btree and holds
         * the dhandle exclusively.  While the cursor is open, any other
         * session calling __wt_session_lock_dhandle on the same file will hit
         * the WT_BTREE_SPECIAL_FLAGS check and return EBUSY.
         */
        WT_CURSOR *bulk = nullptr;
        REQUIRE(pub_s1->open_cursor(pub_s1, "file:t.wt", nullptr, "bulk", &bulk) == 0);

        WT_SESSION_IMPL *s2 = conn.create_session();

        REQUIRE(s2->dhandle == nullptr);

        /*
         * Call the internal function directly to bypass the API_SESSION_POP
         * mechanism, which would otherwise restore session->dhandle on return.
         * This is the path taken by internal subsystems (checkpoint, RTS, …).
         */
        int ret = __wt_session_get_dhandle(s2, "file:t.wt", nullptr, nullptr, 0);
        REQUIRE(ret == EBUSY);

        /*
         * This is the invariant WT-15078 requires: session->dhandle must be
         * NULL after a failed get.  Before the fix it is left pointing at the
         * bulk-loaded handle, giving callers the illusion that a lock was
         * acquired.
         */
        CHECK(s2->dhandle == nullptr);

        REQUIRE(bulk->close(bulk) == 0);
    }

    utils::wiredtiger_cleanup(home);
}
