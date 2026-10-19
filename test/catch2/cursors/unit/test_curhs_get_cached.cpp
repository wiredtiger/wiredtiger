/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

/*
 * Looking up the history store btree through a cached cursor must leave the session's dhandle as
 * the caller had it. The lookup holds no reference to the history store dhandle once the cursor
 * closes, so a dhandle left on the session can be freed by sweep while the session still points at
 * it.
 */

#include <catch2/catch.hpp>

#include "wiredtiger.h"
#include "wt_internal.h"
#include "../../utils.h"
#include "../../wrappers/connection_wrapper.h"

static void
check_lookup_keeps_dhandle(WT_SESSION_IMPL *session, WT_DATA_HANDLE *expected)
{
    WT_EVICT *evict = S2C(session)->evict;
    WT_BTREE *hs_btree = nullptr;

    /* Eviction performs this lookup while holding the pass lock. */
    __wt_spin_lock(session, &evict->evict_pass_lock);
    int ret = __wt_curhs_get_cached(session, WT_HS_ID, &hs_btree);
    __wt_spin_unlock(session, &evict->evict_pass_lock);

    REQUIRE(ret == 0);
    REQUIRE(hs_btree != nullptr);
    CHECK(session->dhandle == expected);
}

TEST_CASE("History store cache lookup leaves the session dhandle unchanged",
  "[dhandle][curhs_get_cached]")
{
    const std::string home = "WT_TEST.curhs_get_cached";
    utils::wiredtiger_cleanup(home);

    {
        connection_wrapper conn(home);
        WT_SESSION_IMPL *session = conn.create_session();
        WT_SESSION *wt_session = &session->iface;
        const char *uri = "file:cursor_test.wt";
        REQUIRE(wt_session->create(wt_session, uri, "key_format=i,value_format=i") == 0);

        /* Cache a history store cursor in this session, as eviction threads do. */
        REQUIRE(session->dhandle == nullptr);
        REQUIRE(__wt_curhs_cache(session) == 0);

        check_lookup_keeps_dhandle(session, nullptr);

        /* Repeat while the session holds another tree's dhandle. */
        REQUIRE(__wt_session_get_dhandle(session, uri, nullptr, nullptr, 0) == 0);
        WT_DATA_HANDLE *dhandle = session->dhandle;
        check_lookup_keeps_dhandle(session, dhandle);

        /* Release the handle acquired above, even if the lookup replaced it on the session. */
        int ret;
        WT_WITH_DHANDLE(session, dhandle, ret = __wt_session_release_dhandle(session));
        REQUIRE(ret == 0);
    }

    utils::wiredtiger_cleanup(home);
}
