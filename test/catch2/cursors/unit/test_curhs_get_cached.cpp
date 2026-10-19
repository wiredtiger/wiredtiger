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

#include "wt_internal.h"
#include "../../wrappers/connection_wrapper.h"

namespace {

class curhs_get_cached_fixture {
public:
    curhs_get_cached_fixture() : _conn(home), _session(_conn.create_session())
    {
        auto *iface = &_session->iface;
        REQUIRE(iface->create(iface, uri, "key_format=i,value_format=i") == 0);

        // Cache a history store cursor in the session, as eviction threads do.
        REQUIRE(_session->dhandle == nullptr);
        REQUIRE(__wt_curhs_cache(_session) == 0);
    }

    WT_SESSION_IMPL *
    session() const
    {
        return _session;
    }

    // Look up the history store btree the way eviction does, holding the pass lock.
    void
    lookup_cached_hs() const
    {
        auto *pass_lock = &S2C(_session)->evict->evict_pass_lock;
        WT_BTREE *hs_btree = nullptr;

        __wt_spin_lock(_session, pass_lock);
        const int ret = __wt_curhs_get_cached(_session, WT_HS_ID, &hs_btree);
        __wt_spin_unlock(_session, pass_lock);

        REQUIRE(ret == 0);
        REQUIRE(hs_btree != nullptr);
    }

    static constexpr auto uri = "file:cursor_test.wt";

private:
    static constexpr auto home = "WT_TEST.curhs_get_cached";

    connection_wrapper _conn;
    WT_SESSION_IMPL *_session;
};

} // namespace

TEST_CASE_METHOD(curhs_get_cached_fixture,
  "curhs_get_cached: the lookup leaves the session dhandle unchanged",
  "[curhs_get_cached][dhandle]")
{
    auto *session_impl = session();

    SECTION("session without a dhandle")
    {
        lookup_cached_hs();
        CHECK(session_impl->dhandle == nullptr);
    }

    SECTION("session holding another tree's dhandle")
    {
        REQUIRE(__wt_session_get_dhandle(session_impl, uri, nullptr, nullptr, 0) == 0);
        auto *dhandle = session_impl->dhandle;

        lookup_cached_hs();
        CHECK(session_impl->dhandle == dhandle);

        // Release the handle acquired above, even if the lookup replaced it on the session.
        session_impl->dhandle = dhandle;
        REQUIRE(__wt_session_release_dhandle(session_impl) == 0);
    }
}
