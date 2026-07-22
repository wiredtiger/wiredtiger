/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <memory>

#include "wt_internal.h"

#include "wrappers/mock_session.h"

/*
 * Selective checkpoint pickup may only defer a pure checkpoint advance. This exercises the guard
 * (__disagg_meta_changed_beyond_checkpoint) that decides whether a file's shared metadata differs
 * from the local metadata in anything other than the "checkpoint" field -- in which case the pickup
 * must not be deferred.
 */
struct defer_meta_guard_fixture {
    std::shared_ptr<mock_session> session_wrapper;
    WT_SESSION_IMPL *session = nullptr;

    defer_meta_guard_fixture() : session_wrapper(mock_session::build_test_mock_session())
    {
        session = session_wrapper->get_wt_session_impl();
        REQUIRE(session != nullptr);
    }

    bool
    changed(const char *local, const char *shared)
    {
        bool result = false;
        REQUIRE(__ut_disagg_meta_changed_beyond_checkpoint(session, local, shared, &result) == 0);
        return (result);
    }
};

TEST_CASE_METHOD(defer_meta_guard_fixture, "disagg defer guard: checkpoint-only vs beyond",
  "[disagg_defer_meta_guard]")
{
    SECTION("only the checkpoint differs -> deferrable (not beyond)")
    {
        const char *local = "allocation_size=4KB,key_format=q,checkpoint=(WiredTigerCheckpoint.1=("
                            "addr=\"01\",order=1))";
        const char *shared = "allocation_size=4KB,key_format=q,checkpoint=(WiredTigerCheckpoint.2=("
                             "addr=\"02\",order=2))";
        REQUIRE(changed(local, shared) == false);
    }

    SECTION("checkpoint and checkpoint_lsn advance together -> deferrable (not beyond)")
    {
        /*
         * A checkpoint rewrites checkpoint_lsn too, and the follower leaves its local copy at the
         * unset sentinel, so this field always differs on a routine advance and must be ignored.
         */
        const char *local = "allocation_size=4KB,key_format=q,checkpoint=(order=1),"
                            "checkpoint_lsn=(4294967295,2147483647)";
        const char *shared =
          "allocation_size=4KB,key_format=q,checkpoint=(order=2),checkpoint_lsn=(2,128)";
        REQUIRE(changed(local, shared) == false);
    }

    SECTION("identical metadata -> not beyond")
    {
        const char *m = "allocation_size=4KB,key_format=q,checkpoint=(WiredTigerCheckpoint.1=(order=1))";
        REQUIRE(changed(m, m) == false);
    }

    SECTION("a non-checkpoint field value changed -> beyond")
    {
        const char *local = "allocation_size=4KB,app_metadata=(formatVersion=1),checkpoint=(order=1)";
        const char *shared = "allocation_size=4KB,app_metadata=(formatVersion=2),checkpoint=(order=2)";
        REQUIRE(changed(local, shared) == true);
    }

    SECTION("shared added a field -> beyond")
    {
        const char *local = "allocation_size=4KB,checkpoint=(order=1)";
        const char *shared = "allocation_size=4KB,readonly=true,checkpoint=(order=2)";
        REQUIRE(changed(local, shared) == true);
    }

    SECTION("shared dropped a field -> beyond")
    {
        const char *local = "allocation_size=4KB,readonly=true,checkpoint=(order=1)";
        const char *shared = "allocation_size=4KB,checkpoint=(order=2)";
        REQUIRE(changed(local, shared) == true);
    }

    SECTION("key ordering does not matter -> not beyond")
    {
        const char *local = "allocation_size=4KB,key_format=q,checkpoint=(order=1)";
        const char *shared = "checkpoint=(order=2),key_format=q,allocation_size=4KB";
        REQUIRE(changed(local, shared) == false);
    }
}
