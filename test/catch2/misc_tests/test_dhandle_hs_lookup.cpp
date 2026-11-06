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
#include <chrono>
#include <future>

#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"

#ifndef _WIN32
#include "layered_disagg_utils.h"
#endif

TEST_CASE("session->dhandle is unchanged after a cached history store lookup", "[dhandle]")
{
    constexpr auto uri = "file:cursor_test.wt";

    connection_wrapper conn("WT_TEST.dhandle_hs_lookup");
    auto *session = conn.create_session();
    REQUIRE(session->iface.create(&session->iface, uri, "key_format=i,value_format=i") == 0);

    // Cache a history store cursor in the session, as eviction threads do.
    REQUIRE(session->dhandle == nullptr);
    REQUIRE(__wt_curhs_cache(session) == 0);

    // Look up the history store btree the way eviction does, holding the pass lock.
    auto lookup_cached_hs = [session]() {
        auto *pass_lock = &S2C(session)->evict->evict_pass_lock;
        WT_BTREE *hs_btree = nullptr;

        __wt_spin_lock(session, pass_lock);
        FLD_SET(session->lock_flags, WT_SESSION_LOCKED_PASS);
        const int ret = __wt_curhs_get_cached(session, WT_HS_ID, &hs_btree);
        FLD_CLR(session->lock_flags, WT_SESSION_LOCKED_PASS);
        __wt_spin_unlock(session, pass_lock);

        REQUIRE(ret == 0);
        REQUIRE(hs_btree != nullptr);
    };

    SECTION("session without a dhandle")
    {
        lookup_cached_hs();
        CHECK(session->dhandle == nullptr);
    }

    SECTION("session holding another tree's dhandle")
    {
        REQUIRE(__wt_session_get_dhandle(session, uri, nullptr, nullptr, 0) == 0);
        auto *dhandle = session->dhandle;

        lookup_cached_hs();
        CHECK(session->dhandle == dhandle);

        // Release the handle acquired above, even if the lookup replaced it on the session.
        session->dhandle = dhandle;
        REQUIRE(__wt_session_release_dhandle(session) == 0);
    }
}

TEST_CASE("The pass lock permits acquisition of a closed dhandle", "[dhandle][evict]")
{
    connection_wrapper conn("WT_TEST.dhandle_hs_open");
    auto *session = conn.create_session();
    WT_DATA_HANDLE dhandle = {};
    REQUIRE(__wt_rwlock_init(session, &dhandle.rwlock) == 0);
    session->dhandle = &dhandle;

    auto *pass_lock = &S2C(session)->evict->evict_pass_lock;
    bool is_dead;
    __wt_spin_lock(session, pass_lock);
    FLD_SET(session->lock_flags, WT_SESSION_LOCKED_PASS);
    const int ret = __wt_session_lock_dhandle(session, 0, &is_dead);
    const bool exclusive = F_ISSET(&dhandle, WT_DHANDLE_EXCLUSIVE);
    FLD_CLR(session->lock_flags, WT_SESSION_LOCKED_PASS);
    __wt_spin_unlock(session, pass_lock);

    int release_ret = 0;
    if (ret == 0 && !is_dead)
        release_ret = __wt_session_release_dhandle(session);
    else
        WT_DHANDLE_CLEAR(session);
    __wt_rwlock_destroy(session, &dhandle.rwlock);

    CHECK(ret == 0);
    CHECK(!is_dead);
    CHECK(exclusive);
    CHECK(release_ret == 0);
    CHECK(session->dhandle == nullptr);
}

#ifndef _WIN32
TEST_CASE("Cached history store lookup does not wait for a dhandle writer", "[dhandle][evict]")
{
    const char *home = "WT_TEST.dhandle_hs_contention";
    const char *caller_uri = "file:cursor_test.wt";
    const bool outdated = GENERATE(false, true);
    const bool with_caller_dhandle = GENERATE(false, true);
    const bool nowait_preset = GENERATE(false, true);
    testutil_remove(home);
    {
        connection_wrapper conn(home, layered_disagg_build_cfg("follower").c_str());
        auto *session = conn.create_session();
        auto *writer = conn.create_session();
        WT_CURSOR *hs_cursor;
        REQUIRE(session->iface.open_cursor(
                  &session->iface, WT_HS_URI_SHARED, nullptr, nullptr, &hs_cursor) == 0);
        WT_DHANDLE_CLEAR(session);
        REQUIRE(hs_cursor->close(hs_cursor) == 0);

        uint64_t hash_value;
        __wt_cursor_get_hash(session, WT_HS_URI_SHARED, nullptr, &hash_value);
        auto *bucket = &session->cursor_cache[hash_value & (S2C(session)->hash_size - 1)];
        auto *cursor = TAILQ_FIRST(bucket);
        REQUIRE(cursor != nullptr);
        auto *dhandle = ((WT_CURSOR_BTREE *)cursor)->dhandle;

        if (with_caller_dhandle) {
            REQUIRE(session->iface.create(
                      &session->iface, caller_uri, "key_format=i,value_format=i") == 0);
            REQUIRE(__wt_session_get_dhandle(session, caller_uri, nullptr, nullptr, 0) == 0);
        }
        auto *caller_dhandle = session->dhandle;
        REQUIRE((caller_dhandle != nullptr) == with_caller_dhandle);
        if (nowait_preset)
            FLD_SET(session->lock_flags, WT_SESSION_NO_DHANDLE_LOCK_WAIT);
        const uint32_t caller_lock_flags = session->lock_flags;
        WT_DHANDLE_ACQUIRE(dhandle);
        writer->dhandle = dhandle;

        std::promise<void> started;
        std::promise<void> run_lookup;
        auto started_future = started.get_future();
        auto run_lookup_future = run_lookup.get_future();
        WT_BTREE *hs_btree = nullptr;
        auto lookup = std::async(std::launch::async, [&]() {
            auto *pass_lock = &S2C(session)->evict->evict_pass_lock;
            __wt_spin_lock(session, pass_lock);
            FLD_SET(session->lock_flags, WT_SESSION_LOCKED_PASS);
            started.set_value();
            run_lookup_future.wait();
            const int ret = __wt_curhs_get_cached(session, WT_HS_ID_SHARED, &hs_btree);
            FLD_CLR(session->lock_flags, WT_SESSION_LOCKED_PASS);
            __wt_spin_unlock(session, pass_lock);
            return ret;
        });

        const auto started_status = started_future.wait_for(std::chrono::seconds(5));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        int writer_ret;
        while ((writer_ret = __wt_session_dhandle_try_writelock(writer)) == EBUSY &&
          std::chrono::steady_clock::now() < deadline)
            __wt_yield();
        if (writer_ret == 0)
            __wt_atomic_store_bool_relaxed(&dhandle->outdated, outdated);
        run_lookup.set_value();
        const auto lookup_status = lookup.wait_for(std::chrono::seconds(5));

        // Release the writer before asserting so a blocking lookup can finish on failure.
        if (writer_ret == 0)
            __wt_session_dhandle_writeunlock(writer);
        WT_DHANDLE_CLEAR(writer);
        const int ret = lookup.get();

        CAPTURE(outdated, with_caller_dhandle, nowait_preset);
        CHECK(started_status == std::future_status::ready);
        CHECK(writer_ret == 0);
        CHECK(lookup_status == std::future_status::ready);
        CHECK(ret == WT_NOTFOUND);
        CHECK(hs_btree == nullptr);
        CHECK(session->dhandle == caller_dhandle);
        CHECK(session->lock_flags == caller_lock_flags);
        CHECK(TAILQ_EMPTY(bucket));
        CHECK(__wt_atomic_load_int32_relaxed(&dhandle->session_inuse) == 0);
        WT_DHANDLE_RELEASE(dhandle);
        FLD_CLR(session->lock_flags, WT_SESSION_NO_DHANDLE_LOCK_WAIT);

        if (with_caller_dhandle) {
            // Release the acquired handle even if the failed lookup replaced it on the session.
            session->dhandle = caller_dhandle;
            REQUIRE(__wt_session_release_dhandle(session) == 0);
            CHECK(session->dhandle == nullptr);

            WT_CURSOR *caller_cursor;
            REQUIRE(session->iface.open_cursor(
                      &session->iface, caller_uri, nullptr, nullptr, &caller_cursor) == 0);
            caller_cursor->set_key(caller_cursor, 1);
            caller_cursor->set_value(caller_cursor, 42);
            REQUIRE(caller_cursor->insert(caller_cursor) == 0);
            caller_cursor->set_key(caller_cursor, 1);
            REQUIRE(caller_cursor->search(caller_cursor) == 0);
            int value;
            REQUIRE(caller_cursor->get_value(caller_cursor, &value) == 0);
            CHECK(value == 42);
            REQUIRE(caller_cursor->close(caller_cursor) == 0);
        }
    }
    testutil_remove(home);
}

TEST_CASE("Closing a cached history store lookup does not wait for a swept dhandle writer",
  "[dhandle][evict]")
{
    const char *home = "WT_TEST.dhandle_hs_close_sweep";
    testutil_remove(home);
    {
        connection_wrapper conn(home, layered_disagg_build_cfg("follower").c_str());
        auto *session = conn.create_session();
        auto *writer = conn.create_session();

        // Cache a usable local history store cursor and a shared history store cursor.
        REQUIRE(__wt_curhs_cache(session) == 0);
        WT_CURSOR *hs_cursor;
        REQUIRE(session->iface.open_cursor(
                  &session->iface, WT_HS_URI_SHARED, nullptr, nullptr, &hs_cursor) == 0);
        WT_DHANDLE_CLEAR(session);
        REQUIRE(hs_cursor->close(hs_cursor) == 0);

        uint64_t hash_value;
        __wt_cursor_get_hash(session, WT_HS_URI_SHARED, nullptr, &hash_value);
        const uint32_t shared_bucket = (uint32_t)(hash_value & (S2C(session)->hash_size - 1));
        auto *bucket = &session->cursor_cache[shared_bucket];
        auto *shared_cursor = TAILQ_FIRST(bucket);
        REQUIRE(shared_cursor != nullptr);
        auto *dhandle = ((WT_CURSOR_BTREE *)shared_cursor)->dhandle;
        WT_DHANDLE_ACQUIRE(dhandle);
        writer->dhandle = dhandle;

        const uint32_t caller_lock_flags = session->lock_flags;

        std::promise<void> started;
        std::promise<void> run_lookup;
        auto started_future = started.get_future();
        auto run_lookup_future = run_lookup.get_future();
        WT_BTREE *hs_btree = nullptr;
        auto lookup = std::async(std::launch::async, [&]() {
            auto *pass_lock = &S2C(session)->evict->evict_pass_lock;
            __wt_spin_lock(session, pass_lock);
            FLD_SET(session->lock_flags, WT_SESSION_LOCKED_PASS);
            started.set_value();
            run_lookup_future.wait();
            const int ret = __wt_curhs_get_cached(session, WT_HS_ID, &hs_btree);
            FLD_CLR(session->lock_flags, WT_SESSION_LOCKED_PASS);
            __wt_spin_unlock(session, pass_lock);
            return ret;
        });

        const auto started_status = started_future.wait_for(std::chrono::seconds(5));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        int writer_ret;
        while ((writer_ret = __wt_session_dhandle_try_writelock(writer)) == EBUSY &&
          std::chrono::steady_clock::now() < deadline)
            __wt_yield();
        if (writer_ret == 0)
            __wt_atomic_store_bool_relaxed(&dhandle->outdated, true);

        // Make closing the local history store cursor sweep the shared cursor's bucket.
        session->cursor_sweep_countdown = 1;
        session->cursor_sweep_position = shared_bucket;
        session->last_cursor_sweep = 0;
        run_lookup.set_value();
        const auto lookup_status = lookup.wait_for(std::chrono::seconds(5));

        // Release the writer before asserting so a blocking lookup can finish on failure.
        if (writer_ret == 0)
            __wt_session_dhandle_writeunlock(writer);
        WT_DHANDLE_CLEAR(writer);
        const int ret = lookup.get();

        CHECK(started_status == std::future_status::ready);
        CHECK(writer_ret == 0);
        CHECK(lookup_status == std::future_status::ready);
        CHECK(ret == 0);
        CHECK(hs_btree != nullptr);
        CHECK(session->dhandle == nullptr);
        CHECK(session->lock_flags == caller_lock_flags);
        CHECK(TAILQ_EMPTY(bucket));
        CHECK(__wt_atomic_load_int32_relaxed(&dhandle->session_inuse) == 0);
        WT_DHANDLE_RELEASE(dhandle);
    }
    testutil_remove(home);
}
#endif /* !_WIN32 */
