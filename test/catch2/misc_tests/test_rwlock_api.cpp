/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <catch2/catch.hpp>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "wt_internal.h"
#include "../wrappers/connection_wrapper.h"

namespace {

struct rwlock_test {
    connection_wrapper connection{"WT_TEST_RWLOCK_API", "create"};
    WT_SESSION_IMPL *session = connection.create_session();
    WT_RWLOCK lock{};

    rwlock_test()
    {
        REQUIRE(__wt_rwlock_init(session, &lock) == 0);
    }

    ~rwlock_test()
    {
        __wt_rwlock_destroy(session, &lock);
    }
};

} // namespace

TEST_CASE("Read/write lock reports its uncontended acquisition and release state", "[rwlock]")
{
    rwlock_test test;

    CHECK_FALSE(__wt_rwlock_islocked(test.session, &test.lock));
    CHECK(__wt_try_readlock(test.session, &test.lock) == 0);
    CHECK(__wt_rwlock_islocked(test.session, &test.lock));
    __wt_readunlock(test.session, &test.lock);
    CHECK_FALSE(__wt_rwlock_islocked(test.session, &test.lock));

    CHECK(__wt_try_writelock(test.session, &test.lock) == 0);
    CHECK(__wt_rwlock_islocked(test.session, &test.lock));
    __wt_writeunlock(test.session, &test.lock);
    CHECK_FALSE(__wt_rwlock_islocked(test.session, &test.lock));

    __wt_readlock(test.session, &test.lock);
    CHECK(__wt_rwlock_islocked(test.session, &test.lock));
    __wt_readunlock(test.session, &test.lock);
    __wt_writelock(test.session, &test.lock);
    CHECK(__wt_rwlock_islocked(test.session, &test.lock));
    __wt_writeunlock(test.session, &test.lock);
    CHECK_FALSE(__wt_rwlock_islocked(test.session, &test.lock));
}

TEST_CASE("Read/write lock excludes writers from overlapping critical sections", "[rwlock]")
{
    bool second_writer = GENERATE(true, false);
    INFO("Second worker takes a " << (second_writer ? "write" : "read") << " lock");

    rwlock_test test;
    WT_SESSION_IMPL *other = test.connection.create_session();
    constexpr unsigned iterations = 1000;
    std::mutex mutex;
    std::condition_variable condition;
    unsigned ready = 0, inside = 0, value = 0;
    bool start = false, overlap = false, changed = false;

    auto worker = [&](WT_SESSION_IMPL *session, bool writer) {
        {
            std::unique_lock<std::mutex> guard(mutex);
            ++ready;
            condition.notify_all();
            condition.wait(guard, [&] { return start; });
        }

        for (unsigned i = 0; i < iterations; ++i) {
            if (writer)
                __wt_writelock(session, &test.lock);
            else
                __wt_readlock(session, &test.lock);

            unsigned snapshot;
            /* Keep the test's bookkeeping synchronized even if the target lock is broken. */
            {
                std::lock_guard<std::mutex> guard(mutex);
                overlap |= inside != 0;
                ++inside;
                snapshot = writer ? ++value : value;
            }

            std::this_thread::yield();

            {
                std::lock_guard<std::mutex> guard(mutex);
                changed |= value != snapshot;
                --inside;
            }

            if (writer)
                __wt_writeunlock(session, &test.lock);
            else
                __wt_readunlock(session, &test.lock);
        }
    };

    std::thread first(worker, test.session, true);
    std::thread second(worker, other, second_writer);
    bool both_ready;
    {
        std::unique_lock<std::mutex> guard(mutex);
        both_ready =
          condition.wait_for(guard, std::chrono::seconds(10), [&] { return ready == 2; });
        start = true;
    }
    condition.notify_all();
    first.join();
    second.join();

    CHECK(both_ready);
    CHECK_FALSE(overlap);
    CHECK_FALSE(changed);
    CHECK(inside == 0);
    CHECK(value == iterations * (second_writer ? 2 : 1));
}

TEST_CASE("Read lock admits another reader but rejects an exclusive try lock", "[rwlock]")
{
    rwlock_test test;
    WT_SESSION_IMPL *other = test.connection.create_session();
    int reader_ret = WT_ERROR, writer_ret = WT_ERROR;

    __wt_readlock(test.session, &test.lock);
    std::thread worker([&] {
        reader_ret = __wt_try_readlock(other, &test.lock);
        if (reader_ret == 0)
            __wt_readunlock(other, &test.lock);
        writer_ret = __wt_try_writelock(other, &test.lock);
        if (writer_ret == 0)
            __wt_writeunlock(other, &test.lock);
    });
    worker.join();
    __wt_readunlock(test.session, &test.lock);

    CHECK(reader_ret == 0);
    CHECK(writer_ret == EBUSY);
    CHECK_FALSE(__wt_rwlock_islocked(test.session, &test.lock));
}

TEST_CASE(
  "Read/write lock rejects shared and exclusive try locks while a writer holds it", "[rwlock]")
{
    rwlock_test test;
    WT_SESSION_IMPL *other = test.connection.create_session();
    int reader_ret = WT_ERROR, writer_ret = WT_ERROR;

    __wt_writelock(test.session, &test.lock);
    std::thread worker([&] {
        reader_ret = __wt_try_readlock(other, &test.lock);
        if (reader_ret == 0)
            __wt_readunlock(other, &test.lock);
        writer_ret = __wt_try_writelock(other, &test.lock);
        if (writer_ret == 0)
            __wt_writeunlock(other, &test.lock);
    });
    worker.join();
    __wt_writeunlock(test.session, &test.lock);

    CHECK(reader_ret == EBUSY);
    CHECK(writer_ret == EBUSY);
    CHECK_FALSE(__wt_rwlock_islocked(test.session, &test.lock));
}

#if READ_WRITE_LOCK_TYPE == READ_WRITE_LOCK_CUSTOM
TEST_CASE("Tracked read/write lock counts uncontended writer acquisitions", "[rwlock]")
{
    connection_wrapper connection("WT_TEST_RWLOCK_COUNTS", "create,statistics=(all)");
    WT_SESSION_IMPL *session = connection.create_session();
    WT_RWLOCK lock{};
    auto initialize = [&]() -> int {
        WT_RWLOCK_INIT_TRACKED(session, &lock, truncate_list);
        return (0);
    };
    REQUIRE(initialize() == 0);
    lock.stat_session_usecs_off = -1;

    auto write_count = [&]() {
        WT_CURSOR *cursor;
        const char *description, *printable;
        int64_t value;
        REQUIRE(session->iface.open_cursor(
                  &session->iface, "statistics:", nullptr, nullptr, &cursor) == 0);
        cursor->set_key(cursor, WT_STAT_CONN_LOCK_TRUNCATE_LIST_WRITE_COUNT);
        REQUIRE(cursor->search(cursor) == 0);
        REQUIRE(cursor->get_value(cursor, &description, &printable, &value) == 0);
        REQUIRE(cursor->close(cursor) == 0);
        return (value);
    };

    int64_t before = write_count();
    __wt_writelock(session, &lock);
    __wt_writeunlock(session, &lock);
    CHECK(write_count() == before + 1);
    CHECK(__wt_try_writelock(session, &lock) == 0);
    __wt_writeunlock(session, &lock);
    CHECK(write_count() == before + 2);
    __wt_rwlock_destroy(session, &lock);
}
#endif

TEST_CASE("Read/write lock destruction consumes the caller's retained write lock", "[rwlock]")
{
    connection_wrapper connection("WT_TEST_RWLOCK_DESTROY", "create");
    WT_SESSION_IMPL *session = connection.create_session();
    WT_RWLOCK lock{};

    REQUIRE(__wt_rwlock_init(session, &lock) == 0);
    __wt_writelock(session, &lock);
    CHECK(__wt_rwlock_islocked(session, &lock));
    __wt_rwlock_destroy(session, &lock);

    REQUIRE(__wt_rwlock_init(session, &lock) == 0);
    CHECK(__wt_try_writelock(session, &lock) == 0);
    __wt_writeunlock(session, &lock);
    __wt_rwlock_destroy(session, &lock);
}
