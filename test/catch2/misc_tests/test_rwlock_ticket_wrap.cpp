/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <chrono>
#include <thread>
#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "../wrappers/mock_session.h"

TEST_CASE("Test rwlock: reader queues behind the writer holding ticket 255", "[rwlock]")
{
    std::shared_ptr<mock_session> writer_mock = mock_session::build_test_mock_session();
    std::shared_ptr<mock_session> reader_mock = mock_session::build_test_mock_session();
    WT_SESSION_IMPL *writer_session = writer_mock->get_wt_session_impl();
    WT_SESSION_IMPL *reader_session = reader_mock->get_wt_session_impl();
    WT_RWLOCK lock, state;

    REQUIRE(__wt_rwlock_init(writer_session, &lock) == 0);

    for (int i = 0; i < 255; ++i) {
        __wt_writelock(writer_session, &lock);
        __wt_writeunlock(writer_session, &lock);
    }
    __wt_writelock(writer_session, &lock);
    REQUIRE(lock.u.s.current == 255);
    REQUIRE(lock.u.s.next == 0);

    std::thread reader([&]() {
        __wt_readlock(reader_session, &lock);
        __wt_readunlock(reader_session, &lock);
    });

    /* A reader that does not queue sleeps on the condition variable until the writer is done. */
    bool queued = false;
    for (auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
      !queued && std::chrono::steady_clock::now() < deadline;) {
        state.u.v = __wt_atomic_load_uint64_v_relaxed(&lock.u.v);
        queued = state.u.s.readers_queued == 1;
        if (!queued)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    __wt_writeunlock(writer_session, &lock);
    reader.join();
    CHECK(queued);

    __wt_rwlock_destroy(writer_session, &lock);
}
