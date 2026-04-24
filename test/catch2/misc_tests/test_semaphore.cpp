/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <atomic>
#include <list>
#include <thread>
#include <catch2/catch.hpp>
#include "wt_internal.h"
#include "../wrappers/mock_session.h"

TEST_CASE("Test Semaphore: Basic Operations", "[semaphore]")
{
    std::shared_ptr<mock_session> session_mock = mock_session::build_test_mock_session();

    WT_SEMAPHORE sem;
    WT_SESSION_IMPL *session = session_mock->get_wt_session_impl();

    SECTION("Initialize and destroy semaphore")
    {
        CHECK(__wt_semaphore_init(session, &sem, 0, "Test") == 0);
        CHECK(__wt_semaphore_destroy(session, &sem) == 0);
    }

    SECTION("Initialize and destroy semaphore, with non-zero initial count")
    {
        CHECK(__wt_semaphore_init(session, &sem, 5, "Test") == 0);
        for (int i = 0; i < 5; ++i)
            CHECK(__wt_semaphore_wait(session, &sem) == 0);
        CHECK(__wt_semaphore_destroy(session, &sem) == 0);
    }

    SECTION("Post and wait operations")
    {
        CHECK(__wt_semaphore_init(session, &sem, 0, "Test") == 0);
        CHECK(__wt_semaphore_post(session, &sem) == 0);
        CHECK(__wt_semaphore_wait(session, &sem) == 0);
        CHECK(__wt_semaphore_destroy(session, &sem) == 0);
    }

    SECTION("Multiple posts and waits")
    {
        CHECK(__wt_semaphore_init(session, &sem, 0, "Test") == 0);

        for (int i = 0; i < 5; ++i)
            CHECK(__wt_semaphore_post(session, &sem) == 0);
        for (int i = 0; i < 5; ++i)
            CHECK(__wt_semaphore_wait(session, &sem) == 0);

        CHECK(__wt_semaphore_destroy(session, &sem) == 0);
    }
}

TEST_CASE("Test Semaphore: Multi-threaded Operations", "[semaphore]")
{
    std::shared_ptr<mock_session> session_mock = mock_session::build_test_mock_session();

    WT_SEMAPHORE sem;
    WT_SESSION_IMPL *session = session_mock->get_wt_session_impl();
    std::atomic<int> counter(0);
    const int num_threads = 4;

    SECTION("Multiple threads waiting on semaphore")
    {
        CHECK(__wt_semaphore_init(session, &sem, 0, "Test") == 0);
        std::list<std::thread> threads;

        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&]() {
                CHECK(__wt_semaphore_wait(session, &sem) == 0);
                counter++;
            });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        for (int i = 0; i < num_threads; ++i)
            CHECK(__wt_semaphore_post(session, &sem) == 0);

        for (auto &t : threads)
            t.join();

        CHECK(counter == num_threads);
        CHECK(__wt_semaphore_destroy(session, &sem) == 0);
    }

    SECTION("Producer-consumer pattern")
    {
        CHECK(__wt_semaphore_init(session, &sem, 0, "Test") == 0);
        counter = 0;

        std::thread producer([&]() {
            for (int i = 0; i < 10; ++i)
                CHECK(__wt_semaphore_post(session, &sem) == 0);
        });

        std::thread consumer([&]() {
            for (int i = 0; i < 10; ++i) {
                CHECK(__wt_semaphore_wait(session, &sem) == 0);
                counter++;
            }
        });

        producer.join();
        consumer.join();

        CHECK(counter == 10);
        CHECK(__wt_semaphore_destroy(session, &sem) == 0);
    }
}
