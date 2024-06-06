/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <iostream>
#include <iomanip>
#include <list>
#include <memory>
#include <thread>

#include "wt_internal.h"
#include <catch2/catch.hpp>

namespace {
using futex_word = WT_FUTEX_WORD;

time_t
msec_to_usec(time_t msec)
{
    return (msec * 1000);
}

struct Waiter {
    futex_word val_on_wake;
    int ret;
    int eno;

    void
    wait_on(futex_word *addr, futex_word expected, time_t timeout)
    {
        ret = __wt_futex_wait(addr, expected, timeout, &val_on_wake);
        eno = errno;
    }
};

std::ostream &
operator<<(std::ostream &out, const Waiter &w)
{
    out << "WaitOutcome(" << w.val_on_wake << "  " << w.ret << "  " << w.eno << ")";
    return out;
}

struct Outcomes {
    unsigned int wakeup{0};
    unsigned int spurious{0};
    unsigned int timedout{0};
    unsigned int failed{0};
};

std::ostream &
operator<<(std::ostream &out, const Outcomes &o)
{
    out << "Outcomes(" << o.wakeup << "  " << o.spurious << "  " << o.timedout << "  " << o.failed
        << ")";
    return out;
}

template <typename Iter>
Outcomes
outcomes(futex_word wake_val, Iter beg, Iter end)
{
    Outcomes outcomes;
    for (auto &&w = beg; w < end; w++)
        if (w->ret == 0) {
            if (w->val_on_wake == wake_val)
                outcomes.wakeup++;
            else
                outcomes.spurious++;
        } else {
            if (w->eno == ETIMEDOUT)
                outcomes.timedout++;
            else
                outcomes.failed++;
        }
    return outcomes;
}

} // namespace

TEST_CASE("wake one", "[futex]")
{
    futex_word ftx;
    std::vector<Waiter> waiters(1);
    std::list<std::thread> threads;
    ftx = 1234;
    for (auto &w : waiters)
        threads.push_back(std::thread(&Waiter::wait_on, &w, &ftx, 1234, msec_to_usec(300)));
    std::this_thread::sleep_for(std::chrono::duration<time_t, std::milli>(100));
    REQUIRE(__wt_futex_wake(&ftx, WT_FUTEX_WAKE_ONE, 4321) == 0);
    for (auto &t : threads)
        t.join();
    CHECK(waiters[0].val_on_wake == 4321);
}

TEST_CASE("timeout one", "[futex]")
{
    futex_word ftx;
    std::vector<Waiter> waiters(1);
    std::list<std::thread> threads;
    ftx = 0;
    for (auto &&w : waiters)
        threads.push_back(std::thread(&Waiter::wait_on, &w, &ftx, 0, msec_to_usec(300)));
    std::this_thread::sleep_for(std::chrono::duration<time_t, std::milli>(100));
    for (auto &&t : threads)
        t.join();
    CHECK(waiters[0].eno == ETIMEDOUT);
}

TEST_CASE("wake one of two", "[futex]")
{
    const futex_word expected = 89349;
    const futex_word wake_val = 9728;
    futex_word ftx;
    std::vector<Waiter> waiters(2);
    std::list<std::thread> threads;
    ftx = expected;
    for (auto &&w : waiters)
        threads.push_back(std::thread(&Waiter::wait_on, &w, &ftx, expected, msec_to_usec(300)));
    std::this_thread::sleep_for(std::chrono::duration<time_t, std::milli>(100));
    REQUIRE(__wt_futex_wake(&ftx, WT_FUTEX_WAKE_ONE, wake_val) == 0);
    for (auto &&t : threads)
        t.join();
    auto out = outcomes(wake_val, waiters.begin(), waiters.end());
    CHECK(out.timedout <= 1);
    CHECK(out.wakeup <= 1);
}

TEST_CASE("wake all of two", "[futex]")
{
    const futex_word expected = 779109;
    const futex_word wake_val = 51103;
    futex_word ftx;
    std::vector<Waiter> waiters(2);
    std::list<std::thread> threads;
    ftx = expected;
    for (auto &&w : waiters)
        threads.push_back(std::thread(&Waiter::wait_on, &w, &ftx, expected, msec_to_usec(300)));
    std::this_thread::sleep_for(std::chrono::duration<time_t, std::milli>(100));
    REQUIRE(__wt_futex_wake(&ftx, WT_FUTEX_WAKE_ALL, wake_val) == 0);
    for (auto &&t : threads)
        t.join();
    auto outs = outcomes(wake_val, waiters.begin(), waiters.end());
    CAPTURE(waiters);
    CAPTURE(outs);
    CHECK((outs.wakeup + outs.spurious) == 2);
}

TEST_CASE("wake three separately", "[futex]")
{
    const futex_word expected = 7011900;
    const std::vector<futex_word> wake_vals{{11819, 3249384, 422992}};
    futex_word ftx;
    std::vector<Waiter> waiters(3);
    std::list<std::thread> threads;
    ftx = expected;
    for (auto &&w : waiters)
        threads.push_back(std::thread(&Waiter::wait_on, &w, &ftx, expected, msec_to_usec(600)));
    for (auto &&wval : wake_vals) {
        std::this_thread::sleep_for(std::chrono::duration<time_t, std::milli>(100));
        REQUIRE(__wt_futex_wake(&ftx, WT_FUTEX_WAKE_ONE, wval) == 0);
    }
    for (auto &&t : threads)
        t.join();
    CAPTURE(waiters);
    for (auto &&wval : wake_vals) {
        auto outs = outcomes(wval, waiters.begin(), waiters.end());
        CAPTURE(outs);
        CHECK(outs.wakeup <= 1);
        if (outs.wakeup < 1)
            CHECK(outs.spurious > 0);
    }
}
