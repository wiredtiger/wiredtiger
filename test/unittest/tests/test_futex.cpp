/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <iostream>
#include <iomanip>
#include <memory>
#include <thread>

#include "wt_internal.h"
#include <catch2/catch.hpp>

using futex_word = WT_FUTEX_WORD;

namespace {
time_t
msec_to_usec(time_t msec)
{
    return msec * 1000;
}


struct Wakeup {
    int ret;
    int eno;
    futex_word actual;

    bool isWakeup(futex_word wakeup_val) const { return ret == 0 && eno == 0 && actual == wakeup_val; }
    bool isTimeout() const { return ret == -1 && eno == ETIMEDOUT; }
    bool isSpurious() const { return ret == -1 && (eno == EAGAIN || eno == EINTR); }
    bool isError() const { return ret == -1 && eno != ETIMEDOUT && eno != EAGAIN && eno != EINTR; }

    friend std::ostream& operator<<(std::ostream& out, const Wakeup &wakeup);
};

std::ostream& operator<<(std::ostream& out, const Wakeup &wakeup)
{
    out << "Wakeup(" << wakeup.ret << ", " << wakeup.eno << ", " << wakeup.actual << ")";
    return out;
}

bool operator==(const Wakeup &lhs, const Wakeup &rhs) noexcept
{
    return lhs.ret == rhs.ret && lhs.eno == rhs.eno && lhs.actual == rhs.actual;
}

class Waiter {
    futex_word *_futex;
    Wakeup _wakeup;

    public:
    Waiter(futex_word *futex) : _futex(futex) {}

    void futex_wait()
    {
        futex_word _expected = 0;
        auto _timeout = msec_to_usec(100);
        futex_word actual = 0;
        int ret = __wt_futex_wait(_futex, _expected, _timeout, &actual);
        _wakeup = { ret, errno, actual };
    }

    Wakeup wakeup() const { return _wakeup; }
};

std::ostream& operator<<(std::ostream& out, const Waiter &waiter)
{
    out << "Waiter(wakeup=" << waiter.wakeup() << ")";
    return out;
}


struct WakeupResults {
    int wakeups{ 0 };
    int timeouts{ 0 };
    int spurious{ 0 };
    int errored{ 0 };
};

std::ostream& operator<<(std::ostream& out, const WakeupResults &res)
{
    out << "Results(" << res.wakeups << ", " << res.timeouts << ", " << res.spurious << ", " << res.errored << ")";
    return out;
}

template<typename Iter>
WakeupResults CollectResults(futex_word wake_val, Iter begin, Iter end)
{
    WakeupResults results;
    for (auto b = begin; b < end; b++) {
        auto&& w = b->wakeup();
        if (w.isWakeup(wake_val))
            results.wakeups++;
        else if (w.isTimeout())
            results.timeouts++;
        else if (w.isSpurious())
            results.spurious++;
        else
            results.errored++;
    }
    return results;
}

} // namespace

TEST_CASE("wake one", "[futex]")
{
    futex_word test_futex(0);
    Waiter waiter(&test_futex);
    std::thread t1(&Waiter::futex_wait, &waiter);
    std::this_thread::sleep_for(std::chrono::duration<time_t, std::milli>(50));
    REQUIRE(__wt_futex_wake(&test_futex, WT_FUTEX_WAKE_ONE, 0x1234) == 0);
    t1.join();
    REQUIRE(waiter.wakeup().isWakeup(0x1234));
}

TEST_CASE("timeout one", "[futex]")
{
    futex_word test_futex(0);
    Waiter waiter(&test_futex);
    std::thread t1(&Waiter::futex_wait, &waiter);
    std::this_thread::sleep_for(std::chrono::duration<time_t, std::milli>(50));
    t1.join();
    REQUIRE(waiter.wakeup().isTimeout());
}

TEST_CASE("wake one of two", "[futex]")
{
    futex_word test_futex(0);
    std::vector<Waiter> waiters(2, &test_futex);
    std::vector<std::thread> threads;
    for (auto&& w : waiters)
        threads.push_back(std::thread{&Waiter::futex_wait, &w});
    std::this_thread::sleep_for(std::chrono::duration<time_t, std::milli>(50));
    REQUIRE(__wt_futex_wake(&test_futex, WT_FUTEX_WAKE_ONE, 0x1234) == 0);
    for (auto&& t : threads)
        t.join();
    auto results = CollectResults(0x1234, waiters.begin(), waiters.end());
    CAPTURE(waiters);
    REQUIRE(results.wakeups == 1);
    CAPTURE(results);
    REQUIRE(results.timeouts == 1);
}

TEST_CASE("wake all of two", "[futex]")
{
    futex_word test_futex(0);
    std::vector<Waiter> waiters(2, &test_futex);
    std::vector<std::thread> threads;
    for (auto&& w : waiters)
        threads.push_back(std::thread{&Waiter::futex_wait, &w});
    std::this_thread::sleep_for(std::chrono::duration<time_t, std::milli>(50));
    REQUIRE(__wt_futex_wake(&test_futex, WT_FUTEX_WAKE_ALL, 0x1234) == 0);
    for (auto&& t : threads)
        t.join();
    CAPTURE(waiters);
    auto results = CollectResults(0x1234, waiters.begin(), waiters.end());
    CAPTURE(results);
    REQUIRE(results.wakeups == 2);
}
