/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include <iomanip>
#include <iostream>
#include <limits>
#include <list>
#include <memory>
#include <thread>

#include "wt_internal.h"
#include <catch2/catch.hpp>

using namespace std;

namespace {
using futex_word = WT_FUTEX_WORD;
using uint = unsigned int;

time_t
msec_to_usec(time_t msec)
{
    return (msec * 1000);
}

class IncreasingTimeout {
    time_t _usec;

public:
    static const unsigned int DefaultInitialScaleup = 3;
    float GrowthFactor = 1.5f;

    IncreasingTimeout(const chrono::duration<time_t, milli> &wake_delay,
      unsigned int scaleup = DefaultInitialScaleup)
    {
        auto tmp = chrono::duration_cast<chrono::microseconds>(wake_delay);
        assert(tmp.count() * scaleup <= std::numeric_limits<time_t>::max());
        _usec = tmp.count() * scaleup;
    }

    time_t
    operator()(int iteration)
    {
        return _usec * (GrowthFactor * (iteration + 1));
    }
};

struct Waiter {
    futex_word expected;
    futex_word val_on_wake{0};
    int ret{0};
    int eno{0};

    explicit Waiter(futex_word expect) : expected(expect), val_on_wake(expect) {}

    void
    wait_on(futex_word *addr, time_t timeout)
    {
        ret = __wt_futex_wait(addr, expected, timeout, &val_on_wake);
        eno = errno;
    }
};

ostream &
operator<<(ostream &out, const Waiter &w)
{
    out << "Waiter(" << w.expected << ", " << w.val_on_wake << ",  " << w.ret << ",  " << w.eno
        << ")";
    return out;
}

enum Outcome {
    /*
     * Wake ups and timeouts match the test expectations.
     */
    Expected,

    /*
     * Spurious wakeups were detected, but any other results look valid. So don't fail the test, but
     * retry for an expected outcome.
     */
    Acceptable,

    /* Waiting on the futex returned an error. */
    Error,

    /* Return value was zero, but errno is non-zero, which is unexpected. */
    RetZeroWithErrno,

    /*
     * A thread timed out, but the futex value changed. This is an error because the tests
     * are setup to specifically preclude this possibility. In practice there is nothing
     * to stop another thread updating the futex value without calling wake.
     */
    TimeoutWithUnexpecedValue,

    /*
     * The expected wake up value is not present. This could because there was no such
     * value wakeup value list, or more waiters than expected were woken with the same value.
     */
    WakeWithUnexpectedValue,

    /* An unexpected non-spurious wake up is present. */
    UnexpectWakeNonSpuriousWakeup,

    /* Unlike spurious wakeups, consider unexpected timeouts as a test failure. */
    UnexpectedTimeouts,
};

bool
OutcomeNotFailure(Outcome result)
{
    return result == Outcome::Expected || result == Outcome::Acceptable;
}

ostream &
operator<<(ostream &out, const Outcome &result)
{
#ifdef R_
#error "R_ already defined"
#endif

#define R_(r)      \
    case r:        \
        out << #r; \
        break

    switch (result) {
        R_(Expected);
        R_(Acceptable);
        R_(Error);
        R_(RetZeroWithErrno);
        R_(TimeoutWithUnexpecedValue);
        R_(WakeWithUnexpectedValue);
        R_(UnexpectWakeNonSpuriousWakeup);
        R_(UnexpectedTimeouts);
    }

#undef R_
    return out;
}
/*
 * CheckOutcomes --
 *      Check completed waiters to see if they meet expectations.
 *
 * There must be a SEPARATE wake up value for each and every thread that is
 * expected to be awoken.
 */
Outcome
CheckOutcomes(
  const vector<Waiter> &waiters, const unsigned int timeouts, const vector<futex_word> &wake_vals)
{
    list<futex_word> wake_values(wake_vals.begin(), wake_vals.end());
    unsigned int timedout = 0;

    /*
     * Loop over the waiters looking for problems.
     *
     * Note: spurious wakeups (expected == wake up value, and no errors) are detected after the loop
     * exits so there are is no test in the loop.
     */
    for (auto &&w : waiters) {
        if (w.ret == -1) {
            if (w.eno != ETIMEDOUT)
                return Outcome::Error;
            else if (w.val_on_wake != w.expected)
                return Outcome::TimeoutWithUnexpecedValue;
            timedout++;
        } else if (w.eno != 0)
            return Outcome::RetZeroWithErrno;
        else if (wake_values.empty() && w.val_on_wake != w.expected)
            return Outcome::UnexpectWakeNonSpuriousWakeup;
        else if (w.val_on_wake != w.expected) {
            /* This is a non-spurious wake up. */

            auto match = find(wake_values.begin(), wake_values.end(), w.val_on_wake);
            if (match == wake_values.end())
                return Outcome::WakeWithUnexpectedValue;
            wake_values.erase(match);
        }
    }

    if (timedout > timeouts) {
        return UnexpectedTimeouts;
    }

    if (wake_values.empty() && timedout == timeouts)
        return Outcome::Expected;

    /*
     * Spurious wake ups detected: while this is not an error, is not a successful test.
     */
    return Outcome::Acceptable;
}

} // namespace

class FutexTester {
public:
    chrono::duration<time_t, milli> WAKE_DELAY{100};
    chrono::duration<time_t, milli> INTER_WAKE_DELAY{1};

    futex_word _futex;
    vector<Waiter> _waiters;
    vector<thread> _threads;

    void
    start_waiters(futex_word expected, time_t timeout_usec)
    {
        _futex = expected; // make atomic
        for (auto &w : _waiters)
            _threads.push_back(thread(&Waiter::wait_on, &w, &_futex, timeout_usec));
    }

    void
    delay_then_wake(const vector<pair<WT_FUTEX_WAKE, futex_word>> &wake_signals)
    {
        this_thread::sleep_for(WAKE_DELAY);
        for (auto &&sig : wake_signals) {
            REQUIRE(__wt_futex_wake(&_futex, sig.first, sig.second) == 0);
            this_thread::sleep_for(INTER_WAKE_DELAY);
        }
    }

    void
    wait_and_check(const vector<pair<WT_FUTEX_WAKE, futex_word>> &wake_signals)
    {
        for (auto &&t : _threads)
            t.join();
        CAPTURE(_waiters);

        std::vector<futex_word> wake_vals;
        std::for_each(wake_signals.begin(), wake_signals.end(),
          [&wake_vals](
            const pair<WT_FUTEX_WAKE, futex_word> &info) { wake_vals.push_back(info.second); });
        CAPTURE(wake_vals);

        uint expected_timeouts = _threads.size() - wake_vals.size();
        auto result = CheckOutcomes(_waiters, expected_timeouts, wake_vals);
        CAPTURE(result);
        REQUIRE(OutcomeNotFailure(result));
    }
};

class FutexTestFixture {
    int RETRY_MAX = 3;

public:
    FutexTestFixture() {}

    void
    wake_one()
    {
        vector<pair<WT_FUTEX_WAKE, futex_word>> wake_info{{WT_FUTEX_WAKE_ONE, 1234}};

        FutexTester tester;
        tester._waiters.push_back(Waiter(4321));
        tester.start_waiters(4321, msec_to_usec(300));
        tester.delay_then_wake(wake_info);
        tester.wait_and_check(wake_info);
    }

    void
    timeout_one()
    {
        FutexTester tester;
        tester._waiters.push_back(Waiter(0));
        tester.start_waiters(0, msec_to_usec(200));
        tester.wait_and_check({});
    }

    void
    wake_one_of_two()
    {
        vector<pair<WT_FUTEX_WAKE, futex_word>> wake_info{{WT_FUTEX_WAKE_ONE, 1111}};
        
        FutexTester tester;
        tester._waiters.push_back(Waiter(89349));
        tester._waiters.push_back(Waiter(89349));
        tester.start_waiters(89349, msec_to_usec(300));
        tester.delay_then_wake(wake_info);
        tester.wait_and_check(wake_info);
    }

    void
    wake_two_of_two()
    {
        vector<pair<WT_FUTEX_WAKE, futex_word>> wake_info{{WT_FUTEX_WAKE_ONE, 6928374}};

        FutexTester tester;
        tester._waiters.push_back(Waiter(32234));
        tester._waiters.push_back(Waiter(32234));
        tester.start_waiters(32234, msec_to_usec(300));
        tester.delay_then_wake(wake_info);
        tester.wait_and_check(wake_info);
    }

    void
    wake_three_individually()
    {
        vector<pair<WT_FUTEX_WAKE, futex_word>> wake_info{
          {WT_FUTEX_WAKE_ONE, 234234}, {WT_FUTEX_WAKE_ONE, 45675}, {WT_FUTEX_WAKE_ONE, 239043820}};
        FutexTester tester;
        tester._waiters.push_back(Waiter(5644));
        tester._waiters.push_back(Waiter(5644));
        tester._waiters.push_back(Waiter(5644));
        tester.start_waiters(5644, msec_to_usec(300));
        tester.delay_then_wake(wake_info);
        tester.wait_and_check(wake_info);
    }
};

TEST_CASE_METHOD(FutexTestFixture, "Wake one", "[futex2]")
{
    wake_one();
}

TEST_CASE_METHOD(FutexTestFixture, "Timeout one", "[futex2]")
{
    timeout_one();
}

TEST_CASE_METHOD(FutexTestFixture, "Wake one of two", "[futex2]")
{
    wake_one_of_two();
}

TEST_CASE_METHOD(FutexTestFixture, "Wake two of two", "[futex2]")
{
    wake_two_of_two();
}

TEST_CASE_METHOD(FutexTestFixture, "Wake three individually", "[futex2]")
{
    wake_three_individually();
}
