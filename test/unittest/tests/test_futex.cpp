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

void
futex_atomic_store(futex_word *addr, futex_word value)
{
#ifdef _WIN32
    InterlockedExchange(addr, value);
#else
    __atomic_store_n(addr, value, __ATOMIC_SEQ_CST);
#endif
}

time_t
msec_to_usec(time_t msec)
{
    return (msec * 1000);
}

struct Waiter {
    futex_word expected;
    futex_word val_on_wake{0};
    int ret{0};
    int eno{0};

    Waiter() = delete;

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
     * Results are as expected so the test was successfully.
     */
    Expected,

    /*
     * Results contain spurious wakeups, but are otherwise valid. Don't fail the test, but prefer to
     * retry for an expected outcome.
     */
    Acceptable,

    /* Waiting on the futex returned an error. */
    Error,

    /* Return value was zero, but errno is non-zero. */
    RetZeroWithErrno,

    /*
     * A waiter timed out, but the futex value changed. The tests do not change the futex value with
     * out then calling wake so treat as an error.
     */
    TimeoutWithUnexpecedValue,

    /*
     * This could because this value was never present in the wakeup value list, or more waiters
     * than expected were awoken with the same value.
     */
    WakeWithUnexpectedValue,

    /* An unexpected non-spurious wake up is present. This is almost certainly a bug in the test. */
    UnexpectWakeNonSpuriousWakeup,

    /* Unexpected timeouts are a different type of soft failure to spurious wakeups. */
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
 * check_outcomes --
 *     Check completed waiters to see if they meet expectations.
 *
 * There must be a SEPARATE wake up value for each and every waiter that is expected to be awoken.
 */
Outcome
check_outcomes(
  const vector<Waiter> &waiters, const unsigned int timeouts, const vector<futex_word> &wake_vals)
{
    list<futex_word> wake_values(wake_vals.begin(), wake_vals.end());
    unsigned int timedout = 0;

    /*
     * Loop over the waiters and inspect their state.
     *
     * Note: spurious wakeups (no error and expected == wake up value) are detected after the loop
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
     * Spurious wake ups detected: while this is not an error, is not a consider successful.
     */
    return Outcome::Acceptable;
}

} // namespace

/*
 * Reify for each test run: do NOT reuse.
 */
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
        futex_atomic_store(&_futex, expected);
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

        size_t expected_timeouts = _threads.size() - wake_vals.size();
        auto result = check_outcomes(_waiters, expected_timeouts, wake_vals);
        CAPTURE(result);
        REQUIRE(OutcomeNotFailure(result));
    }
};

TEST_CASE("Wake one", "[futex]")
{
    vector<pair<WT_FUTEX_WAKE, futex_word>> wake_info{{WT_FUTEX_WAKE_ONE, 1234}};

    FutexTester tester;
    tester._waiters.push_back(Waiter(4321));
    tester.start_waiters(4321, msec_to_usec(300));
    tester.delay_then_wake(wake_info);
    tester.wait_and_check(wake_info);
}

TEST_CASE("Timeout one", "[futex]")
{
    FutexTester tester;
    tester._waiters.push_back(Waiter(0));
    tester.start_waiters(0, msec_to_usec(200));
    tester.wait_and_check({});
}

TEST_CASE("Wake one of two", "[futex]")
{
    vector<pair<WT_FUTEX_WAKE, futex_word>> wake_info{{WT_FUTEX_WAKE_ONE, 1111}};

    FutexTester tester;
    tester._waiters.push_back(Waiter(89349));
    tester._waiters.push_back(Waiter(89349));
    tester.start_waiters(89349, msec_to_usec(300));
    tester.delay_then_wake(wake_info);
    tester.wait_and_check(wake_info);
}

TEST_CASE("Wake two of two", "[futex]")
{
    vector<pair<WT_FUTEX_WAKE, futex_word>> wake_info{{WT_FUTEX_WAKE_ONE, 6928374}};

    FutexTester tester;
    tester._waiters.push_back(Waiter(32234));
    tester._waiters.push_back(Waiter(32234));
    tester.start_waiters(32234, msec_to_usec(300));
    tester.delay_then_wake(wake_info);
    tester.wait_and_check(wake_info);
}

TEST_CASE("Wake three separately", "[futex]")
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
