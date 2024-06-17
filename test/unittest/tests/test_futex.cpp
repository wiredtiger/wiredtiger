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

struct waiter {
    futex_word _expected;
    futex_word _val_on_wake{0};
    int _ret{0};
    int _eno{0};

    waiter() = delete;

    explicit waiter(futex_word expect) : _expected(expect), _val_on_wake(expect) {}

    void
    wait_on(futex_word *addr, time_t timeout)
    {
        _ret = __wt_futex_wait(addr, _expected, timeout, &_val_on_wake);
        _eno = errno;
    }
};

ostream &
operator<<(ostream &out, const waiter &w)
{
    out << "Waiter(" << w._expected << ", " << w._val_on_wake << ",  " << w._ret << ",  " << w._eno
        << ")";
    return out;
}

enum outcome {
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
    TimeoutWithUnexpectedValue,

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

ostream &
operator<<(ostream &out, const outcome &result)
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
        R_(TimeoutWithUnexpectedValue);
        R_(WakeWithUnexpectedValue);
        R_(UnexpectWakeNonSpuriousWakeup);
        R_(UnexpectedTimeouts);
    }

#undef R_
    return out;
}

struct wake_signal {
    WT_FUTEX_WAKE _type;
    futex_word _value;

    wake_signal(WT_FUTEX_WAKE t, futex_word v) : _type(t), _value(v) {}
};

struct wake_one : public wake_signal {
    wake_one(futex_word val) : wake_signal(WT_FUTEX_WAKE_ONE, val) {}
};

struct wake_all : public wake_signal {
    wake_all(futex_word val) : wake_signal(WT_FUTEX_WAKE_ALL, val) {}
};

} // namespace

/*
 * Reify for each test run: do NOT reuse.
 */
class futex_tester {
public:
    chrono::duration<time_t, milli> WAKE_DELAY{100};
    chrono::duration<time_t, milli> INTER_WAKE_DELAY{1};

    futex_word _futex;
    vector<waiter> _waiters;
    vector<thread> _threads;

    void
    create_waiters(size_t count, futex_word expected)
    {
        for (size_t i = 0; i < count; i++)
            _waiters.push_back(waiter(expected));
    }

    void
    start_waiters(futex_word expected, time_t timeout_usec)
    {
        futex_atomic_store(&_futex, expected);
        for (auto &w : _waiters)
            _threads.push_back(thread(&waiter::wait_on, &w, &_futex, timeout_usec));
    }

    void
    delay_then_wake(time_t wake_start_delay_usec, const vector<wake_signal> &wake_signals)
    {
        chrono::duration<time_t, micro> WAKE_DELAY{wake_start_delay_usec};
        this_thread::sleep_for(WAKE_DELAY);
        for (auto &&sig : wake_signals) {
            REQUIRE(__wt_futex_wake(&_futex, sig._type, sig._value) == 0);
            this_thread::sleep_for(INTER_WAKE_DELAY);
        }
    }

    void
    wait_and_check(const vector<wake_signal> &wake_signals)
    {
        size_t expected_timeouts;

        for (auto &&t : _threads)
            t.join();
        CAPTURE(_waiters);

        /* Infer the number of expected timeouts. */
        if (wake_signals.empty())
            expected_timeouts = _threads.size();
        else if (wake_signals.begin()->_type == WT_FUTEX_WAKE_ALL)
            expected_timeouts = 0;
        else
            expected_timeouts = _threads.size() - wake_signals.size();

        auto result = check_outcomes(expected_timeouts, wake_signals);
        CAPTURE(result);
        REQUIRE((result == outcome::Expected || result == outcome::Acceptable));
    }

    /*
     * check_outcomes --
     *     Check completed waiters to see if they meet expectations.
     *
     * There must be either a WakeAll signal or a WakeOne signal for every waiter that is expected
     *     to be awoken.
     */
    outcome
    check_outcomes(const size_t timeouts, const vector<wake_signal> &wake_sigs)
    {
        list<wake_signal> wakes(wake_sigs.begin(), wake_sigs.end());
        size_t timedout = 0;

        /*
         * Loop over the waiters and inspect their state.
         *
         * Note: spurious wakeups (no error and expected == wake up value) are detected after the
         * loop exits so there are is no test in the loop.
         */
        for (auto &&w : _waiters) {
            if (w._ret == -1) {
                if (w._eno != ETIMEDOUT)
                    return outcome::Error;
                else if (w._val_on_wake != w._expected)
                    return outcome::TimeoutWithUnexpectedValue;
                timedout++;
            } else if (w._eno != 0)
                return outcome::RetZeroWithErrno;
            else if (wakes.empty() && w._val_on_wake != w._expected)
                return outcome::UnexpectWakeNonSpuriousWakeup;
            else if (w._val_on_wake != w._expected) {
                /* This is a non-spurious wake up. */
                auto match = find_if(wakes.begin(), wakes.end(),
                  [&w](const wake_signal &s) { return s._value == w._val_on_wake; });
                if (match == wakes.end())
                    return outcome::WakeWithUnexpectedValue;
                if (match->_type == WT_FUTEX_WAKE_ONE)
                    wakes.erase(match);
            }
        }

        if (timedout > timeouts) {
            return UnexpectedTimeouts;
        }

        if (wakes.empty() && timedout == timeouts)
            return outcome::Expected;

        /*
         * Spurious wake ups detected: while this is not an error, is not a consider successful.
         */
        return outcome::Acceptable;
    }
};

TEST_CASE("Wake one", "[futex]")
{
    vector<wake_signal> wake_info{{WT_FUTEX_WAKE_ONE, 1234}};

    futex_tester tester;
    tester._waiters.push_back(waiter(4321));
    tester.start_waiters(4321, msec_to_usec(300));
    tester.delay_then_wake(msec_to_usec(100), wake_info);
    tester.wait_and_check(wake_info);
}

TEST_CASE("Timeout one", "[futex]")
{
    futex_tester tester;
    tester._waiters.push_back(waiter(0));
    tester.start_waiters(0, msec_to_usec(200));
    tester.wait_and_check({});
}

TEST_CASE("Wake one of two", "[futex]")
{
    vector<wake_signal> wake_info{wake_one(1111)};

    futex_tester tester;
    tester.create_waiters(2, 89349);
    tester.start_waiters(89349, msec_to_usec(300));
    tester.delay_then_wake(msec_to_usec(100), wake_info);
    tester.wait_and_check(wake_info);
}

TEST_CASE("Wake two of two", "[futex]")
{
    vector<wake_signal> wake_info{wake_all(6928374)};

    futex_tester tester;
    tester.create_waiters(2, 32234);
    tester.start_waiters(32234, msec_to_usec(450));
    tester.delay_then_wake(msec_to_usec(100), wake_info);
    tester.wait_and_check(wake_info);
}

TEST_CASE("Wake three separately", "[futex]")
{
    vector<wake_signal> wake_info{wake_one(234234), wake_one(45675), wake_one(239043820)};

    futex_tester tester;
    tester.create_waiters(3, 5644);
    tester.start_waiters(5644, msec_to_usec(300));
    tester.delay_then_wake(msec_to_usec(100), wake_info);
    tester.wait_and_check(wake_info);
}
