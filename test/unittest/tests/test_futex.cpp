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
using namespace std::placeholders;

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

    bool
    error() const
    {
        return _ret != 0 && _eno != ETIMEDOUT;
    }

    bool
    timedout() const
    {
        return _ret != 0 && _eno == ETIMEDOUT;
    }

    bool
    awoken(const futex_word wake_val) const
    {
        return _ret == 0 && _val_on_wake == wake_val;
    }

    bool
    spurious(const futex_word wake_val) const
    {
        return _ret == 0 && _val_on_wake != wake_val;
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
    AsExpected,         /* Wake ups and timeouts are as expected. */
    SpuriousWakeups,    /* Spurious timeouts were present. */
    Error,              /* One or more waiters encountered an error other than timeout. */
    UnexpectedTimeouts, /* More timeouts than expected. */

    /* The following is a test implementation error. */
    LostWakeup /* Waiter awoken without corresponding wake up signal. */
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
        R_(AsExpected);
        R_(SpuriousWakeups);
        R_(Error);
        R_(UnexpectedTimeouts);
        R_(LostWakeup);
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

bool
is_wake_all(const wake_signal &wsig)
{
    return wsig._type == WT_FUTEX_WAKE_ALL;
}

} // namespace

/*
 * Reify for each test run: do NOT reuse.
 */
class futex_tester {
public:
    const chrono::duration<time_t, milli> INTER_WAKE_DELAY{1};

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
        for (auto &&t : _threads)
            t.join();
        CAPTURE(_waiters);

        auto result = inspect_waiters(wake_signals);
        CAPTURE(result);
        REQUIRE((result == outcome::AsExpected || result == outcome::SpuriousWakeups));
    }

    outcome
    inspect_waiters(const vector<wake_signal> &wake_sigs)
    {
        /* Test validity check. */
        REQUIRE(_waiters.size() >= wake_sigs.size());

        auto wbeg = _waiters.cbegin();
        auto wend = _waiters.cend();

        /* Presence of any error other than timeout is failure. */
        auto err_cnt = count_if(wbeg, wend, bind(&waiter::error, _1));
        if (err_cnt > 0) {
            return (outcome::Error);
        }

        /*
         * If "wake all" is being tested it is expected to be the only signal: use a simplified
         * method to determine outcome.
         */
        if (find_if(wake_sigs.begin(), wake_sigs.end(), is_wake_all) != wake_sigs.end()) {
            REQUIRE(wake_sigs.size() == 1);
            auto sig = wake_sigs.front();
            auto wake_cnt = count_if(wbeg, wend, bind(&waiter::awoken, _1, sig._value));
            auto spurious_cnt = count_if(wbeg, wend, bind(&waiter::spurious, _1, sig._value));
            REQUIRE(((wake_cnt + spurious_cnt) == _waiters.size()));
            return (spurious_cnt > 0) ? outcome::SpuriousWakeups : outcome::AsExpected;
        }

        /* Account for any expected timeouts. */
        list<waiter> rem_waiters{_waiters.begin(), _waiters.end()};
        auto timeout_cnt = count_if(wbeg, wend, bind(&waiter::timedout, _1));
        if (timeout_cnt > 0) {
            /*
             * The timeout count should match the difference in the number of explicit wake up
             * signals and waiters.
             */
            if (timeout_cnt != (_waiters.size() - wake_sigs.size())) {
                return (outcome::UnexpectedTimeouts);
            }
            /* Now that timeouts are accounted for, they are no longer of interest. */
            rem_waiters.remove_if(bind(&waiter::timedout, _1));
        }

        /* Match remaining explicit wake ups with remaining waiters. */
        bool spurious_wakeups = false;
        list<wake_signal> rem_sigs{wake_sigs.begin(), wake_sigs.end()};
        while (!rem_sigs.empty() && !rem_waiters.empty()) {
            auto sig = rem_sigs.front();
            auto match = find_if(
              rem_waiters.begin(), rem_waiters.end(), bind(&waiter::awoken, _1, sig._value));

            if (match == rem_waiters.end()) {
                /*
                 * No matching waiter for the wake up, so there should be a waiter that awoke
                 * spuriously.
                 */
                auto spurious = find_if(
                  rem_waiters.begin(), rem_waiters.end(), bind(&waiter::spurious, _1, sig._value));
                if (spurious == rem_waiters.end()) {
                    return (outcome::LostWakeup);
                }
                rem_waiters.erase(spurious);
                spurious_wakeups = true;
            } else {
                rem_waiters.erase(match);
            }
            rem_sigs.pop_front();
        }

        /* Test validation check. */
        REQUIRE((rem_waiters.empty() && rem_sigs.empty()));

        return ((spurious_wakeups) ? outcome::SpuriousWakeups : outcome::AsExpected);
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
