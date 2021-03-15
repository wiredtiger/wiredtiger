/*-
 * Public Domain 2014-present MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef TIMESTAMP_MANAGER_H
#define TIMESTAMP_MANAGER_H

#include <atomic>
#include <chrono>
#include <thread>

namespace test_harness {
/*
 * The timestamp monitor class manages global timestamp state for all components in the test
 * harness. It also manages the global timestamps within WiredTiger.
 */
class timestamp_manager : public component {
    public:
    timestamp_manager(int64_t timestamp_window_seconds)
        /* _periodic_update_s is hardcoded to 1 second for now. */
        : _periodic_update_s(1), _oldest_ts(0U), _previous_ts(0U), _stable_ts(0U),
          _timestamp_window_seconds(timestamp_window_seconds), _ts(0U)

    {
    }

    void
    run()
    {
        while (_running) {
            /* Stable ts is increased every period. */
            std::this_thread::sleep_for(std::chrono::seconds(_periodic_update_s));
            ++_stable_ts;
            testutil_assert(_stable_ts > _oldest_ts);
            /*
             * Keep a time window between the stable and oldest ts less than the max defined in the
             * configuration.
             */
            if ((_stable_ts - _oldest_ts) > _timestamp_window_seconds)
                ++_oldest_ts;
        }
    }

    /* Get current stable timestamp. */
    wt_timestamp_t
    get_stable() const
    {
        return (_stable_ts);
    }

    /* Get current oldest timestamp. */
    wt_timestamp_t
    get_oldest() const
    {
        return (_oldest_ts);
    }

    /* Get a valid commit timestamp based on current time. */
    wt_timestamp_t
    get_next_ts()
    {
        _ts.fetch_add(1);
        return (_ts);
    }

    /* Wrapper of the set_timestamp method implemented in the connection_manager class. */
    void
    set_timestamp(const std::string &config)
    {
        connection_manager::instance().set_timestamp(config);
    }

    void
    set_oldest_ts(wt_timestamp_t ts)
    {
        _oldest_ts = ts;
    }

    void
    set_stable_ts(wt_timestamp_t ts)
    {
        _stable_ts = ts;
    }

    private:
    const wt_timestamp_t _periodic_update_s;
    wt_timestamp_t _oldest_ts, _previous_ts, _stable_ts;
    const int64_t _timestamp_window_seconds;
    std::atomic<wt_timestamp_t> _ts;
};
} // namespace test_harness

#endif
