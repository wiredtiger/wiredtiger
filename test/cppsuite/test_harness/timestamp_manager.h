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

#include "component.h"
#include <atomic>
#include <chrono>
#include <sstream>
#include <thread>

namespace test_harness {
/*
 * The timestamp monitor class manages global timestamp state for all components in the test
 * harness. It also manages the global timestamps within WiredTiger.
 */
class timestamp_manager : public component {
    public:
    timestamp_manager(configuration *config)
        : /* _periodic_update_s is hardcoded to 1 second for now. */
          component(config), _is_enabled(false), _periodic_update_s(1), _oldest_ts(0U),
          _previous_ts(0U), _stable_ts(0U), _timestamp_window_seconds(0), _ts(0U)

    {
    }

    void
    load()
    {
        testutil_assert(_config != nullptr);
        testutil_check(_config->get_int(TIMESTAMP_WINDOW_SECONDS, _timestamp_window_seconds));
        testutil_assert(_timestamp_window_seconds >= 0);
        testutil_check(_config->get_bool(ENABLE_TIMESTAMP, _is_enabled));
        component::load();
    }
<<<<<<< HEAD
=======

>>>>>>> WT-7275 Timestamp manager is always part of the compoents and its parameters are checked within its load function. Removed unused functions and call to WT API to write timestamps.
    void
    run()
    {
        std::string config;

        while (_is_enabled && _running) {
            /* Stable ts is increased every period. */
            std::this_thread::sleep_for(std::chrono::seconds(_periodic_update_s));
            ++_stable_ts;
            config = std::string(STABLE_TIMESTAMP) + "=" + decimal_to_hex(_stable_ts);
            testutil_assert(_stable_ts > _oldest_ts);
            /*
             * Keep a time window between the stable and oldest ts less than the max defined in the
             * configuration.
             */
            if ((_stable_ts - _oldest_ts) > _timestamp_window_seconds) {
                ++_oldest_ts;
                config += "," + std::string(OLDEST_TIMESTAMP) + "=" + decimal_to_hex(_oldest_ts);
            }
            connection_manager::instance().set_timestamp(config);
        }
    }

    /* Get a valid commit timestamp. */
    wt_timestamp_t
    get_next_ts()
    {
        _ts.fetch_add(1);
        return (_ts);
    }

    private:
    const std::string
    decimal_to_hex(int64_t value) const
    {
        std::stringstream ss;
        ss << std::hex << value;
        std::string res(ss.str());
        return res;
    }

    private:
    bool _is_enabled;
    const wt_timestamp_t _periodic_update_s;
    wt_timestamp_t _oldest_ts, _previous_ts, _stable_ts;
    int64_t _timestamp_window_seconds;
    std::atomic<wt_timestamp_t> _ts;
};
} // namespace test_harness

#endif
