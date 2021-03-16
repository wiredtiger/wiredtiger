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
          component(config), _is_enabled(false), _latest_ts(0U), _oldest_lag(0), _oldest_ts(0U),
          _periodic_update_s(1), _stable_lag(0), _stable_ts(0U)
    {
    }

    void
    load()
    {
        testutil_assert(_config != nullptr);
        testutil_check(_config->get_int(OLDEST_LAG, _oldest_lag));
        testutil_check(_config->get_int(STABLE_LAG, _stable_lag));
        testutil_assert(_oldest_lag >= 0);
        testutil_assert(_stable_lag >= 0);
        testutil_check(_config->get_bool(ENABLE_TIMESTAMP, _is_enabled));
        component::load();
    }

    void
    run()
    {
        std::string config;

        while (_is_enabled && _running) {
            /* Timestamps are checked periodically. */
            std::this_thread::sleep_for(std::chrono::seconds(_periodic_update_s));

            /*
             * Keep a time window between the latest and stable ts less than the max defined in the
             * configuration.
             */
            testutil_assert(_latest_ts >= _stable_ts);
            if ((_latest_ts - _stable_ts) > _stable_lag) {
                _stable_ts = _latest_ts - _stable_lag;
                config += std::string(STABLE_TS) + "=" + decimal_to_hex(_stable_ts);
            }

            /*
             * Keep a time window between the stable and oldest ts less than the max defined in the
             * configuration.
             */
            testutil_assert(_stable_ts > _oldest_ts);
            if ((_stable_ts - _oldest_ts) > _oldest_lag) {
                _oldest_ts = _stable_ts - _oldest_lag;
                if (!config.empty())
                    config += ",";
                config += std::string(OLDEST_TS) + "=" + decimal_to_hex(_oldest_ts);
            }

            /* Save the new timestamps. */
            if (!config.empty()) {
                connection_manager::instance().set_timestamp(config);
                config = "";
            }
        }
    }

    /* Get a valid commit timestamp. */
    wt_timestamp_t
    get_next_ts()
    {
        _latest_ts.fetch_add(1);
        return (_latest_ts);
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
    std::atomic<wt_timestamp_t> _latest_ts;
    wt_timestamp_t _oldest_ts, _stable_ts;
    /*
     * _oldest_lag is the time window between the stable and oldest timestamps.
     * stable_lag is the time window between the latest and stable timestamps.
     */
    int64_t _oldest_lag, _stable_lag;
};
} // namespace test_harness

#endif
