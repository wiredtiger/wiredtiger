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

#include "timestamp_manager.h"

#include <iostream>
#include <sstream>
#include <string>

#include "connection_simulator.h"

timestamp_manager::timestamp_manager() {}

/* Get an instance of timestamp_manager class. */
timestamp_manager &
timestamp_manager::get_timestamp_manager()
{
    static timestamp_manager _timestamp_manager_instance;
    return (_timestamp_manager_instance);
}

/* Parse config string to a config map. */
bool
timestamp_manager::parse_config(
  const std::string &config, std::map<std::string, std::string> &config_map)
{
    std::istringstream conf(config);
    std::string token;

    while (std::getline(conf, token, ',')) {
        int pos = token.find('=');
        if (pos == -1)
            return (false);
        config_map.insert({token.substr(0, pos), token.substr(pos + 1)});
    }
    return (!config_map.empty());
}

/*
 * Validate both oldest and stable timestamps.
 * 1) Validation fails if Illegal timestamp value is passed (if less than or equal to 0).
 * 2) It is a no-op to set the oldest or stable timestamps behind the global
 *    values. Hence, ignore and continue validating.
 * 3) Validation fails if oldest is greater than the stable timestamp.
 */
bool
timestamp_manager::validate_oldest_and_stable_ts(
  uint64_t &new_stable_ts, uint64_t &new_oldest_ts, bool &has_oldest, bool &has_stable)
{
    /* No need to validate timestamps if timestamps are not passed in the config. */
    if (!has_oldest && !has_stable)
        return (true);

    connection_simulator *conn = &connection_simulator::get_connection();

    /* If config has oldest timestamp */
    if (has_oldest) {
        /* Cannot proceed any further, if oldest timestamp value <= 0 validation fails! */
        if ((int64_t)new_oldest_ts <= 0)
            return (false);
        /* It is a no-op to set the new oldest timestamps behind the current oldest timestamp. */
        if (new_oldest_ts <= conn->get_oldest_ts())
            has_oldest = false;
    }

    /* If config has stable timestamp */
    if (has_stable) {
        /* Cannot proceed any further, if stable timestamp value <= 0 validation fails! */
        if ((int64_t)new_stable_ts <= 0)
            return (false);
        /* It is a no-op to set the new stable timestamps behind the current stable timestamp. */
        if (new_stable_ts <= conn->get_stable_ts())
            has_stable = false;
    }

    /* No need to validate timestamps if stable or/and oldest were behind the global values. */
    if (!has_oldest && !has_stable)
        return (true);

    /* No need to validate timestamps if there is no new and no current oldest timestamp. */
    if (!has_oldest && conn->get_oldest_ts() == 0)
        return (true);

    /* No need to validate timestamps if there is no new and no current stable timestamp. */
    if (!has_stable && conn->get_stable_ts() == 0)
        return (true);

    /*
     * If the oldest timestamp was not passed in the config or was behind the current oldest
     * timestamp, modify the new_oldest_ts to the current oldest timestamp.
     */
    if ((!has_oldest && conn->get_oldest_ts() != 0))
        new_oldest_ts = conn->get_oldest_ts();

    /*
     * If the stable timestamp was not passed in the config or was behind the current stable
     * timestamp, modify the new_stable_ts to the current stable timestamp
     */
    if ((!has_stable && conn->get_stable_ts() != 0))
        new_stable_ts = conn->get_stable_ts();

    /* Validation fails if oldest is greater than the stable timestamp. */
    if (new_oldest_ts > new_stable_ts)
        return (false);

    return (true);
}

/*
 * Validate durable timestamp.
 * 1) Validation fails if Illegal timestamp value is passed (if less than or equal to 0).
 */
bool
timestamp_manager::validate_durable_ts(
  const uint64_t &new_durable_ts, const bool &has_durable) const
{
    /* If durable timestamp was not passed in the config, no validation is needed. */
    if (!has_durable)
        return (true);

    /* Illegal timestamp value (if less than or equal to 0). Validation fails!  */
    if ((int64_t)new_durable_ts <= 0)
        return (false);

    return (true);
}
