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

#include "connection_simulator.h"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>

#include "timestamp_manager.h"

/* Get an instance of connection_simulator class. */
connection_simulator &
connection_simulator::get_connection()
{
    static connection_simulator _connection_instance;
    return (_connection_instance);
}

uint64_t
connection_simulator::get_oldest_ts() const
{
    return (_oldest_ts);
}

uint64_t
connection_simulator::get_stable_ts() const
{
    return (_stable_ts);
}

session_simulator *
connection_simulator::open_session()
{
    session_simulator *session = new session_simulator();

    _session_list.push_back(session);

    return (session);
}

bool
connection_simulator::close_session(session_simulator *session)
{
    std::vector<session_simulator *>::iterator position =
      std::find(_session_list.begin(), _session_list.end(), session);

    /* If session doesn't exist in the session_list return false. */
    if (position == _session_list.end()) {
        std::cerr << "Error: session is not present in the session list" << std::endl;
        return (false);
    }

    _session_list.erase(position);
    delete session;
    return (true);
}

bool
connection_simulator::query_timestamp(const std::string &config, std::string &hex_ts)
{
    uint64_t ts;

    /* parse the timestamp config and get what we need, for now oldest, stable and durable. error if
     * incorrect config */
    /* if no config is passed then return all durable */
    /* in case of a unsup ts return false*/

    /* conveert to hex and set the hex_ts */
}

/*
 * Parse a single timestamp configuration string eg. oldest_timestamp=10, and then decode it from
 * hex to decimal.
 */
bool
connection_simulator::parse_and_decode_timestamp_config_single(const std::string &config,
  uint64_t &new_oldest_ts, uint64_t &new_stable_ts, uint64_t &new_durable_ts, bool &has_oldest,
  bool &has_stable, bool &has_durable)
{
    const std::string oldest_ts_str = "oldest_timestamp=";
    const std::string stable_ts_str = "stable_timestamp=";
    const std::string durable_ts_str = "durable_timestamp=";

    int durable_found = config.find(durable_ts_str);
    int oldest_found = config.find(oldest_ts_str);
    int stable_found = config.find(stable_ts_str);

    std::stringstream stream;
    std::string hex_ts;
    if (oldest_found != -1) {
        hex_ts = config.substr(oldest_found + oldest_ts_str.size());
        stream << hex_ts;
        stream >> std::hex >> new_oldest_ts;
        has_oldest = true;
        return (true);
    }
    if (stable_found != -1) {
        hex_ts = config.substr(stable_found + stable_ts_str.size());
        stream << hex_ts;
        stream >> std::hex >> new_stable_ts;
        has_stable = true;
        return (true);
    }
    if (durable_found != -1) {
        hex_ts = config.substr(durable_found + durable_ts_str.size());
        stream << hex_ts;
        stream >> std::hex >> new_durable_ts;
        has_durable = true;
        return (true);
    }

    return (false);
}

/*
 * Parse the timestamp configuration string with timestamps separated by ',', and then decode it
 * from hex to decimal.
 */
void
connection_simulator::parse_and_decode_timestamp_config(const std::string &config,
  uint64_t &new_oldest_ts, uint64_t &new_stable_ts, uint64_t &new_durable_ts, bool &has_oldest,
  bool &has_stable, bool &has_durable)
{
    std::string conf = config;
    /* Loop over the timestamp configuration strings separated by ','. */
    size_t pos;
    while ((pos = conf.find(",")) != std::string::npos) {
        std::string token = conf.substr(0, pos);

        /* Throw an error for an unknown config. */
        if (!parse_and_decode_timestamp_config_single(token, new_oldest_ts, new_stable_ts,
              new_durable_ts, has_oldest, has_stable, has_durable))
            throw std::runtime_error(
              "Could not set the timestamp as there is no system level timestamp called '" + token +
              "'");

        conf.erase(0, pos + 1);
    }

    /* Parse the final timestamp configuration string, and throw an error for an unknown config. */
    if (!parse_and_decode_timestamp_config_single(
          conf, new_oldest_ts, new_stable_ts, new_durable_ts, has_oldest, has_stable, has_durable))
        throw std::runtime_error(
          "Could not set the timestamp as there is no system level timestamp called '" + conf +
          "'");
}

bool
connection_simulator::set_timestamp(const std::string &config)
{
    /* If no timestamp was supplied, there's nothing to do. */
    if (config.empty())
        return (true);

    timestamp_manager *ts_manager = &timestamp_manager::get_timestamp_manager();
    uint64_t new_stable_ts = 0, new_oldest_ts = 0, new_durable_ts = 0;
    bool has_stable = false, has_oldest = false, has_durable = false;

    parse_and_decode_timestamp_config(
      config, new_oldest_ts, new_stable_ts, new_durable_ts, has_oldest, has_stable, has_durable);

    /* Validate the new durable timestamp. */
    if (!ts_manager->validate_durable_ts(new_durable_ts, has_durable))
        return (false);

    /* Validate the new oldest and stable timestamp. */
    if (!ts_manager->validate_oldest_and_stable_ts(
          new_stable_ts, new_oldest_ts, has_oldest, has_stable))
        return (false);

    if (has_stable)
        _stable_ts = new_stable_ts;
    if (has_oldest)
        _oldest_ts = new_oldest_ts;
    if (has_durable)
        _durable_ts = new_durable_ts;

    return (true);
}

connection_simulator::connection_simulator() : _oldest_ts(0), _stable_ts(0), _durable_ts(0) {}

connection_simulator::~connection_simulator()
{
    for (auto session : _session_list)
        delete session;
}
