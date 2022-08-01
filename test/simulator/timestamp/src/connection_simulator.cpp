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
#include <cassert>
#include <iostream>
#include <map>
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

void
connection_simulator::close_session(session_simulator *session)
{
    auto position = std::find(_session_list.begin(), _session_list.end(), session);

    /* The session to be closed should be present in the session list. */
    assert(position != _session_list.end());

    _session_list.erase(position);
    delete session;
    session = NULL;
}

bool
connection_simulator::query_timestamp(const std::string &config, std::string &hex_ts)
{
    std::string query_timestamp;
    /* For an empty config default to all_durable. */
    if (config.empty())
        query_timestamp = "all_durable";
    else {
        timestamp_manager *ts_manager = &timestamp_manager::get_timestamp_manager();
        std::map<std::string, std::string> config_map;

        /* Throw an error if the config cannot be parsed. */
        if (!ts_manager->parse_config(config, config_map))
            throw std::runtime_error("Incorrect config passed to set timestamp: " + config);

        auto pos = config_map.find("get");
        if (pos != config_map.end()) {
            query_timestamp = pos->second;
            config_map.erase(pos);
        } else
            throw std::runtime_error("Incorrect config (" + config + ") passed in query timestamp");

        if (!config_map.empty())
            throw std::runtime_error("Incorrect config (" + config + ") passed in query timestamp");
    }

    /*
     * For now, the simulator only supports all_durable, oldest_timestamp, and stable_timestamp.
     * Hence, we ignore last_checkpoint, oldest_reader, pinned and recovery.
     */
    uint64_t ts;
    if (query_timestamp == "all_durable")
        ts = _durable_ts;
    else if (query_timestamp == "oldest_timestamp" || query_timestamp == "oldest")
        ts = _oldest_ts;
    else if (query_timestamp == "stable_timestamp" || query_timestamp == "stable")
        ts = _stable_ts;
    else if (query_timestamp == "last_checkpoint")
        return (false);
    else if (query_timestamp == "oldest_reader")
        return (false);
    else if (query_timestamp == "pinned")
        return (false);
    else if (query_timestamp == "recovery")
        return (false);
    else
        throw std::runtime_error("Incorrect config (" + config + ") passed in query timestamp");

    /* Convert the timestamp from decimal to hex-decimal. */
    hex_ts = decimal_to_hex(ts);

    return (true);
}

inline uint64_t
connection_simulator::hex_to_decimal(const std::string &hex_ts)
{
    std::stringstream stream;
    uint64_t ts;

    stream << hex_ts;
    stream >> std::hex >> ts;

    return (ts);
}

inline std::string
connection_simulator::decimal_to_hex(const u_int64_t ts)
{
    std::stringstream stream;
    std::string hex_ts;

    stream << std::hex << ts;
    hex_ts = stream.str();

    return (hex_ts);
}

/* Get the timestamps and decode config map. */
bool
connection_simulator::decode_timestamp_config_map(std::map<std::string, std::string> &config_map,
  uint64_t &new_oldest_ts, uint64_t &new_stable_ts, uint64_t &new_durable_ts, bool &has_oldest,
  bool &has_stable, bool &has_durable)
{
    auto pos = config_map.find("oldest_timestamp");
    if (pos != config_map.end()) {
        new_oldest_ts = hex_to_decimal(pos->second);
        has_oldest = true;
        config_map.erase(pos);
    }

    pos = config_map.find("stable_timestamp");
    if (pos != config_map.end()) {
        new_stable_ts = hex_to_decimal(pos->second);
        has_stable = true;
        config_map.erase(pos);
    }

    pos = config_map.find("durable_timestamp");
    if (pos != config_map.end()) {
        new_durable_ts = hex_to_decimal(pos->second);
        has_durable = true;
        config_map.erase(pos);
    }

    return (config_map.empty());
}

bool
connection_simulator::set_timestamp(const std::string &config)
{
    /* If no timestamp was supplied, there's nothing to do. */
    if (config.empty())
        return (true);

    timestamp_manager *ts_manager = &timestamp_manager::get_timestamp_manager();
    std::map<std::string, std::string> config_map;

    /* Throw an error if the config cannot be parsed. */
    if (!ts_manager->parse_config(config, config_map))
        throw std::runtime_error("Incorrect config passed to set timestamp: " + config);

    uint64_t new_stable_ts = 0, new_oldest_ts = 0, new_durable_ts = 0;
    bool has_stable = false, has_oldest = false, has_durable = false;

    if (!decode_timestamp_config_map(config_map, new_oldest_ts, new_stable_ts, new_durable_ts,
          has_oldest, has_stable, has_durable))
        throw std::runtime_error("Incorrect config passed to set timestamp: " + config);

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
