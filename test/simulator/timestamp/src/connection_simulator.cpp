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

#include <algorithm>
#include <iostream>
#include <string.h>

#include "connection_simulator.h"
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
    return _oldest_ts;
}

uint64_t
connection_simulator::get_stable_ts() const
{
    return _stable_ts;
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

int
connection_simulator::query_timestamp() const
{
    return (0);
}

/* Parse a single timestamp configuration string eg. oldest_timestamp=10. */
bool
connection_simulator::parse_timestamp_config_single(const std::string &config,
  uint64_t *new_oldest_ts, uint64_t *new_stable_ts, uint64_t *new_durable_ts)
{
    int durable_found = config.find("durable_timestamp=");
    int oldest_found = config.find("oldest_timestamp=");
    int stable_found = config.find("stable_timestamp=");

    std::string ts_string;
    if (oldest_found != -1) {
        ts_string = config.substr(oldest_found + 17);
        *new_oldest_ts = std::stoi(ts_string);
        return (true);
    }
    if (stable_found != -1) {
        ts_string = config.substr(stable_found + 17);
        *new_stable_ts = std::stoi(ts_string);
        return (true);
    }
    if (durable_found != -1) {
        ts_string = config.substr(durable_found + 18);
        *new_durable_ts = std::stoi(ts_string);
        return (true);
    }

    return (false);
}

/* Parse the timestamp configuration string with timestamps separated by ','. */
void
connection_simulator::parse_timestamp_config(const std::string &config, uint64_t *new_oldest_ts,
  uint64_t *new_stable_ts, uint64_t *new_durable_ts)
{
    std::string conf = config;
    /* Loop over the timestamp configuration strings separated by ','. */
    size_t pos;
    while ((pos = conf.find(",")) != std::string::npos) {
        std::string token = conf.substr(0, pos);

        if (!parse_timestamp_config_single(token, new_oldest_ts, new_stable_ts, new_durable_ts))
            throw std::runtime_error(
              "Could not set the timestamp as there is no system level timestamp called '" + token +
              "'");

        conf.erase(0, pos + 1);
    }

    /* Parse the final timestamp configuration string. */
    if (!parse_timestamp_config_single(conf, new_oldest_ts, new_stable_ts, new_durable_ts))
        throw std::runtime_error(
          "Could not set the timestamp as there is no system level timestamp called '" + conf +
          "'");
}

bool
connection_simulator::set_timestamp(const std::string &config)
{
    /* Set the new stable and oldest timestamps to the previous global values by default. */
    uint64_t new_stable_ts = _stable_ts;
    uint64_t new_oldest_ts = _oldest_ts;
    uint64_t new_durable_ts = _durable_ts;

    parse_timestamp_config(config, &new_oldest_ts, &new_stable_ts, &new_durable_ts);

    /* Validate the new oldest timestamp if it is being updated. */
    if (new_oldest_ts != _oldest_ts &&
      _ts_mgr->validate_oldest_ts(new_stable_ts, new_oldest_ts) != 0) {
        return (false);
    }

    /* Validate the new stable timestamp if it is being updated. */
    if (new_stable_ts != _stable_ts &&
      _ts_mgr->validate_stable_ts(new_stable_ts, new_oldest_ts) != 0) {
        return (false);
    }

    /*
     * The new timestamps have been validated up to this point. Now we can update the connection
     * timestamps.
     */
    _stable_ts = new_stable_ts;
    _oldest_ts = new_oldest_ts;
    _durable_ts = new_durable_ts;

    return (true);
}

connection_simulator::connection_simulator() {}

connection_simulator::~connection_simulator()
{
    for (auto session : _session_list)
        delete session;
}
