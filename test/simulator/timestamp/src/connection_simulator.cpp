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

session_simulator *
connection_simulator::open_session()
{
    session_simulator *session = new session_simulator();

    _session_list.push_back(session);

    return (session);
}

int
connection_simulator::query_timestamp() const
{
    return (0);
}

/*
 * Parse a single timestamp configuration string eg. oldest_timestamp=10. The string is split on the
 * '=' and the new timestamp is assigned.
 */
int
connection_simulator::parse_timestamp_config_single(
  std::string config, uint64_t *new_oldest_ts, uint64_t *new_stable_ts)
{

    std::string ts_type;
    uint64_t ts;

    /* The substring before the '=' indicates the type of timestamp to be set. */
    ts_type = config.substr(0, config.find("="));
    /* Copy the substring after the '=' to get the timestamp value from the config string. */
    std::string ts_string = config.substr(config.find("=") + 1);
    /* Convert the timestamp to an int. */
    ts = std::stoi(ts_string);

    switch (system_timestamps_map[ts_type]) {
    case oldest_timestamp:
        *new_oldest_ts = ts;
        break;
    case stable_timestamp:
        *new_stable_ts = ts;
        break;
    case durable_timestamp:
        durable_ts = ts;
        break;
    }

    return 0;
}

/*
 *  Parse the timestamp configuration string with timestamps separated by ','.
 */
int
connection_simulator::parse_timestamp_config(
  std::string config, uint64_t *new_oldest_ts, uint64_t *new_stable_ts)
{
    size_t pos = 0;
    std::string token;

    /* Loop over the timestamp configuration strings separated by ','. */
    while ((pos = config.find(",")) != std::string::npos) {
        token = config.substr(0, pos);
        std::cout << token << std::endl;

        parse_timestamp_config_single(token, new_oldest_ts, new_stable_ts);

        config.erase(0, pos + 1);
    }

    /* Parse the final timestamp configuration string. */
    parse_timestamp_config_single(config, new_oldest_ts, new_stable_ts);

    return 0;
}

int
connection_simulator::set_timestamp(std::string config)
{
    /* Set the new stable and oldest timestamps to the previous global values by default. */
    uint64_t new_stable_ts = stable_ts;
    uint64_t new_oldest_ts = oldest_ts;

    parse_timestamp_config(config, &new_oldest_ts, &new_stable_ts);

    /* Validate the new oldest timestamp if it is being updated. */
    if (new_oldest_ts != oldest_ts &&
      ts_mgr->validate_oldest_ts(new_stable_ts, new_oldest_ts) != 0) {
        return 1;
    }

    /* Validate the new stable timestamp if it is being updated. */
    if (new_stable_ts != stable_ts &&
      ts_mgr->validate_stable_ts(new_stable_ts, new_oldest_ts) != 0) {
        return 1;
    }

    /*
     * The new timestamps have been validated up to this point. Now we can update the connection
     * timestamps.
     */
    stable_ts = new_stable_ts;
    oldest_ts = new_oldest_ts;

    return (0);
}

void
connection_simulator::system_timestamps_map_setup()
{
    system_timestamps_map["oldest_timestamp"] = oldest_timestamp;
    system_timestamps_map["stable_timestamp"] = stable_timestamp;
    system_timestamps_map["durable_timestamp"] = durable_timestamp;

    return;
}

connection_simulator::connection_simulator()
{
    system_timestamps_map_setup();
}

connection_simulator::~connection_simulator()
{
    for (auto session : _session_list)
        delete session;
}
