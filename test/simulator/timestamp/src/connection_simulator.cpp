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
#include "timestamp_manager.h"
#include <iostream>
#include <string.h>

/* Get an instance of connection_simulator class. */
connection_simulator &
connection_simulator::get_connection()
{
    static connection_simulator _connection_instance;
    return (_connection_instance);
}

std::shared_ptr<session_simulator>
connection_simulator::open_session()
{
    std::shared_ptr<session_simulator> session = std::make_shared<session_simulator>();

    session_list.push_back(session);

    return (session);
}

int
connection_simulator::query_timestamp()
{
    return (0);
}

int connection_simulator::parse_timestamp_config_single(std::string config, std::string *ts_type, int *ts){

    
    *ts_type = config.substr(0, config.find("="));
    // Copy the substring after the '=' to get the timestamp value from the config string.
    std::string ts_string = config.substr(config.find("=") + 1);
    std::cout << "setting " << *ts_type << " to: " << ts_string << std::endl;
    // Convert the ts to an int.
    *ts = std::stoi(ts_string);

    return 0;
}

int connection_simulator::parse_timestamp_config(std::string config, int *new_oldest_ts, int *new_stable_ts){
    std::string s = config;
    size_t pos = 0;
    std::string token;

    while ((pos = s.find(",")) != std::string::npos) {
        token = s.substr(0, pos);
        std::cout << token << std::endl;

        std::string ts_type;
        int ts;
        parse_timestamp_config_single(token, &ts_type, &ts);
        switch (system_timestamps_map[ts_type]) {
            case oldest_timestamp:
                *new_oldest_ts = ts;
                break;
            case stable_timestamp:
                *new_stable_ts =  ts;
                break;
            case durable_timestamp:
                break;
        }
        
        s.erase(0, pos + 1);
    }

    std::string ts_type;
    int ts;
    parse_timestamp_config_single(s, &ts_type, &ts);
    switch (system_timestamps_map[ts_type]) {
        case oldest_timestamp:
            *new_oldest_ts = ts;
            break;
        case stable_timestamp:
            *new_stable_ts =  ts;
            break;
        case durable_timestamp:
            break;
    }

    return 0;
}

int
connection_simulator::set_timestamp(std::string config)
{
    int new_stable_ts = stable_ts;
    int new_oldest_ts = oldest_ts;

    parse_timestamp_config(config, &new_oldest_ts, &new_stable_ts);

    if (new_oldest_ts != oldest_ts && ts_mgr->validate_oldest_ts(new_stable_ts, new_oldest_ts) != 0) {
        return 1;
    }

    if (new_stable_ts != stable_ts && ts_mgr->validate_stable_ts(new_stable_ts, new_oldest_ts) != 0) {
        return 1;
    }

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


connection_simulator::connection_simulator() {
    system_timestamps_map_setup();
}

