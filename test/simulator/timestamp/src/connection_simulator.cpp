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

bool connection_simulator::validate_oldest_ts(int new_oldest_ts){
    // Oldest timestamp can't move backward.
    if (new_oldest_ts <= oldest_ts){
        std::cout << "Oldest timestamp cannot move backwards." << std::endl;
        return false;
    }

    /*
     * The oldest and stable timestamps must always satisfy the condition that oldest <= stable.
     */
    if (new_oldest_ts > stable_ts){
        std::cout << "set_timestamp: oldest timestamp " << new_oldest_ts << "must not be later than stable timestamp " << stable_ts << "." << std::endl;
        return false;
    }

    return true;

}

bool connection_simulator::validate_stable_ts(int new_stable_ts){
    // Oldest timestamp can't move backward.
    if (new_stable_ts <= stable_ts){
        std::cout << "Stable timestamp cannot move backwards." << std::endl;
        return false;
    }

    /*
     * The oldest and stable timestamps must always satisfy the condition that oldest <= stable.
     */
    if (oldest_ts > new_stable_ts){
        std::cout << "set_timestamp: oldest timestamp " << new_stable_ts << "must not be later than stable timestamp " << stable_ts << "." << std::endl;
        return false;
    }

    return true;
}

bool connection_simulator::validate_durable_ts(int new_durable_ts){
    // No constraints on setting the durable ts.
    return true;
}

int connection_simulator::parse_timestamp_config(std::string config, std::string *ts_type, int *ts){

    
    *ts_type = config.substr(0, config.find("="));
    // Copy the substring after the '=' to get the timestamp value from the config string.
    std::string ts_string = config.substr(config.find("=") + 1);
    std::cout << "setting oldest timestamp to: " << ts_string << std::endl;
    // Convert the ts to an int.
    *ts = std::stoi(ts_string);

    return 0;
}

int
connection_simulator::set_timestamp(std::string config)
{

    std::cout << "config: " << config << std::endl;
    std::string ts_type;
    int ts;
    parse_timestamp_config(config, &ts_type, &ts);

    std::cout << "ts_type: " << ts_type << std::endl;
    std::cout << "ts: " << ts << std::endl;

    switch (system_timestamps_map[ts_type]) {
        case oldest_timestamp:
            if(!ts_mgr->validate_oldest_ts(ts)) 
                oldest_ts = ts;
            break;
        case stable_timestamp:
            if(!ts_mgr->validate_stable_ts(ts))
                stable_ts = ts;
            break;
        case durable_timestamp:
            if(!ts_mgr->validate_durable_ts(ts))
                durable_ts = ts;
            break;
    }

    // if (config.find("oldest_timestamp") != std::string::npos){
    //     // Copy the substring after the '=' to get the timestamp value from the config string.
    //     std::string ts_string = config.substr(config.find("=") + 1);
    //     std::cout << "setting oldest timestamp to: " << ts_string << std::endl;
    //     // Convert the ts to an int.
    //     int ts = std::stoi(ts_string);

    //     // Validate the timestamp
    //     if(!validate_oldest_ts(ts))
    //         return 1;

    //     // If the validation was successful update the oldest timestamp.
    //     oldest_ts = ts;
    // } else if (config.find("stable_ts") != std::string::npos){
    //     // Copy the substring after the '=' to get the timestamp value from the config string.
    //     std::string ts_string = config.substr(config.find("=") + 1);
    //     std::cout << "setting oldest timestamp to: " << ts_string << std::endl;
    //     // Convert the ts to an int.
    //     int ts = std::stoi(ts_string);

    //     // Validate the timestamp
    //     if(!validate_stable_ts(ts))
    //         return 1;

    //     // If the validation was successful update the oldest timestamp.
    //     stable_ts = ts;
    // } else if (config.find("durable_ts") != std::string::npos){
    //     // Copy the substring after the '=' to get the timestamp value from the config string.
    //     std::string ts_string = config.substr(config.find("=") + 1);
    //     std::cout << "setting oldest timestamp to: " << ts_string << std::endl;
    //     // Convert the ts to an int.
    //     int ts = std::stoi(ts_string);

    //     // Validate the timestamp
    //     if(!validate_durable_ts(ts))
    //         return 1;

    //     // If the validation was successful update the oldest timestamp.
    //     durable_ts = ts;
    // }

    // // PM-2564-TODO: Using timestamp manager idea.
    // if (config.find("oldest_timestamp") != std::string::npos){
    //     /* Example use of getting timestamp specs. */
    //     // ts_mgr->oldest_ts.get_specs();

    //     // Copy the substring after the '=' to get the timestamp value from the config string.
    //     std::string ts_string = config.substr(config.find("=") + 1);
    //     std::cout << "setting oldest timestamp to: " << ts_string << std::endl;
    //     // Convert the ts to an int.
    //     int ts = std::stoi(ts_string);
    //     // Validate the timestamp using the timestamp manager.
    //     ts_mgr->validate_oldest_ts(ts);
    //     // If the validation is successful then update the connection oldest timestamp member.
    //     oldest_ts = ts;
    //     // Then update the timestamp managers pointer to the oldest timestamp so that it is aware of it.
    //     ts_mgr->set_oldest_ts(&oldest_ts);
    // }


    // Check to see if the oldest timestamp has been set correctly.

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

