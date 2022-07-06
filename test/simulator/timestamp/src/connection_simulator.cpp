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

int
connection_simulator::set_timestamp(std::string config)
{

    std::cout << "config: " << config << std::endl;

    // PM-2564-TODO: Question
    // What is the purpose of the timestamp manager why would we do this?
    // ts_mgr.set_timestamp(config);

    // PM-2564-TODO: First approach
    // Instead of just handling it here?
    // Check if the config string contains an oldest timestamp.
    // if (config.find("oldest_timestamp") != std::string::npos){
    //     oldest_ts.get_specs();
    //     std::cout << "Setting oldest timestamp to ";
    //     // Copy the substring after the '=' to get the timestamp value from the config string.
    //     std::string ts = config.substr(config.find("=") + 1);
    //     std::cout << "setting timestamp to: " << ts << std::endl;
    //     oldest_ts.set_ts(std::stoi(ts));
    // }

    // PM-2564-TODO: Proposed idea.
    if (config.find("oldest_timestamp") != std::string::npos){
        /* Example use of getting timestamp specs. */
        ts_mgr.oldest_ts.get_specs();
        // Copy the substring after the '=' to get the timestamp value from the config string.
        std::string ts = config.substr(config.find("=") + 1);
        std::cout << "setting oldest timestamp to: " << ts << std::endl;
        ts_mgr.set_oldest_ts(std::stoi(ts));
    }


    // Check to see if the oldest timestamp has been set correctly.

    return (0);
}

connection_simulator::connection_simulator() {}
