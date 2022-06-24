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
#include <map>
#include <fstream>
#include "connection_sim.h"
#include "json.hpp"

using json = nlohmann::json;

std::map<unsigned int, Connection> connection_map;

void print_connection_map();

void print_connection_map() {
    for (std::map<unsigned int, Connection>::iterator it = connection_map.begin(); it != connection_map.end(); ++it) {
        std::cout << it->first << " => " << &it->second << std::endl;
    }
}

// JSON object for wiredtiger_open

int
main()
{
    // Loop over the call log entries from the call log manager.
    // read a JSON file
    std::ifstream i("/home/ubuntu/wiredtiger/test/simulator/timestamp/wt_call_log.json");
    json j2;
    i >> j2;

    std::cout << j2.dump(4) << std::endl;

    std::cout << j2["Operation"]["ClassName"] << std::endl;
    std::cout << "API call: " << j2["Operation"]["MethodName"] << std::endl;

    // If the call log entry is wiredtiger_open -> Create a new connection object.    
    if (j2["Operation"]["MethodName"] == "wiredtiger_open"){
        Connection *conn = new Connection();

        // Get the connection objectid from the call log entry.
        std::string s = j2["Operation"]["Output"]["objectId"];
        unsigned int x = std::stoul(s, nullptr, 16);

        // Check to see if the object id is the same after changing the type.
        std::cout << std::hex << x << std::endl;

        // Add this object to the connection map.
        connection_map.insert({x, *conn});

        // Check the entries in the connection map.
        print_connection_map();

        // Show that open session creates a new session object.
        conn->open_session();

        delete(conn);
    }

    return (0);
}
