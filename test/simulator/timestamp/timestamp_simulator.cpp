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
#include "connection_sim.h"
#include "json.hpp"

using json = nlohmann::json;

std::map<int, Connection> connection_map;

void print_connection_map();

void print_connection_map() {
    for (std::map<int, Connection>::iterator it = connection_map.begin(); it != connection_map.end(); ++it) {
        std::cout << it->first << " => " << &it->second << std::endl;
    }
}

int
main()
{
    json j = {{"pi", 3.141}, {"happy", true}};
    std::cout << j.dump(4) << std::endl;

    // Loop over the call log entries from the call log manager.
    // If the call log entry is wiredtiger_open -> Create a new connection object.    
    Connection *conn = new Connection();

    // PM-2564-TODO: Add this object to the object map manager here.
    int  object_id = 1;
    connection_map.insert({object_id, *conn});

    print_connection_map();

    conn->open_session();

    delete(conn);

    return (0);
}
