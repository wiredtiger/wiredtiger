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

#include <fstream>
#include <iostream>

#include "call_log_manager.h"

call_log_manager::call_log_manager(std::string call_log_file)
{
    std::ifstream file(call_log_file);
    call_log = json::parse(file);
    api_map_setup();
}

void
call_log_manager::api_map_setup()
{
    api_map["wiredtiger_open"] = wiredtiger_open;
    api_map["open_session"] = open_session;
    api_map["set_timestamp"] = set_timestamp;
}

int
call_log_manager::process_call_log()
{
    for (const auto &call_log_entry : call_log)
        process_call_log_entry(call_log_entry);

    return (0);
}

int
call_log_manager::process_call_log_entry(json call_log_entry)
{
    try {
        if (call_log_entry.empty())
            return (0);

        std::shared_ptr<session_simulator> session = nullptr;

        switch (api_map[call_log_entry["method_name"]]) {
        case wiredtiger_open:
            std::cout << "WiredTiger open call" << std::endl;
            conn = &connection_simulator::get_connection();
            break;
        case open_session:
            std::cout << "Open session call" << std::endl;
            session = conn->open_session();
            /* Insert this session into the mapping between the simulator session object and the
             * wiredtiger session object. */
            session_map.insert(std::pair<std::string, std::shared_ptr<session_simulator>>(
              call_log_entry["session_id"], session));
            break;
        case set_timestamp:
            std::cout << "Set timestamp call" << std::endl;
            /* Convert the config char * to a string object. */
            std::string config(call_log_entry["input"]["config"]);
            conn->set_timestamp(config);
            break;
        }
    } catch (const std::exception &e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }

    return (0);
}
