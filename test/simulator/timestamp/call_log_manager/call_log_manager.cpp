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
#include <memory>

#include "call_log_manager.h"

call_log_manager::call_log_manager(const std::string &call_log_file)
{
    std::ifstream file(call_log_file);
    if (file.fail()) {
        std::cout << "File '" << call_log_file << "' either doesn't exist or is not accessible."
                  << std::endl;
        exit(1);
    }

    _call_log = json::parse(file);
    api_map_setup();
}

void
call_log_manager::api_map_setup()
{
    _api_map["wiredtiger_open"] = api_method::wiredtiger_open;
    _api_map["open_session"] = api_method::open_session;
}

void
call_log_manager::process_call_log()
{
    for (const auto &call_log_entry : _call_log)
        process_call_log_entry(call_log_entry);
}

void
call_log_manager::process_call_log_entry(json call_log_entry)
{
    try {
        const std::string method_name = call_log_entry["MethodName"].get<std::string>();
        std::cout << "Processing entry " << method_name << std::endl;
        switch (_api_map.at(method_name)) {
        case api_method::wiredtiger_open:
            _conn = &connection_simulator::get_connection();
            break;
        case api_method::open_session:
            session_simulator *session = _conn->open_session();
            /* Insert this session into the mapping between the simulator session object and the
             * wiredtiger session object. */
            _session_map.insert(std::pair<std::string, session_simulator *>(
              call_log_entry["Output"]["objectId"], session));
            break;
        }
    } catch (const std::exception &e) {
        std::cerr << "process_call_log_entry: Cannot process call log entry. " << e.what()
                  << std::endl;
        return;
    }
}

int
main(int argc, char *argv[])
{
    /* Exit if call log file was not passed. */
    if (argc != 2) {
        std::cout << "call_log_interface: missing call log file path" << std::endl;
        exit(1);
    }

    const std::string call_log_file = argv[1];

    auto cl_manager = std::make_unique<call_log_manager>(call_log_file);
    cl_manager->process_call_log();

    return (0);
}
