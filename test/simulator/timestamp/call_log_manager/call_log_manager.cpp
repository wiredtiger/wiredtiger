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

call_log_manager::call_log_manager(std::string call_log_file)
{
    std::ifstream file(call_log_file);
    if (file.fail()) {
        std::cout << "File '" << call_log_file << "' either doesn't exist or is not accessible."
                  << std::endl;
        exit(1);
    }

    call_log = json::parse(file);
    api_map_setup();
}

void
call_log_manager::api_map_setup()
{
    api_map["wiredtiger_open"] = wiredtiger_open;
    api_map["open_session"] = open_session;
}

void
call_log_manager::process_call_log()
{
    for (const auto &call_log_entry : call_log)
        process_call_log_entry(call_log_entry);
}

void
call_log_manager::process_call_log_entry(json call_log_entry)
{
    std::string method_name = call_log_entry["MethodName"].get<std::string>();
    switch (api_map.at(method_name)) {
    case wiredtiger_open:
        /* conn = &connection_simulator::get_connection(); */
        break;
    case open_session:
        break;
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

    std::string call_log_file = argv[1];

    auto cl_manager = std::make_unique<call_log_manager>(call_log_file);
    cl_manager->process_call_log();

    return (0);
}
