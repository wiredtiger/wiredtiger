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

#include "call_log_manager.h"

#include <fstream>
#include <iostream>
#include <memory>

call_log_manager::call_log_manager(const std::string &call_log_file) : _conn(nullptr)
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
    _api_map["close_session"] = api_method::close_session;
    _api_map["open_session"] = api_method::open_session;
    _api_map["set_timestamp"] = api_method::set_timestamp;
    _api_map["wiredtiger_open"] = api_method::wiredtiger_open;
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
        const std::string method_name = call_log_entry["method_name"].get<std::string>();
        switch (_api_map.at(method_name)) {
        case api_method::wiredtiger_open: {
            _conn = &connection_simulator::get_connection();
            break;
        }
        case api_method::open_session: {
            const std::string session_id = call_log_entry["session_id"].get<std::string>();
            /*
             * Not having a valid connection is a fatal error since no other operations can happen
             * without a connection.
             */
            if (_conn == nullptr)
                throw std::runtime_error("Could not open session (session ID: " + session_id + ")" +
                  ", connection does not exist");

            /* We should not open sessions with an ID that is already in use. */
            if (_session_map.find(session_id) != _session_map.end()) {
                std::cerr
                  << "Could not open duplicate session, session already exists (session ID: "
                  << session_id << ")" << std::endl;
                break;
            }

            session_simulator *session = _conn->open_session();
            /*
             * Insert this session into the mapping between the simulator session object and the
             * wiredtiger session object.
             */
            _session_map.insert(std::pair<std::string, session_simulator *>(session_id, session));
            break;
        }
        case api_method::close_session: {
            const std::string session_id = call_log_entry["session_id"].get<std::string>();
            /*
             * Not having a valid connection is a fatal error since no other operations can happen
             * without a connection.
             */
            if (_conn == nullptr)
                throw std::runtime_error("Could not close the session (session ID: " + session_id +
                  ")" + ", connection does not exist");

            /* We should not close sessions with an ID that does not exist in the session map. */
            if (_session_map.find(session_id) == _session_map.end()) {
                std::cerr << "Could not close session, session does not exist (session ID: "
                          << session_id << ")" << std::endl;
                break;
            }

            session_simulator *session = _session_map.at(session_id);

            /* Remove the session from the connection and the session map. */
            if (_conn->close_session(session))
                _session_map.erase(session_id);
            else
                std::cerr << "Could not close the session (session ID: " << session_id << ")"
                          << std::endl;
            break;
        }
        case api_method::set_timestamp: {
            /*
             * Not having a valid connection is a fatal error since no other operations can happen
             * without a connection.
             */
            if (_conn == nullptr)
                throw std::runtime_error(
                  "Could not set the timestamp as connection does not exist");

            /* Convert the config char * to a string object. */
            const std::string config = call_log_entry["input"]["config"].get<std::string>();

            /*
             * A generated call log without a configuration string in the set timestamp entry will
             * have the string "(null)". We can ignore the set timestamp call if there is no
             * configuration.
             */
            if (config != "(null)" && !_conn->set_timestamp(config)) {
                throw std::runtime_error("Failure to set timestamp. Timestamps may not be valid!");
            }
            break;
        }
        case api_method::begin_transaction: {
            const std::string session_id = call_log_entry["session_id"].get<std::string>();
            /*
             * Not having a valid connection is a fatal error since no other operations can happen
             * without a connection.
             */
            if (_conn == nullptr)
                throw std::runtime_error("Could not close the session (session ID: " + session_id +
                  ")" + ", connection does not exist");

            /* We should not begin transactions on sessions with an ID that does not exist in the
             * session map. */
            if (_session_map.find(session_id) == _session_map.end()) {
                std::cerr << "Could not begin transaction, session does not exist (session ID: "
                          << session_id << ")" << std::endl;
                break;
            }

            /* Get the session from the session map. */
            session_simulator *session = _session_map.at(session_id);

            /*
             * Check no other transactions are running. There should be a 1:1 relationship between
             * session and transaction.
             */
            if (session->is_txn_running())
                break;

            /* Get the session from the session map and set the txn_running to be true. */
            session->set_txn_running(true);
        }
        }
    } catch (const std::exception &e) {
        std::cerr << "exception: " << e.what() << std::endl;
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
