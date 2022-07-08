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

#ifndef CONNECTION_SIMULATOR_H
#define CONNECTION_SIMULATOR_H

#include <vector>
#include <memory>
#include <map>

#include "session_simulator.h"
#include "timestamp_simulator.h"
#include "timestamp_manager.h"

class timestamp_manager;

/* connection_simulator is a singleton class (Global access of one and only one instance). */
class connection_simulator {
    /* Member variables */
    private:
    std::vector<std::shared_ptr<session_simulator>> session_list;
    enum system_timestamps { oldest_timestamp, stable_timestamp, durable_timestamp };
    std::map<std::string, system_timestamps> system_timestamps_map;
    // PM-2564-TODO: Approach 1 - Connection is responsible for the system level timestamps.
    int oldest_ts;
    int stable_ts;
    int durable_ts;

    // PM-2564-TODO: Approach 2 - timestamp manager is responsible for the system level timestamps.
    timestamp_manager *ts_mgr;

    /* Methods */
    public:
    static connection_simulator &get_connection();
    std::shared_ptr<session_simulator> open_session();
    int query_timestamp();
    int set_timestamp(std::string config);

    int get_oldest_ts() { return oldest_ts; }
    int get_stable_ts() { return stable_ts; }
    ~connection_simulator() = default;

    /* No copies of the singleton allowed. */
    private:
    connection_simulator();
    int parse_timestamp_config(std::string config, std::string *ts_type, int *ts);
    bool validate_oldest_ts(int new_oldest_ts);
    bool validate_stable_ts(int new_stable_ts);
    bool validate_durable_ts(int new_durable_ts);
    void system_timestamps_map_setup();

    public:
    /* Deleted functions should generally be public as it results in better error messages. */
    connection_simulator(connection_simulator const &) = delete;
    connection_simulator &operator=(connection_simulator const &) = delete;
};

#endif
