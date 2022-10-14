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

#include "session_simulator.h"

#include <cassert>
#include <iostream>
#include <map>

#include "connection_simulator.h"
#include "error_simulator.h"
#include "timestamp_manager.h"

session_simulator::session_simulator() : _txn_running(false) {}

int
session_simulator::begin_transaction(const std::string &config)
{
    /* Make sure that the transaction from this session isn't running. */
    assert(!_txn_running);

    /* Reset timestamps */
    _commit_ts = 0;
    _durable_ts = 0;
    _first_commit_ts = 0;
    _prepare_ts = 0;
    _read_ts = 0;
    _has_commit_ts = false;
    _ts_round_prepared = false;
    _ts_round_read = false;

    timestamp_manager *ts_manager = &timestamp_manager::get_timestamp_manager();
    std::map<std::string, std::string> config_map;

    ts_manager->parse_config(config, config_map);

    /* Check if the read or prepared timestamp should be rounded up. */
    auto pos = config_map.find("roundup_timestamps");
    if (pos != config_map.end()) {
        if (pos->second.find("read=true"))
            _ts_round_read = true;
        if (pos->second.find("prepared=true"))
            _ts_round_prepared = true;
        config_map.erase(pos);
    }

    /* Set and validate the read timestamp if it provided. */
    pos = config_map.find("read_timestamp");
    if (pos != config_map.end()) {
        WT_SIM_RET(ts_manager->validate_hex_value(pos->second, "read timestamp"));
        uint64_t read_ts = ts_manager->hex_to_decimal(pos->second);
        set_read_timestamp(read_ts);
        config_map.erase(pos);
    }

    /*
     * For now, the simulator only supports roundup_timestamp and read_timestamp in the config
     * string for begin_transaction(). Hence, we ignore ignore_prepare, isolation, name,
     * no_timestamp, operation_timeout_ms, priority and sync.
     */
    pos = config_map.find("ignore_prepare");
    if (pos != config_map.end())
        config_map.erase(pos);

    pos = config_map.find("isolation");
    if (pos != config_map.end())
        config_map.erase(pos);

    pos = config_map.find("name");
    if (pos != config_map.end())
        config_map.erase(pos);

    pos = config_map.find("no_timestamp");
    if (pos != config_map.end())
        config_map.erase(pos);

    pos = config_map.find("operation_timeout_ms");
    if (pos != config_map.end())
        config_map.erase(pos);

    /* Return an error if any config other than the ones mentioned above are passed. */
    if (!config_map.empty())
        return (EINVAL);

    /* Transaction can run successfully if we got to this point. */
    _txn_running = true;
    return (0);
}

void
session_simulator::rollback_transaction()
{
    /* Make sure that the transaction from this session is running. */
    assert(_txn_running);

    _txn_running = false;
}

int
session_simulator::commit_transaction(const std::string &config)
{
    /* Make sure that the transaction from this session is running. */
    assert(_txn_running);

    timestamp_manager *ts_manager = &timestamp_manager::get_timestamp_manager();
    std::map<std::string, std::string> config_map;

    ts_manager->parse_config(config, config_map);

    auto pos = config_map.find("commit_timestamp");
    if (pos != config_map.end()) {
        WT_SIM_RET(ts_manager->validate_hex_value(pos->second, "commit timestamp"));
        uint64_t commit_ts = ts_manager->hex_to_decimal(pos->second);
        if (commit_ts == 0)
            WT_SIM_RET_MSG(EINVAL, "Illegal commit timestamp: zero not permitted.");
        config_map.erase(pos);
        WT_SIM_RET(set_commit_timestamp(commit_ts));
    }

    _txn_running = false;

    return (0);
}

uint64_t
session_simulator::get_commit_timestamp() const
{
    return _commit_ts;
}

uint64_t
session_simulator::get_durable_timestamp() const
{
    return _durable_ts;
}

uint64_t
session_simulator::get_first_commit_timestamp() const
{
    return _first_commit_ts;
}

uint64_t
session_simulator::get_prepare_timestamp() const
{
    return _prepare_ts;
}

uint64_t
session_simulator::get_read_timestamp() const
{
    return _read_ts;
}

bool
session_simulator::get_ts_round_prepared() const
{
    return _ts_round_prepared;
}

bool
session_simulator::get_ts_round_read()
{
    return _ts_round_read;
}

int
session_simulator::set_commit_timestamp(uint64_t commit_ts)
{
    timestamp_manager *ts_manager = &timestamp_manager::get_timestamp_manager();
    WT_SIM_RET(ts_manager->validate_commit_timestamp(this, commit_ts));
    if (!_has_commit_ts) {
        _first_commit_ts = commit_ts;
        _has_commit_ts = true;
    }

    if (_ts_round_prepared && commit_ts < _prepare_ts)
        _commit_ts = _prepare_ts;
    else if (commit_ts >= _prepare_ts)
        _commit_ts = commit_ts;

    return (0);
}

void
session_simulator::set_durable_timestamp(uint64_t ts)
{
    _durable_ts = ts;
}

void
session_simulator::set_prepare_timestamp(uint64_t ts)
{
    _prepare_ts = ts;
}

bool
session_simulator::has_prepare_timestamp()
{
    return _prepare_ts != 0;
}

int
session_simulator::set_read_timestamp(uint64_t read_ts)
{
    timestamp_manager *ts_manager = &timestamp_manager::get_timestamp_manager();
    WT_SIM_RET(ts_manager->validate_read_timestamp(this, read_ts));

    /*
     * If the given timestamp is earlier than the oldest timestamp then round the read timestamp to
     * oldest timestamp.
     */
    connection_simulator *conn = &connection_simulator::get_connection();
    uint64_t oldest_ts = conn->get_oldest_ts();
    if (_ts_round_read && read_ts < oldest_ts)
        _read_ts = oldest_ts;
    else if (read_ts >= oldest_ts)
        _read_ts = read_ts;

    return (0);
}

int
session_simulator::decode_timestamp_config_map(std::map<std::string, std::string> &config_map,
  uint64_t &commit_ts, uint64_t &durable_ts, uint64_t &prepare_ts, uint64_t &read_ts)
{
    timestamp_manager *ts_manager = &timestamp_manager::get_timestamp_manager();
    auto pos = config_map.find("commit_timestamp");
    if (pos != config_map.end()) {
        WT_SIM_RET(ts_manager->validate_hex_value(pos->second, "commit timestamp"));
        commit_ts = ts_manager->hex_to_decimal(pos->second);
        if (commit_ts == 0)
            WT_SIM_RET_MSG(EINVAL, "Illegal commit timestamp: zero not permitted.");
        config_map.erase(pos);
    }

    pos = config_map.find("durable_timestamp");
    if (pos != config_map.end()) {
        WT_SIM_RET(ts_manager->validate_hex_value(pos->second, "durable timestamp"));
        durable_ts = ts_manager->hex_to_decimal(pos->second);
        if (durable_ts == 0)
            WT_SIM_RET_MSG(EINVAL, "Illegal durable timestamp: zero not permitted.");
        config_map.erase(pos);
    }

    pos = config_map.find("prepare_timestamp");
    if (pos != config_map.end()) {
        WT_SIM_RET(ts_manager->validate_hex_value(pos->second, "prepare timestamp"));
        prepare_ts = ts_manager->hex_to_decimal(pos->second);
        if (prepare_ts == 0)
            WT_SIM_RET_MSG(EINVAL, "Illegal prepare timestamp: zero not permitted.");
        config_map.erase(pos);
    }

    pos = config_map.find("read_timestamp");
    if (pos != config_map.end()) {
        WT_SIM_RET(ts_manager->validate_hex_value(pos->second, "read timestamp"));
        read_ts = ts_manager->hex_to_decimal(pos->second);
        if (read_ts == 0)
            WT_SIM_RET_MSG(EINVAL, "Illegal read timestamp: zero not permitted.");
        config_map.erase(pos);
    }

    return (config_map.empty() ? 0 : EINVAL);
}

int
session_simulator::timestamp_transaction(const std::string &config)
{
    /* Make sure that the transaction from this session is running. */
    assert(_txn_running);

    /* If no timestamp was supplied, there's nothing to do. */
    if (config.empty())
        return (0);

    timestamp_manager *ts_manager = &timestamp_manager::get_timestamp_manager();
    std::map<std::string, std::string> config_map;

    ts_manager->parse_config(config, config_map);

    uint64_t commit_ts = 0, durable_ts = 0, prepare_ts = 0, read_ts = 0;

    /* Decode a configuration string that may contain multiple timestamps and store them here. */
    WT_SIM_RET_MSG(
      decode_timestamp_config_map(config_map, commit_ts, durable_ts, prepare_ts, read_ts),
      "Incorrect config passed to 'timestamp_transaction': '" + config + "'");

    /* Check if the timestamps were included in the configuration string and set them. */
    if (commit_ts != 0)
        WT_SIM_RET(set_commit_timestamp(commit_ts));

    if (durable_ts != 0)
        set_durable_timestamp(durable_ts);

    if (prepare_ts != 0)
        set_prepare_timestamp(prepare_ts);

    if (read_ts != 0)
        WT_SIM_RET(set_read_timestamp(read_ts));

    return (0);
}

int
session_simulator::timestamp_transaction_uint(const std::string &ts_type, uint64_t ts)
{
    /* Make sure that the transaction from this session is running. */
    assert(_txn_running);

    /* Zero timestamp is not permitted. */
    if (ts == 0)
        WT_SIM_RET_MSG(EINVAL, "Illegal " + std::to_string(ts) + " timestamp: zero not permitted.");

    if (ts_type == "commit")
        WT_SIM_RET(set_commit_timestamp(ts));
    else if (ts_type == "durable")
        set_durable_timestamp(ts);
    else if (ts_type == "prepare")
        set_prepare_timestamp(ts);
    else if (ts_type == "read")
        WT_SIM_RET(set_read_timestamp(ts));
    else {
        WT_SIM_RET_MSG(
          EINVAL, "Invalid timestamp type (" + ts_type + ") passed to timestamp transaction uint.");
    }

    return (0);
}

int
session_simulator::query_timestamp(
  const std::string &config, std::string &hex_ts, bool &ts_supported)
{
    std::string query_timestamp;
    timestamp_manager *ts_manager = &timestamp_manager::get_timestamp_manager();
    std::map<std::string, std::string> config_map;

    ts_manager->parse_config(config, config_map);

    /* For query timestamp we only expect one config. */
    if (config_map.size() != 1)
        WT_SIM_RET_MSG(EINVAL, "Incorrect config (" + config + ") passed in query timestamp");

    auto pos = config_map.find("get");
    if (pos == config_map.end())
        WT_SIM_RET_MSG(EINVAL, "Incorrect config (" + config + ") passed in query timestamp");

    query_timestamp = pos->second;

    ts_supported = true;
    uint64_t ts;
    if (query_timestamp == "commit")
        ts = _commit_ts;
    else if (query_timestamp == "first_commit")
        ts = _first_commit_ts;
    else if (query_timestamp == "prepare")
        ts = _prepare_ts;
    else if (query_timestamp == "read")
        ts = _read_ts;
    else {
        ts_supported = false;
        WT_SIM_RET_MSG(EINVAL, "Incorrect config (" + config + ") passed in query timestamp");
    }

    /* Convert the timestamp from decimal to hex-decimal. */
    hex_ts = ts_manager->decimal_to_hex(ts);

    return (0);
}
