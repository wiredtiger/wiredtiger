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

#include "timestamp_manager.h"

#include <iostream>
#include <sstream>
#include <string>

#include "connection_simulator.h"
#include "error_simulator.h"

timestamp_manager::timestamp_manager() {}

/* Get an instance of timestamp_manager class. */
timestamp_manager &
timestamp_manager::get_timestamp_manager()
{
    static timestamp_manager _timestamp_manager_instance;
    return (_timestamp_manager_instance);
}

uint64_t
timestamp_manager::hex_to_decimal(const std::string &hex_ts)
{
    std::stringstream stream;
    uint64_t ts;

    stream << hex_ts;
    stream >> std::hex >> ts;

    return (ts);
}

std::string
timestamp_manager::decimal_to_hex(const uint64_t ts)
{
    std::stringstream stream;
    stream << std::hex << ts;

    return (stream.str());
}

int
timestamp_manager::validate_hex_value(const std::string &ts_string)
{
    /* Check that the timestamp string has valid hexadecimal characters. */
    for (auto &ch : ts_string)
        if (!std::isxdigit(ch))
            WT_SIM_RET_MSG(EINVAL, "Illegal commit timestamp: invalid hex value.");

    return (0);
}

/* Remove leading and trailing spaces from a string. */
std::string
timestamp_manager::trim(std::string str)
{
    str.erase(str.find_last_not_of(" ") + 1);
    str.erase(0, str.find_first_not_of(" "));
    return str;
}

/* Parse config string to a config map. */
void
timestamp_manager::parse_config(
  const std::string &config, std::map<std::string, std::string> &config_map)
{
    std::istringstream conf(config);
    std::string token;

    while (std::getline(conf, token, ',')) {
        int pos = token.find('=');
        if (token != "(null)") {
            if (pos == -1)
                config_map.insert({trim(token), ""});
            else
                config_map.insert({trim(token.substr(0, pos)), trim(token.substr(pos + 1))});
        }
    }
}

/*
 * Validate both oldest and stable timestamps.
 * 1) Validation fails if Illegal timestamp value is passed (if less than or equal to 0).
 * 2) It is a no-op to set the oldest or stable timestamps behind the global
 *    values. Hence, ignore and continue validating.
 * 3) Validation fails if oldest is greater than the stable timestamp.
 */
int
timestamp_manager::validate_oldest_and_stable_ts(
  uint64_t &new_stable_ts, uint64_t &new_oldest_ts, bool &has_oldest, bool &has_stable)
{
    /* No need to validate timestamps if timestamps are not passed in the config. */
    if (!has_oldest && !has_stable)
        return (0);

    connection_simulator *conn = &connection_simulator::get_connection();

    /* If config has oldest timestamp */
    if (has_oldest) {
        /* Cannot proceed any further, if oldest timestamp value <= 0 validation fails! */
        if ((int64_t)new_oldest_ts <= 0)
            WT_SIM_RET_MSG(EINVAL,
              "Illegal timestamp value, 'oldest timestamp' : '" +
                std::to_string((int64_t)new_oldest_ts) + "' is less than or equal to zero.");
        /* It is a no-op to set the new oldest timestamps behind the current oldest timestamp. */
        if (new_oldest_ts <= conn->get_oldest_ts())
            has_oldest = false;
    }

    /* If config has stable timestamp */
    if (has_stable) {
        /* Cannot proceed any further, if stable timestamp value <= 0 validation fails! */
        if ((int64_t)new_stable_ts <= 0)
            WT_SIM_RET_MSG(EINVAL,
              "Illegal timestamp value, 'stable timestamp' : '" +
                std::to_string((int64_t)new_stable_ts) + "' is less than or equal to zero.");
        /* It is a no-op to set the new stable timestamps behind the current stable timestamp. */
        if (new_stable_ts <= conn->get_stable_ts())
            has_stable = false;
    }

    /* No need to validate timestamps if stable or/and oldest were behind the global values. */
    if (!has_oldest && !has_stable)
        return (0);

    /* No need to validate timestamps if there is no new and no current oldest timestamp. */
    if (!has_oldest && conn->get_oldest_ts() == 0)
        return (0);

    /* No need to validate timestamps if there is no new and no current stable timestamp. */
    if (!has_stable && conn->get_stable_ts() == 0)
        return (0);

    /*
     * If the oldest timestamp was not passed in the config or was behind the current oldest
     * timestamp, modify the new_oldest_ts to the current oldest timestamp.
     */
    if ((!has_oldest && conn->get_oldest_ts() != 0))
        new_oldest_ts = conn->get_oldest_ts();

    /*
     * If the stable timestamp was not passed in the config or was behind the current stable
     * timestamp, modify the new_stable_ts to the current stable timestamp
     */
    if ((!has_stable && conn->get_stable_ts() != 0))
        new_stable_ts = conn->get_stable_ts();

    /* Validation fails if oldest is greater than the stable timestamp. */
    if (new_oldest_ts > new_stable_ts)
        WT_SIM_RET_MSG(EINVAL,
          "'oldest timestamp' (" + std::to_string(new_oldest_ts) +
            ") must not be later than 'stable timestamp' (" + std::to_string(new_stable_ts) + ")");

    return (0);
}

/*
 * Validate durable timestamp.
 * 1) Validation fails if Illegal timestamp value is passed (if less than or equal to 0).
 */
int
timestamp_manager::validate_durable_ts(
  const uint64_t &new_durable_ts, const bool &has_durable) const
{
    /* If durable timestamp was not passed in the config, no validation is needed. */
    if (!has_durable)
        return (0);

    /* Illegal timestamp value (if less than or equal to 0). Validation fails!  */
    if ((int64_t)new_durable_ts <= 0)
        WT_SIM_RET_MSG(EINVAL,
          "Illegal timestamp value, 'durable timestamp' : '" +
            std::to_string((int64_t)new_durable_ts) + "' is less than or equal to zero.");

    return (0);
}

/*
 * Validate the read timestamp. The constraints on the read timestamp are:
 * 1) The read timestamp can only be set before a transaction is prepared
 * 2) Read timestamps can only be set once.
 * 3) The read timestamp must be greater than or equal to the oldest timestamp unless rounding
 * the read timestamp is enabled.
 */
int
timestamp_manager::validate_read_timestamp(session_simulator *session, const uint64_t read_ts) const
{
    /* The read timestamp can't be set after a transaction is prepared. */
    if (session->get_prepare_timestamp() != 0) {
        WT_SIM_RET_MSG(EINVAL, "Cannot set a read timestamp after a transaction is prepared.");
    }

    /* Read timestamps can't change once set. */
    if (session->get_read_timestamp() != 0) {
        WT_SIM_RET_MSG(EINVAL, "A read_timestamp can only be set once per transaction.");
    }

    /*
     * We cannot set the read timestamp to be earlier than the oldest timestamp if we're not
     * rounding to the oldest.
     */
    connection_simulator *conn = &connection_simulator::get_connection();
    if (read_ts < conn->get_oldest_ts() && !session->get_ts_round_read()) {
        WT_SIM_RET_MSG(EINVAL,
          "Cannot set read timestamp before the oldest timestamp, unless we round the read "
          "timestamp up to the oldest.");
    }

    return (0);
}

/* Validate the commit timestamp. */
int
timestamp_manager::validate_commit_timestamp(
  session_simulator *session, const uint64_t commit_ts) const
{
    uint64_t prepare_ts = session->get_prepare_timestamp();
    /*
     * We cannot set the commit timestamp to be earlier than the first commit timestamp when setting
     * the commit timestamp multiple times within a transaction.
     */
    uint64_t first_commit_ts = session->get_first_commit_timestamp();
    if (first_commit_ts != 0 && commit_ts < first_commit_ts) {
        WT_SIM_RET_MSG(EINVAL,
          "commit timestamp " + std::to_string(commit_ts) +
            " older than the first commit timestamp " + std::to_string(first_commit_ts) +
            " for this transaction");
    }

    /*
     * For a non-prepared transaction the commit timestamp should not be less or equal to the oldest
     * and/or stable timestamp.
     */
    connection_simulator *conn = &connection_simulator::get_connection();
    uint64_t oldest_ts = conn->get_oldest_ts();
    if (oldest_ts != 0 && commit_ts < oldest_ts) {
        WT_SIM_RET_MSG(EINVAL,
          "commit timestamp " + std::to_string(commit_ts) + "is less than the oldest timestamp " +
            std::to_string(oldest_ts));
    }

    uint64_t stable_ts = conn->get_stable_ts();
    if (stable_ts != 0 && commit_ts <= stable_ts) {
        WT_SIM_RET_MSG(EINVAL,
          "commit timestamp " + std::to_string(commit_ts) + "must be after the stable timestamp " +
            std::to_string(stable_ts));
    }

    /* The commit timestamp must be greater than the latest active read timestamp. */
    uint64_t latest_active_read = conn->get_latest_active_read();
    if (latest_active_read >= commit_ts)
        WT_SIM_RET_MSG(EINVAL,
          "commit timestamp " + std::to_string(commit_ts) +
            "must be after all active read timestamps " + std::to_string(latest_active_read));

    /*
     * For a prepared transaction, the commit timestamp should not be less than the prepare
     * timestamp. Also, the commit timestamp cannot be set before the transaction has actually been
     * prepared.
     *
     * If the commit timestamp is less than the oldest timestamp and the transaction is configured
     * to roundup timestamps of a prepared transaction, then we will roundup the commit timestamp to
     * the prepare timestamp of the transaction.
     */
    if (session->has_prepare_timestamp()) {
        if (!session->get_ts_round_prepared() && commit_ts < prepare_ts)
            WT_SIM_RET_MSG(EINVAL,
              "commit timestamp " + std::to_string(commit_ts) +
                "is less than the prepare timestamp " + std::to_string(prepare_ts) +
                " for this transaction.");
    }

    return (0);
}
