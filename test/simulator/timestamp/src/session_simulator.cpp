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
#include "error_simulator.h"

#include <cassert>
#include <iostream>

session_simulator::session_simulator() : _txn_running(false) {}

void
session_simulator::begin_transaction()
{
    /* Make sure that the transaction from this session isn't running. */
    assert(!_txn_running);

    _txn_running = true;
}

void
session_simulator::rollback_transaction()
{
    /* Make sure that the transaction from this session is running. */
    assert(_txn_running);

    _txn_running = false;
}

void
session_simulator::commit_transaction()
{
    /* Make sure that the transaction from this session is running. */
    assert(_txn_running);

    _txn_running = false;
}

void
session_simulator::set_commit_timestamp(uint64_t ts)
{
    _commit_ts = ts;
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

void
session_simulator::set_read_timestamp(uint64_t ts)
{
    _read_ts = ts;
}

int
session_simulator::timestamp_transaction_uint(const std::string &ts_type, uint64_t ts)
{
    /* Zero timestamp is not permitted. */
    if (ts == 0) {
        WT_SIM_RET_MSG(EINVAL, "Illegal " + std::to_string(ts) + " timestamp: zero not permitted.");
    }

    if (ts_type == "commit") {
        set_commit_timestamp(ts);
    } else if (ts_type == "durable") {
        set_durable_timestamp(ts);
    } else if (ts_type == "prepare") {
        set_prepare_timestamp(ts);
    } else if (ts_type == "read") {
        set_read_timestamp(ts);
    }

    return (0);
}
