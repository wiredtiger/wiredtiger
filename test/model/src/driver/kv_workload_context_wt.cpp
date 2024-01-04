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
#include "model/driver/kv_workload_context_wt.h"

namespace model {

/*
 * session_context::~session_context --
 *     Destroy the context, alongside the corresponding resources.
 */
kv_workload_context_wt::session_context::~session_context()
{
    int r;

    for (auto p : _cursors) {
        WT_CURSOR *cursor = p.second;
        r = cursor->close(cursor);
        /* We cannot fail the cleanup, so just print a warning */
        if (r != 0)
            std::cerr << "Could not close a cursor: " << _session->strerror(_session, r) << " ("
                      << r << ")" << std::endl;
    }

    r = _session->close(_session, nullptr);
    /* We cannot fail the cleanup, so just print a warning */
    if (r != 0)
        std::cerr << "Could not close a session: " << wiredtiger_strerror(r) << " (" << r << ")"
                  << std::endl;
}

/*
 * session_context::cursor --
 *     Get a cursor. Create one if it does not already exist. Use the second argument to get and/or
 *     create additional cursors for the given table.
 */
WT_CURSOR *
kv_workload_context_wt::session_context::cursor(table_id_t table_id, int table_cur_id)
{
    cursor_id_t id = cursor_id(table_id, table_cur_id);
    auto i = _cursors.find(id);
    if (i != _cursors.end())
        return i->second;

    WT_CURSOR *cursor;
    int ret = _session->open_cursor(
      _session, _workload_context.table_uri(table_id), nullptr, nullptr, &cursor);
    if (ret != 0)
        throw wiredtiger_exception("Failed to open a cursor", ret);

    _cursors[id] = cursor;
    return cursor;
}

/*
 * kv_workload_context_wt::allocate_txn_session --
 *     Allocate a session context for a transaction.
 */
kv_workload_context_wt::session_context_ptr
kv_workload_context_wt::allocate_txn_session(txn_id_t id)
{
    std::unique_lock lock(_sessions_lock);
    if (_sessions.find(id) != _sessions.end())
        throw model_exception("A session with the given ID already exists");

    WT_SESSION *session;
    int ret = _connection->open_session(_connection, nullptr, nullptr, &session);
    if (ret != 0)
        throw wiredtiger_exception("Failed to open a session", ret);

    auto context = std::make_shared<session_context>(*this, session);
    _sessions[id] = context;
    return context;
}

} /* namespace model */
