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

#ifndef MODEL_DRIVER_KV_WORKLOAD_CONTEXT_WT_H
#define MODEL_DRIVER_KV_WORKLOAD_CONTEXT_WT_H

#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include "model/driver/kv_workload.h"
#include "model/core.h"
#include "wiredtiger.h"

namespace model {

/*
 * kv_workload_context_wt --
 *     The workload context for WiredTiger.
 */
class kv_workload_context_wt {

public:
    /*
     * session_context --
     *     The WiredTiger session context.
     */
    class session_context {

        using cursor_id_t = int;
        static constexpr cursor_id_t k_cursors_per_table = 16;

    public:
        /*
         * session_context::session_context --
         *     Create the context.
         */
        inline session_context(kv_workload_context_wt &workload_context, WT_SESSION *session)
            : _session(session), _workload_context(workload_context)
        {
        }

        /*
         * session_context::~session_context --
         *     Destroy the context, alongside the corresponding resources.
         */
        ~session_context();

        /*
         * session_context::session --
         *     Get the session.
         */
        inline WT_SESSION *
        session() const noexcept
        {
            return _session;
        }

        /*
         * session_context::cursor --
         *     Get a cursor. Create one if it does not already exist. Use the second argument to get
         *     and/or create additional cursors for the given table.
         */
        WT_CURSOR *cursor(table_id_t table_id, int table_cur_id = 0);

    private:
        /*
         * session_context::cursor_id --
         *     Get a cursor ID.
         */
        static inline cursor_id_t
        cursor_id(table_id_t table_id, int table_cur_id)
        {
            if (table_cur_id < 0 || table_cur_id >= k_cursors_per_table)
                throw model_exception("Cursor ID out of range");
            return (cursor_id_t)(table_id * k_cursors_per_table + table_cur_id);
        }

    private:
        WT_SESSION *_session;
        kv_workload_context_wt &_workload_context;

        std::unordered_map<cursor_id_t, WT_CURSOR *> _cursors; /* One cursor per table. */
    };

    /*
     * session_context_ptr --
     *     The shared pointer for the session context.
     */
    using session_context_ptr = std::shared_ptr<session_context>;

public:
    /*
     * kv_workload_context_wt::kv_workload_context_wt --
     *     Create a new workload context.
     */
    inline kv_workload_context_wt(const char *home, const char *connection_config)
        : _connection(nullptr), _connection_config(connection_config), _home(home)
    {
    }

    /*
     * kv_workload_context_wt::~kv_workload_context_wt --
     *     Clean up the workload context.
     */
    ~kv_workload_context_wt();

    /*
     * kv_workload_context_wt::connection --
     *     Get the connection.
     */
    inline WT_CONNECTION *
    connection() const
    {
        if (_connection == nullptr)
            throw model_exception("WiredTiger is not open");
        return _connection;
    }

    /*
     * kv_workload_context_wt::wiredtiger_open --
     *     Open WiredTiger.
     */
    void wiredtiger_open();

    /*
     * kv_workload_context_wt::wiredtiger_close --
     *     Close WiredTiger.
     */
    void wiredtiger_close();

    /*
     * kv_workload_context_wt::add_table_uri --
     *     Add a table URI.
     */
    inline void
    add_table_uri(table_id_t id, std::string uri)
    {
        std::unique_lock lock(_table_uris_lock);
        if (_table_uris.find(id) != _table_uris.end())
            throw model_exception("A table with the given ID already exists");
        _table_uris[id] = uri;
    }

    /*
     * kv_workload_context_wt::table_uri --
     *     Get the table URI. The returned C string pointer is valid for the rest of the duration of
     *     this object.
     */
    inline const char *
    table_uri(table_id_t id) const
    {
        std::shared_lock lock(_table_uris_lock);
        auto i = _table_uris.find(id);
        if (i == _table_uris.end())
            throw model_exception("A table with the given ID does not exist");
        return i->second.c_str();
    }

    /*
     * kv_workload_context_wt::allocate_txn_session --
     *     Allocate a session context for a transaction.
     */
    session_context_ptr allocate_txn_session(txn_id_t id);

    /*
     * kv_workload_context_wt::remove_txn_session --
     *     Remove a session context from the transaction.
     */
    inline session_context_ptr
    remove_txn_session(txn_id_t id)
    {
        std::unique_lock lock(_sessions_lock);
        auto i = _sessions.find(id);
        if (i == _sessions.end())
            throw model_exception("A session with the given ID does not already exist");
        session_context_ptr session = i->second;
        _sessions.erase(i);
        return session;
    }

    /*
     * kv_workload_context_wt::session_context --
     *     Get the session context associated with the given transaction.
     */
    inline session_context_ptr
    txn_session(txn_id_t id) const
    {
        std::shared_lock lock(_sessions_lock);
        auto i = _sessions.find(id);
        if (i == _sessions.end())
            throw model_exception("A session with the given ID does not exist");
        return i->second;
    }

private:
    WT_CONNECTION *_connection;
    std::string _connection_config;
    std::string _home;

    mutable std::shared_mutex _table_uris_lock;
    std::unordered_map<table_id_t, std::string> _table_uris;

    mutable std::shared_mutex _sessions_lock;
    std::unordered_map<txn_id_t, session_context_ptr> _sessions;
};

} /* namespace model */
#endif
