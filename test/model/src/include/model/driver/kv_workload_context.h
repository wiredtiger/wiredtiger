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

#ifndef MODEL_DRIVER_KV_WORKLOAD_CONTEXT_H
#define MODEL_DRIVER_KV_WORKLOAD_CONTEXT_H

#include <shared_mutex>
#include <unordered_map>
#include "model/driver/kv_workload.h"
#include "model/core.h"
#include "model/kv_database.h"
#include "model/kv_table.h"
#include "model/kv_transaction.h"
#include "wiredtiger.h"

namespace model {

/*
 * kv_workload_context --
 *     The workload context for the model.
 */
class kv_workload_context {

public:
    /*
     * kv_workload_context::kv_workload_context --
     *     Create a new workload context.
     */
    inline kv_workload_context(kv_database &database) : _database(database) {}

    /*
     * kv_workload_context::database --
     *     Get the database.
     */
    inline kv_database &
    database() const noexcept
    {
        return _database;
    }

    /*
     * kv_workload_context::add_table --
     *     Add a table.
     */
    inline void
    add_table(table_id_t id, kv_table_ptr ptr)
    {
        std::unique_lock lock(_tables_lock);
        if (_tables.find(id) != _tables.end())
            throw model_exception("A table with the given ID already exists");
        _tables[id] = ptr;
    }

    /*
     * kv_workload_context::table --
     *     Get the table.
     */
    inline kv_table_ptr
    table(table_id_t id) const
    {
        std::shared_lock lock(_tables_lock);
        auto i = _tables.find(id);
        if (i == _tables.end())
            throw model_exception("A table with the given ID does not exist");
        return i->second;
    }

    /*
     * kv_workload_context::add_transaction --
     *     Add a transaction.
     */
    inline void
    add_transaction(txn_id_t id, kv_transaction_ptr ptr)
    {
        std::unique_lock lock(_transactions_lock);
        if (_transactions.find(id) != _transactions.end())
            throw model_exception("A transaction with the given ID already exists");
        _transactions[id] = ptr;
    }

    /*
     * kv_workload_context::remove_transaction --
     *     Remove a transaction.
     */
    inline kv_transaction_ptr
    remove_transaction(txn_id_t id)
    {
        std::unique_lock lock(_transactions_lock);
        auto i = _transactions.find(id);
        if (i == _transactions.end())
            throw model_exception("A transaction with the given ID does not already exist");
        kv_transaction_ptr txn = i->second;
        _transactions.erase(i);
        return txn;
    }

    /*
     * kv_workload_context::transaction --
     *     Get the transaction.
     */
    inline kv_transaction_ptr
    transaction(txn_id_t id) const
    {
        std::shared_lock lock(_transactions_lock);
        auto i = _transactions.find(id);
        if (i == _transactions.end())
            throw model_exception("A transaction with the given ID does not exist");
        return i->second;
    }

private:
    kv_database &_database;

    mutable std::shared_mutex _tables_lock;
    std::unordered_map<table_id_t, kv_table_ptr> _tables;

    mutable std::shared_mutex _transactions_lock;
    std::unordered_map<txn_id_t, kv_transaction_ptr> _transactions;
};

} /* namespace model */
#endif
