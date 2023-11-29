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

#ifndef MODEL_KV_TABLE_H
#define MODEL_KV_TABLE_H

#include <atomic>
#include <map>
#include <mutex>
#include <string>

#include "model/data_value.h"
#include "model/kv_table_item.h"
#include "model/kv_update.h"
#include "model/verify.h"
#include "wiredtiger.h"

namespace model {

/*
 * kv_table --
 *     A database table with key-value pairs.
 */
class kv_table {

public:
    /*
     * kv_table::kv_table --
     *     Create a new instance.
     */
    inline kv_table(const char *name) : _name(name) {}

    /*
     * kv_table::name --
     *     Get the name of the table. The lifetime of the returned pointer follows the lifetime of
     *     this object (given that this is a pointer to a read-only field in this class). We return
     *     this as a regular C pointer so that it can be easily used in C APIs.
     */
    inline const char *
    name() const noexcept
    {
        return _name.c_str();
    }

    /*
     * kv_table::set_key_value_format --
     *     Set the key and value format of the table. This is not actually used by the model itself,
     *     but it is useful when interacting with WiredTiger.
     */
    inline void
    set_key_value_format(const char *key_format, const char *value_format) noexcept
    {
        _key_format = key_format;
        _value_format = value_format;
    }

    /*
     * kv_table::set_key_value_format --
     *     Set the key and value format of the table. This is not actually used by the model itself,
     *     but it is useful when interacting with WiredTiger.
     */
    inline void
    set_key_value_format(const std::string &key_format, const std::string &value_format) noexcept
    {
        _key_format = key_format;
        _value_format = value_format;
    }

    /*
     * kv_table::key_format --
     *     Return the key format of the table. This is returned as a C pointer, which has lifetime
     *     that ends when the key format changes, or when this object is destroyed.
     */
    inline const char *
    key_format() const
    {
        if (_key_format.empty())
            throw model_exception("The key format was not set");
        return _key_format.c_str();
    }

    /*
     * kv_table::value_format --
     *     Return the value format of the table. This is returned as a C pointer, which has lifetime
     *     that ends when the key format changes, or when this object is destroyed.
     */
    inline const char *
    value_format() const
    {
        if (_value_format.empty())
            throw model_exception("The value format was not set");
        return _value_format.c_str();
    }

    /*
     * kv_table::contains_any --
     *     Check whether the table contains the given key-value pair. If there are multiple values
     *     associated with the given timestamp, return true if any of them match.
     */
    bool contains_any(const data_value &key, const data_value &value,
      timestamp_t timestamp = k_timestamp_latest) const;

    /*
     * kv_table::get --
     *     Get the value. Return a copy of the value if is found, or NONE if not found. Throw an
     *     exception on error.
     */
    data_value get(const data_value &key, timestamp_t timestamp = k_timestamp_latest) const;

    /*
     * kv_table::get --
     *     Get the value. Return a copy of the value if is found, or NONE if not found. Throw an
     *     exception on error.
     */
    data_value get(kv_checkpoint_ptr ckpt, const data_value &key,
      timestamp_t timestamp = k_timestamp_latest) const;

    /*
     * kv_table::get --
     *     Get the value. Return a copy of the value if is found, or NONE if not found. Throw an
     *     exception on error.
     */
    data_value get(kv_transaction_ptr txn, const data_value &key) const;

    /*
     * kv_table::get_ext --
     *     Get the value and return the error code instead of throwing an exception.
     */
    int get_ext(
      const data_value &key, data_value &out, timestamp_t timestamp = k_timestamp_latest) const;

    /*
     * kv_table::get_ext --
     *     Get the value and return the error code instead of throwing an exception.
     */
    int get_ext(kv_checkpoint_ptr ckpt, const data_value &key, data_value &out,
      timestamp_t timestamp = k_timestamp_latest) const;

    /*
     * kv_table::get_ext --
     *     Get the value and return the error code instead of throwing an exception.
     */
    int get_ext(kv_transaction_ptr txn, const data_value &key, data_value &out) const;

    /*
     * kv_table::insert --
     *     Insert into the table.
     */
    int insert(const data_value &key, const data_value &value,
      timestamp_t timestamp = k_timestamp_none, bool overwrite = true);

    /*
     * kv_table::insert --
     *     Insert into the table.
     */
    int insert(kv_transaction_ptr txn, const data_value &key, const data_value &value,
      bool overwrite = true);

    /*
     * kv_table::remove --
     *     Delete a value from the table.
     */
    int remove(const data_value &key, timestamp_t timestamp = k_timestamp_none);

    /*
     * kv_table::remove --
     *     Delete a value from the table.
     */
    int remove(kv_transaction_ptr txn, const data_value &key);

    /*
     * kv_table::fix_timestamps --
     *     Fix the commit and durable timestamps for the corresponding update. We need to do this,
     *     because WiredTiger transaction API specifies the commit timestamp after performing the
     *     operations, not before.
     */
    void fix_timestamps(const data_value &key, txn_id_t txn_id, timestamp_t commit_timestamp,
      timestamp_t durable_timestamp);

    /*
     * kv_table::rollback_updates --
     *     Roll back updates of an aborted transaction.
     */
    void rollback_updates(const data_value &key, txn_id_t txn_id);

    /*
     * kv_table::update --
     *     Update a key in the table.
     */
    int update(const data_value &key, const data_value &value,
      timestamp_t timestamp = k_timestamp_none, bool overwrite = true);

    /*
     * kv_table::update --
     *     Update a key in the table.
     */
    int update(kv_transaction_ptr txn, const data_value &key, const data_value &value,
      bool overwrite = true);

    /*
     * kv_table::verify --
     *     Verify the table by comparing a WiredTiger table against the model. Throw an exception on
     *     verification error.
     */
    inline void
    verify(WT_CONNECTION *connection)
    {
        kv_table_verifier(*this).verify(connection);
    }

    /*
     * kv_table::verify_noexcept --
     *     Verify the table by comparing a WiredTiger table against the model.
     */
    inline bool
    verify_noexcept(WT_CONNECTION *connection) noexcept
    {
        return kv_table_verifier(*this).verify_noexcept(connection);
    }

    /*
     * kv_table::verify_cursor --
     *     Create a verification cursor for the table. This method is not thread-safe. In fact,
     *     nothing is thread-safe until the returned cursor stops being used!
     */
    kv_table_verify_cursor verify_cursor();

protected:
    /*
     * kv_table::item --
     *     Get the item that corresponds to the given key, creating one if need be.
     */
    inline kv_table_item &
    item(const data_value &key)
    {
        std::lock_guard lock_guard(_lock);
        return _data[key]; /* this automatically instantiates the item if it does not exist */
    }

    /*
     * kv_table::item_if_exists --
     *     Get the item that corresponds to the given key, if it exists.
     */
    inline kv_table_item *
    item_if_exists(const data_value &key)
    {
        std::lock_guard lock_guard(_lock);
        auto i = _data.find(key);
        if (i == _data.end())
            return nullptr;
        return &i->second;
    }

    /*
     * kv_table::item_if_exists --
     *     Get the item that corresponds to the given key, if it exists.
     */
    inline const kv_table_item *
    item_if_exists(const data_value &key) const
    {
        std::lock_guard lock_guard(_lock);
        auto i = _data.find(key);
        if (i == _data.end())
            return nullptr;
        return &i->second;
    }

private:
    std::string _name;

    std::string _key_format;
    std::string _value_format;

    /*
     * This data structure is designed so that the global lock is only necessary for the map
     * operations; it is okay to release the lock while the caller is still operating on the data
     * returned from the map. To keep this property going, do not remove the any elements from the
     * map. We are keeping the map sorted, so that we can easily compare the model's state with
     * WiredTiger's state. It would also help us in the future if we decide to model range scans.
     */
    std::map<data_value, kv_table_item> _data;
    mutable std::mutex _lock;
};

/*
 * kv_table_ptr --
 *     A shared pointer to the table.
 */
using kv_table_ptr = std::shared_ptr<kv_table>;

} /* namespace model */
#endif
