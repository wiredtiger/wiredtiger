/*-
* Copyright (c) 2014-present MongoDB, Inc.
* Copyright (c) 2008-2014 WiredTiger, Inc.
*	All rights reserved.
*
* See the file LICENSE for redistribution information.
*/

#ifndef WIREDTIGER_VERSIONED_MAP_H
#define WIREDTIGER_VERSIONED_MAP_H

#include <map>
#include <string>
#include "wiredtiger.h"
#include "../utils.h"
#include "cursor_wrapper.h"
#include "transaction_wrapper.h"


template<class Key, class Value>
class VersionedMap
{
    public:

    typedef Key key_map;
    typedef Value mapped_type;
    typedef uint64_t size_type;

    VersionedMap(WT_SESSION* session, std::string& tableName);

    Value get(const Key& key) const;
    Value get_transaction_wrapped(const Key& key, const std::string& config, std::optional<uint64_t> timeStamp) const;

    void set(const Key& key, const Value& value);
    void set_transaction_wrapped(const Key& key, const Value& value, const std::string& config);

    // Methods that are the same or similar to those in std::map
    Value&& at(const Key& key) const;
    [[nodiscard]] size_type size() const;
    [[nodiscard]] size_type size_transaction_wrapped(const std::string& config) const;

    private:
    WT_SESSION* _session; // This class does not own this pointer so should not free it.
    std::string _tableName;
};


template <class Key, class Value>
VersionedMap<Key, Value>::VersionedMap(WT_SESSION* session, std::string& tableName)
    : _session(session),
      _tableName(tableName)
{
}


template <class Key, class Value>
Value&&
VersionedMap<Key, Value>::at(const Key& key) const
{
    return nullptr;
}


template <class Key, class Value>
Value
VersionedMap<Key, Value>::get(const Key& key) const
{
    CursorWrapper cursorWrapper(_session, _tableName);

    cursorWrapper.setKey(key);
    cursorWrapper.search();
    std::string value = cursorWrapper.getValue();
    cursorWrapper.reset();

    return (value);
}


template <class Key, class Value>
Value
VersionedMap<Key, Value>::get_transaction_wrapped(const Key& key, const std::string& config, std::optional<uint64_t> timeStamp) const
{
    TransactionWrapper transactionWrapper(_session, config);
    if (timeStamp) {
        uint64_t ts = timeStamp.value();
        int ret = _session->timestamp_transaction_uint(_session, WT_TS_TXN_TYPE_READ, ts);
        utils::throwIfNonZero(ret);
    }
    return (get(key));
}


template <class Key, class Value>
void
VersionedMap<Key, Value>::set(const Key& key, const Value& value)
{
    CursorWrapper cursorWrapper(_session, _tableName);
    cursorWrapper.setKey(key);
    cursorWrapper.setValue(value);
    cursorWrapper.insert();
    cursorWrapper.reset();
}


template <class Key, class Value>
void
VersionedMap<Key, Value>::set_transaction_wrapped(const Key &key, const Value &value, const std::string& config)
{
    TransactionWrapper transactionWrapper(_session, config);
    set(key, value);
    transactionWrapper.commit("");
}


template <class Key, class Value>
typename VersionedMap<Key, Value>::size_type
VersionedMap<Key, Value>::size() const
{
    CursorWrapper cursorWrapper(_session, _tableName);

    int ret = cursorWrapper.next();
    utils::throwIfNonZero(ret);
    size_type numValues = 0;
    while (ret == 0) {
        numValues++;
        ret = cursorWrapper.next();
    }
    utils::throwIfNotEqual(ret, WT_NOTFOUND); // Check for end-of-table.

    return numValues;
}


template <class Key, class Value>
typename VersionedMap<Key, Value>::size_type
VersionedMap<Key, Value>::size_transaction_wrapped(const std::string& config) const
{
    TransactionWrapper transactionWrapper(_session, config);
    return size();
}

#endif // WIREDTIGER_VERSIONED_MAP_H
