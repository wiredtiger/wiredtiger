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
    VersionedMap(WT_SESSION* session, std::string& tableName);

    Value get(const Key& key) const;
    void set(const Key& key, const Value& value);

    // Methods that are the same or similar to those in std::map
    Value&& at(const Key& key) const;
    [[nodiscard]] uint64_t size() const;

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
    TransactionWrapper transactionWrapper(_session, "");
    CursorWrapper cursorWrapper(_session, _tableName);

    cursorWrapper.setKey(key);
    cursorWrapper.search();
    std::string value = cursorWrapper.getValue();
    cursorWrapper.reset();

    return (value);
}


template <class Key, class Value>
void
VersionedMap<Key, Value>::set(const Key& key, const Value& value)
{
    TransactionWrapper transactionWrapper(_session, "");
    CursorWrapper cursorWrapper(_session, _tableName);

    cursorWrapper.setKey(key);
    cursorWrapper.setValue(value);
    cursorWrapper.insert();
    cursorWrapper.reset();

    transactionWrapper.commit("");
}


template <class Key, class Value>
uint64_t
VersionedMap<Key, Value>::size() const
{
    TransactionWrapper transactionWrapper(_session, "");
    CursorWrapper cursorWrapper(_session, _tableName);

    int ret = cursorWrapper.next();
    utils::throwIfNonZero(ret);
    uint64_t numValues = 0;
    while (ret == 0) {
        numValues++;
        ret = cursorWrapper.next();
    }
    utils::throwIfNotEqual(ret, WT_NOTFOUND); // Check for end-of-table.

    return numValues;
}

#endif // WIREDTIGER_VERSIONED_MAP_H
