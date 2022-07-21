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
    WT_CURSOR* cursor = nullptr;
    utils::throwIfNonZero(_session->open_cursor(_session, _tableName.c_str(), nullptr, nullptr, &cursor));
    utils::throwIfNonZero(_session->begin_transaction(_session, nullptr));

    const char* pKey = key.c_str();  // TODO: This only works it Value is a std::string, so we need to change this
    cursor->set_key(cursor, pKey);
    utils::throwIfNonZero(cursor->search(cursor));

    char const *pValue = nullptr;
    utils::throwIfNonZero(cursor->get_value(cursor, &pValue));
    utils::throwIfNonZero(cursor->reset(cursor));

    utils::throwIfNonZero(_session->rollback_transaction(_session, nullptr));
    utils::throwIfNonZero(cursor->close(cursor));
    return (Value(pValue));
}


template <class Key, class Value>
void
VersionedMap<Key, Value>::set(const Key& key, const Value& value)
{
    WT_CURSOR* cursor = nullptr;
    utils::throwIfNonZero(_session->open_cursor(_session, _tableName.c_str(), nullptr, nullptr, &cursor));
    utils::throwIfNonZero(_session->begin_transaction(_session, nullptr));

    // TODO: This only works it Value is a std::string, so we need to change this
    cursor->set_key(cursor, key.c_str());
    cursor->set_value(cursor, value.c_str());
    utils::throwIfNonZero(cursor->insert(cursor));
    utils::throwIfNonZero(cursor->reset(cursor));

    utils::throwIfNonZero(_session->commit_transaction(_session, nullptr));
    utils::throwIfNonZero(cursor->close(cursor));
}


template <class Key, class Value>
uint64_t
VersionedMap<Key, Value>::size() const
{
    WT_CURSOR* cursor = nullptr;
    utils::throwIfNonZero(_session->open_cursor(_session, _tableName.c_str(), nullptr, nullptr, &cursor));
    utils::throwIfNonZero(_session->begin_transaction(_session, nullptr));
    utils::throwIfNonZero(cursor->reset(cursor));
    int ret = cursor->next(cursor);
    utils::throwIfNonZero(ret);

    uint64_t numValues = 0;
    while (ret == 0) {
        numValues++;
        ret = cursor->next(cursor);
    }
    utils::throwIfNotEqual(ret, WT_NOTFOUND); // Check for end-of-table.

    utils::throwIfNonZero(_session->rollback_transaction(_session, nullptr));
    utils::throwIfNonZero(cursor->close(cursor));
    return numValues;
}

#endif // WIREDTIGER_VERSIONED_MAP_H
