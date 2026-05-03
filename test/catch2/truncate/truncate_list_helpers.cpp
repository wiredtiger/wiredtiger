/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

// Standard include:
#include <memory>
#include <string_view>

// WiredTiger include:
#include "wt_internal.h"
#include "truncate_list_helpers.hpp"

namespace truncate_list_helpers {

WT_ITEM
make_item(const std::string_view view)
{
    WT_ITEM item{};
    item.data = view.data();
    item.size = view.size();
    return item;
}

std::string_view
as_view(const WT_ITEM &item)
{
    return {static_cast<const char *>(item.data), item.size};
}

size_t
truncate_list_size(const WT_LAYERED_TABLE &table)
{
    size_t count = 0;
    WT_TRUNCATE *entry = nullptr;

    TAILQ_FOREACH (entry, &table.truncateqh, q) {
        ++count;
    }

    return count;
}

truncate_list_fixture::truncate_list_fixture()
    : _mock(mock_session::build_test_mock_session()), _session(_mock->get_wt_session_impl())
{
    _table.iface.name = "layered:truncate_list_fixture";
    TAILQ_INIT(&_table.truncateqh);
    REQUIRE(__wt_rwlock_init(_session, &_table.truncate_lock) == 0);
}

truncate_list_fixture::~truncate_list_fixture()
{
    WT_TRUNCATE *entry = nullptr;

    while ((entry = TAILQ_FIRST(&_table.truncateqh)) != nullptr) {
        TAILQ_REMOVE(&_table.truncateqh, entry, q);
        WT_DHANDLE_RELEASE(&_table.iface);
        __wt_free(_session, entry);
    }

    __wt_rwlock_destroy(_session, &_table.truncate_lock);
}

WT_SESSION_IMPL &
truncate_list_fixture::session() const
{
    return *_session;
}

WT_LAYERED_TABLE &
truncate_list_fixture::layered_table()
{
    return _table;
}

WT_TRUNCATE *
truncate_list_fixture::add_entry(const WT_ITEM &start, const WT_ITEM &stop)
{
    WT_TRUNCATE *entry = nullptr;
    REQUIRE(__wt_calloc_one(_session, &entry) == 0);

    entry->layered_table = &_table;
    WT_DHANDLE_ACQUIRE(&_table.iface);

    // This is a shallow copy. For the purposes of the tests, we are assuming that the WT_ITEMs are
    // constructed using string literals, which have static storage duration.
    entry->start_key = start;
    entry->stop_key = stop;

    TAILQ_INSERT_TAIL(&_table.truncateqh, entry, q);
    return entry;
}

uint32_t
truncate_list_fixture::reference_count() const
{
    return __wt_atomic_load_uint32_relaxed(&_table.iface.references);
}

} // namespace truncate_list_helpers
