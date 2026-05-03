/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#pragma once

// Standard include:
#include <cstddef>
#include <cstdint>
#include <string_view>

// External include:
#include <catch2/catch.hpp>

// WiredTiger include:
#include "wt_internal.h"
#include "wrappers/mock_session.h"

namespace truncate_list_helpers {

class scoped_fast_truncate_enable {
public:
    scoped_fast_truncate_enable() : _previous(__wt_process.disagg_fast_truncate_2026)
    {
        __wt_process.disagg_fast_truncate_2026 = true;
    }

    ~scoped_fast_truncate_enable()
    {
        __wt_process.disagg_fast_truncate_2026 = _previous;
    }

    scoped_fast_truncate_enable(const scoped_fast_truncate_enable &) = delete;
    scoped_fast_truncate_enable &operator=(const scoped_fast_truncate_enable &) = delete;

    scoped_fast_truncate_enable(scoped_fast_truncate_enable &&) = delete;
    scoped_fast_truncate_enable &operator=(scoped_fast_truncate_enable &&) = delete;

private:
    bool _previous;
};

[[nodiscard]] WT_ITEM make_item(std::string_view view);

[[nodiscard]] std::string_view as_view(const WT_ITEM &item);

[[nodiscard]] size_t truncate_list_size(const WT_LAYERED_TABLE &table);

class truncate_list_fixture {
public:
    truncate_list_fixture();
    ~truncate_list_fixture();

    truncate_list_fixture(const truncate_list_fixture &) = delete;
    truncate_list_fixture &operator=(const truncate_list_fixture &) = delete;

    truncate_list_fixture(truncate_list_fixture &&) = delete;
    truncate_list_fixture &operator=(truncate_list_fixture &&) = delete;

    [[nodiscard]] WT_SESSION_IMPL &session() const;
    [[nodiscard]] WT_LAYERED_TABLE &layered_table();
    WT_TRUNCATE *add_entry(const WT_ITEM &start, const WT_ITEM &stop);
    [[nodiscard]] uint32_t reference_count() const;

private:
    scoped_fast_truncate_enable _enable;
    std::shared_ptr<mock_session> _mock;
    WT_SESSION_IMPL *_session;
    mutable WT_LAYERED_TABLE _table{};
};

} // namespace truncate_list_helpers
