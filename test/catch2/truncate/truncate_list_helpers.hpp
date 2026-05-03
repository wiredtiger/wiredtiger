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
    auto operator=(const scoped_fast_truncate_enable &) -> scoped_fast_truncate_enable & = delete;

    scoped_fast_truncate_enable(scoped_fast_truncate_enable &&) = delete;
    auto operator=(scoped_fast_truncate_enable &&) -> scoped_fast_truncate_enable & = delete;

private:
    bool _previous;
};

[[nodiscard]] WT_ITEM make_item(std::string_view view);

[[nodiscard]] std::string_view as_view(const WT_ITEM &item);

} // namespace truncate_list_helpers
