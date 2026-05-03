/*-
 * Copyright (c) 2014-present MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

// Standard include:
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

} // namespace truncate_list_helpers
