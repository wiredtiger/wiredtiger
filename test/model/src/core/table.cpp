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

#include <algorithm>

#include "model/table.h"
#include "wiredtiger.h"

namespace model {

/*
 * NONE --
 *     The "None" value.
 */
const data_value NONE = data_value::create_none();

/*
 * kv_item::add_update --
 *     Add an update.
 */
int
kv_item::add_update(kv_update &&update, bool must_exist, bool must_not_exist)
{
    std::lock_guard lock_guard(_lock);
    kv_update::timestamp_comparator cmp;

    /* If this is a non-timestamped update, there cannot be existing timestamped updates. */
    if (update.global()) {
        if (!_updates.empty() && !_updates[_updates.size() - 1].global())
            return (EINVAL);
    }

    /* Position the update. */
    auto i = std::upper_bound(_updates.begin(), _updates.end(), update, cmp);

    /* If need be, fail if the key does not exist. */
    if (must_exist) {
        if (_updates.empty())
            return (WT_NOTFOUND);

        auto j = i;
        if (j == _updates.begin() || (--j)->value() == NONE)
            return (WT_NOTFOUND);
    }

    /* If need be, fail if the key exists. */
    if (must_not_exist && !_updates.empty()) {
        auto j = i;
        if (j != _updates.begin() && (--j)->value() != NONE)
            return (WT_DUPLICATE_KEY);
    }

    /* Insert. */
    _updates.insert(i, update);
    return (0);
}

/*
 * kv_item::get --
 *     Get the corresponding value.
 */
const data_value &
kv_item::get(uint64_t timestamp)
{
    std::lock_guard lock_guard(_lock);
    kv_update::timestamp_comparator cmp;

    if (_updates.empty())
        return (NONE);

    auto i = std::upper_bound(_updates.begin(), _updates.end(), timestamp, cmp);
    if (i == _updates.begin())
        return (NONE);
    return ((--i)->value());
}

/*
 * kv_table::get --
 *     Get the value.
 */
const data_value &
kv_table::get(const data_value &key, uint64_t timestamp)
{
    kv_item *item = item_if_exists(key);
    if (item == NULL)
        return (NONE);
    return (item->get(timestamp));
}

/*
 * kv_table::insert --
 *     Insert into the table.
 */
int
kv_table::insert(const data_value &key, const data_value &value, uint64_t timestamp, bool overwrite)
{
    return (item(key).add_update(std::move(kv_update(value, timestamp)), false, !overwrite));
}

/*
 * kv_table::remove --
 *     Delete a value from the table. Return true if the value was deleted.
 */
int
kv_table::remove(const data_value &key, uint64_t timestamp)
{
    kv_item *item = item_if_exists(key);
    if (item == NULL)
        return (WT_NOTFOUND);
    return (item->add_update(std::move(kv_update(NONE, timestamp)), true, false));
}

/*
 * kv_table::update --
 *     Update a key in the table.
 */
int
kv_table::update(const data_value &key, const data_value &value, uint64_t timestamp, bool overwrite)
{
    return (item(key).add_update(std::move(kv_update(value, timestamp)), !overwrite, false));
}

} /* namespace model */
