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

#ifndef MODEL_CORE_H
#define MODEL_CORE_H

#include <map>
#include <mutex>
#include <string>
#include <vector>

/*
 * WT_TS_MAX --
 *     The maximum timestamp, typically used in reads where we would like to get the latest value.
 */
#ifndef WT_TS_MAX
#define WT_TS_MAX UINT64_MAX
#endif

/*
 * WT_TS_NONE --
 *     No timestamp, e.g., when performing a non-timestamped update.
 */
#ifndef WT_TS_NONE
#define WT_TS_NONE 0
#endif

namespace model {

/*
 * timestamp_t --
 *     The timestamp.
 */
typedef uint64_t timestamp_t;

/*
 * data_value --
 *     The data value stored in the model used for keys and values. We use a generic class, rather
 *     than a specific type such as std::string, to give us flexibility to change data types in the
 *     future, e.g., if this becomes necessary to explore additional code paths.
 */
class data_value {

public:
    /*
     * data_value::data_value --
     *     Create a new instance.
     */
    inline data_value(const char *data) : _data(data), _none(false) {}

    /*
     * data_value::data_value --
     *     Create a new instance.
     */
    inline data_value(const std::string &data) noexcept : _data(data), _none(false) {}

    /*
     * data_value::data_value --
     *     Create a new instance.
     */
    inline data_value(const std::string &&data) noexcept : _data(std::move(data)), _none(false) {}

    /*
     * data_value::create_none --
     *     Create an instance of a "None" value.
     */
    inline static data_value
    create_none() noexcept
    {
        data_value v(std::move(std::string("(none)")));
        v._none = true;
        return v;
    }

    /*
     * data_value::as_string --
     *     Return the data value as a string.
     */
    inline const std::string &
    as_string() const noexcept
    {
        return _data;
    }

    /*
     * data_value::operator== --
     *     Compare to another data value.
     */
    inline bool
    operator==(const data_value &other) const noexcept
    {
        if (_none && other._none)
            return true;
        if (_none != other._none)
            return false;
        return _data == other._data;
    }

    /*
     * data_value::operator!= --
     *     Compare to another data value.
     */
    inline bool
    operator!=(const data_value &other) const noexcept
    {
        return !(*this == other);
    }

    /*
     * data_value::operator< --
     *     Compare to another data value.
     */
    inline bool
    operator<(const data_value &other) const noexcept
    {
        if (_none != other._none)
            return _none;
        return _data < other._data;
    }

    /*
     * data_value::operator<= --
     *     Compare to another data value.
     */
    inline bool
    operator<=(const data_value &other) const noexcept
    {
        if (_none != other._none)
            return _none;
        if (_none)
            return true;
        return _data <= other._data;
    }

    /*
     * data_value::operator> --
     *     Compare to another data value.
     */
    inline bool
    operator>(const data_value &other) const noexcept
    {
        return !(*this <= other);
    }

    /*
     * data_value::operator> --
     *     Compare to another data value.
     */
    inline bool
    operator>=(const data_value &other) const noexcept
    {
        return !(*this < other);
    }

    /*
     * data_value::tombstone --
     *     Check if this is a None value.
     */
    inline bool
    none() const noexcept
    {
        return _none;
    }

private:
    std::string _data;
    bool _none;
};

/*
 * NONE --
 *     The "None" value.
 */
extern const data_value NONE;

/*
 * kv_update --
 *     The data value stored in a KV table, together with the relevant update information, such as
 *     the timestamp.
 */
class kv_update {

public:
    /*
     * kv_update::timestamp_comparator --
     *     The comparator that uses timestamps only.
     */
    struct timestamp_comparator {

        /*
         * kv_update::timestamp_comparator::operator() --
         *     Compare two updates.
         */
        bool
        operator()(const kv_update &left, const kv_update &right) const noexcept
        {
            return left._timestamp < right._timestamp;
        }

        /*
         * kv_update::timestamp_comparator::operator() --
         *     Compare the update to the given timestamp.
         */
        bool
        operator()(const kv_update &left, timestamp_t timestamp) const noexcept
        {
            return left._timestamp < timestamp;
        }

        /*
         * kv_update::timestamp_comparator::operator() --
         *     Compare the update to the given timestamp.
         */
        bool
        operator()(timestamp_t timestamp, const kv_update &right) const noexcept
        {
            return timestamp < right._timestamp;
        }

        /*
         * kv_update::timestamp_comparator::operator() --
         *     Compare two updates.
         */
        bool
        operator()(const kv_update *left, const kv_update *right) const noexcept
        {
            return left->_timestamp < right->_timestamp;
        }

        /*
         * kv_update::timestamp_comparator::operator() --
         *     Compare the update to the given timestamp.
         */
        bool
        operator()(const kv_update *left, timestamp_t timestamp) const noexcept
        {
            return left->_timestamp < timestamp;
        }

        /*
         * kv_update::timestamp_comparator::operator() --
         *     Compare the update to the given timestamp.
         */
        bool
        operator()(timestamp_t timestamp, const kv_update *right) const noexcept
        {
            return timestamp < right->_timestamp;
        }
    };

    /*
     * kv_update::kv_update --
     *     Create a new instance.
     */
    inline kv_update(const data_value &value, timestamp_t timestamp) noexcept
        : _value(value), _timestamp(timestamp)
    {
    }

    /*
     * kv_update::operator== --
     *     Compare to another instance.
     */
    inline bool
    operator==(const kv_update &other) const noexcept
    {
        return _value == other._value && _timestamp == other._timestamp;
    }

    /*
     * kv_update::operator!= --
     *     Compare to another instance.
     */
    inline bool
    operator!=(const kv_update &other) const noexcept
    {
        return !(*this == other);
    }

    /*
     * kv_update::operator< --
     *     Compare to another instance.
     */
    inline bool
    operator<(const kv_update &other) const noexcept
    {
        if (_timestamp != other._timestamp)
            return _timestamp < other._timestamp;
        if (_value != other._value)
            return _value < other._value;
        return true;
    }

    /*
     * kv_update::operator<= --
     *     Compare to another instance.
     */
    inline bool
    operator<=(const kv_update &other) const noexcept
    {
        return *this < other || *this == other;
    }

    /*
     * kv_update::operator> --
     *     Compare to another instance.
     */
    inline bool
    operator>(const kv_update &other) const noexcept
    {
        return !(*this <= other);
    }

    /*
     * kv_update::operator>= --
     *     Compare to another instance.
     */
    inline bool
    operator>=(const kv_update &other) const noexcept
    {
        return !(*this < other);
    }

    /*
     * kv_update::value --
     *     Get the value.
     */
    inline const data_value &
    value() const noexcept
    {
        return _value;
    }

    /*
     * kv_update::global --
     *     Check if this is a globally-visible, non-timestamped update.
     */
    inline bool
    global() const noexcept
    {
        return _timestamp == WT_TS_NONE;
    }

    /*
     * kv_update::value --
     *     Get the value.
     */
    inline timestamp_t
    timestamp() const noexcept
    {
        return _timestamp;
    }

private:
    timestamp_t _timestamp;
    data_value _value;
};

/*
 * kv_item --
 *     The value part of a key-value pair, together with its metadata and previous versions.
 */
class kv_item {

public:
    /*
     * kv_item::kv_item --
     *     Create a new instance.
     */
    inline kv_item() noexcept {}

    /*
     * kv_item::~kv_item --
     *     Delete the instance.
     */
    ~kv_item();

    /*
     * kv_item::add_update --
     *     Add an update.
     */
    int add_update(kv_update &&update, bool must_exist, bool overwrite);

    /*
     * kv_item::contains_any --
     *     Check whether the table contains the given value. If there are multiple values associated
     *     with the given timestamp, return true if any of them match.
     */
    bool contains_any(const data_value &value, timestamp_t timestamp = WT_TS_MAX);

    /*
     * kv_item::get --
     *     Get the corresponding value.
     */
    const data_value &get(timestamp_t timestamp = WT_TS_MAX);

private:
    std::mutex _lock;
    std::vector<kv_update *> _updates; /* sorted list of updates */
};

} /* namespace model */
#endif
