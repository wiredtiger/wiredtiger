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

#ifndef MODEL_TABLE_H
#define MODEL_TABLE_H

#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace model {

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
    inline data_value(const std::string &data) : _data(data), _none(false) {}

    /*
     * data_value::data_value --
     *     Create a new instance.
     */
    inline data_value(const std::string &&data) : _data(std::move(data)), _none(false) {}

    /*
     * data_value::tombstone --
     *     Create an instance of a "None" value.
     */
    inline static data_value
    create_none()
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
    as_string() const
    {
        return (_data);
    }

    /*
     * data_value::operator== --
     *     Compare to another data value.
     */
    inline bool
    operator==(const data_value &other) const
    {
        if (_none && other._none)
            return (true);
        if (_none != other._none)
            return (false);
        return (_data == other._data);
    }

    /*
     * data_value::operator!= --
     *     Compare to another data value.
     */
    inline bool
    operator!=(const data_value &other) const
    {
        return !(*this == other);
    }

    /*
     * data_value::operator< --
     *     Compare to another data value.
     */
    inline bool
    operator<(const data_value &other) const
    {
        if (_none != other._none)
            return (_none);
        return (_data < other._data);
    }

    /*
     * data_value::operator<= --
     *     Compare to another data value.
     */
    inline bool
    operator<=(const data_value &other) const
    {
        if (_none != other._none)
            return (_none);
        if (_none && other._none)
            return (true);
        return (_data <= other._data);
    }

    /*
     * data_value::operator> --
     *     Compare to another data value.
     */
    inline bool
    operator>(const data_value &other) const
    {
        return !(*this <= other);
    }

    /*
     * data_value::operator> --
     *     Compare to another data value.
     */
    inline bool
    operator>=(const data_value &other) const
    {
        return !(*this < other);
    }

    /*
     * data_value::tombstone --
     *     Check if this is a None value.
     */
    inline bool
    none() const
    {
        return (_none);
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
        operator()(const kv_update &left, const kv_update &right) const
        {
            return (left._timestamp < right._timestamp);
        }

        /*
         * kv_update::timestamp_comparator::operator() --
         *     Compare the update to the given timestamp.
         */
        bool
        operator()(const kv_update &left, uint64_t timestamp) const
        {
            return (left._timestamp < timestamp);
        }

        /*
         * kv_update::timestamp_comparator::operator() --
         *     Compare the update to the given timestamp.
         */
        bool
        operator()(uint64_t timestamp, const kv_update &right) const
        {
            return (timestamp < right._timestamp);
        }
    };

    /*
     * kv_update::kv_update --
     *     Create a new instance.
     */
    inline kv_update(const data_value &value, uint64_t timestamp)
        : _value(value), _timestamp(timestamp)
    {
    }

    /*
     * kv_update::operator== --
     *     Compare to another instance.
     */
    inline bool
    operator==(const kv_update &other) const
    {
        return (_value == other._value && _timestamp == other._timestamp);
    }

    /*
     * kv_update::operator!= --
     *     Compare to another instance.
     */
    inline bool
    operator!=(const kv_update &other) const
    {
        return !(*this == other);
    }

    /*
     * kv_update::operator< --
     *     Compare to another instance.
     */
    inline bool
    operator<(const kv_update &other) const
    {
        if (_timestamp != other._timestamp)
            return (_timestamp < other._timestamp);
        if (_value != other._value)
            return (_value < other._value);
        return (true);
    }

    /*
     * kv_update::operator<= --
     *     Compare to another instance.
     */
    inline bool
    operator<=(const kv_update &other) const
    {
        return (*this < other || *this == other);
    }

    /*
     * kv_update::operator> --
     *     Compare to another instance.
     */
    inline bool
    operator>(const kv_update &other) const
    {
        return !(*this <= other);
    }

    /*
     * kv_update::operator>= --
     *     Compare to another instance.
     */
    inline bool
    operator>=(const kv_update &other) const
    {
        return !(*this < other);
    }

    /*
     * kv_update::value --
     *     Get the value.
     */
    inline const data_value &
    value() const
    {
        return (_value);
    }

    /*
     * kv_update::global --
     *     Check if this is a globally-visible, non-timestamped update.
     */
    inline bool
    global() const
    {
        return (_timestamp == 0);
    }

    /*
     * kv_update::value --
     *     Get the value.
     */
    inline uint64_t
    timestamp() const
    {
        return (_timestamp);
    }

private:
    uint64_t _timestamp;
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
    inline kv_item() {}

    /*
     * kv_item::add_update --
     *     Add an update.
     */
    int add_update(kv_update &&update, bool must_exist, bool overwrite);

    /*
     * kv_item::get --
     *     Get the corresponding value.
     */
    const data_value &get(uint64_t timestamp = UINT64_MAX);

private:
    std::mutex _lock;
    std::vector<kv_update> _updates; /* sorted list of updates */
};

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
     *     Get the name of the table.
     */
    inline const char *
    name() const
    {
        return (_name.c_str());
    }

    /*
     * kv_table::get --
     *     Get the value.
     */
    const data_value &get(const data_value &key, uint64_t timestamp = UINT64_MAX);

    /*
     * kv_table::insert --
     *     Insert into the table.
     */
    int insert(const data_value &key, const data_value &value, uint64_t timestamp = 0,
      bool overwrite = true);

    /*
     * kv_table::remove --
     *     Delete a value from the table.
     */
    int remove(const data_value &key, uint64_t timestamp = 0);

    /*
     * kv_table::update --
     *     Update a key in the table.
     */
    int update(const data_value &key, const data_value &value, uint64_t timestamp = 0,
      bool overwrite = true);

protected:
    /*
     * kv_table::item --
     *     Get the item that corresponds to the given key, creating one if need be.
     */
    inline kv_item &
    item(const data_value &key)
    {
        std::lock_guard lock_guard(_lock);
        return (_data[key]); /* this automatically instantiates the item if it does not exist */
    }

    /*
     * kv_table::item_if_exists --
     *     Get the item that corresponds to the given key, if it exists.
     */
    inline kv_item *
    item_if_exists(const data_value &key)
    {
        std::lock_guard lock_guard(_lock);
        auto i = _data.find(key);
        if (i == _data.end())
            return (NULL);
        return (&i->second);
    }

private:
    std::map<data_value, kv_item> _data;
    std::mutex _lock;
    std::string _name;
};

} /* namespace model */
#endif
