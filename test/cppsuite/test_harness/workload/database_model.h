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

#ifndef DATABASE_MODEL_H
#define DATABASE_MODEL_H

#include <map>
#include <string>

namespace test_harness {

/* Key/Value type. */
typedef std::string key_value_t;

/* Representation of key states. */
struct key_t {
    bool exists;
};

/* Representation of a value. */
struct value_t {
    key_value_t value;
};

/* A collection is made of mapped Key objects. */
struct collection_t {
    std::map<key_value_t, key_t> keys;
    std::map<key_value_t, value_t> *values = {nullptr};
};

/* Representation of the collections in memory. */
class database {
    public:
    const std::vector<std::string>
    get_collection_names() const
    {
        std::vector<std::string> collection_names;

        for (auto const &it : collections)
            collection_names.push_back(it.first);

        return (collection_names);
    }

    std::map<std::string, collection_t> collections;
};
} // namespace test_harness

#endif
