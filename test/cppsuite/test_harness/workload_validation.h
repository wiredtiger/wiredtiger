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

#ifndef WORKLOAD_VALIDATION_H
#define WORKLOAD_VALIDATION_H

#include <string>

extern "C" {
#include "wiredtiger.h"
}

namespace test_harness {
/*
 * Class that can validate database state and collection data.
 */
class workload_validation {
    public:
    bool
    verify_database_state(
      WT_SESSION *session, const std::string &collection_name, bool exists) const
    {
        WT_CURSOR *cursor;
        int ret = session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor);
        return (exists ? (ret == 0) : (ret != 0));
    }

    template <typename K>
    int
    is_key_present(WT_SESSION *session, const std::string &collection_name, const K &key)
    {
        WT_CURSOR *cursor;
        testutil_check(session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor));
        cursor->set_key(cursor, key);
        return (cursor->search(cursor));
    }

    template <typename K, typename V>
    bool
    verify_value(WT_SESSION *session, const std::string &collection_name, const K &key,
      const V &expected_value)
    {
        WT_CURSOR *cursor;
        const char *value;

        testutil_check(session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor));
        cursor->set_key(cursor, key);
        testutil_check(cursor->search(cursor));
        testutil_check(cursor->get_value(cursor, &value));

        return (value == expected_value);
    }

    private:
};
} // namespace test_harness

#endif
