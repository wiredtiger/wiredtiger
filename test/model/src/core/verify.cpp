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
#include <iostream>

#include "model/table.h"
#include "model/verify.h"
#include "wiredtiger.h"

namespace model {

/*
 * kv_table_verify_cursor::has_next --
 *     Determine whether the cursor has a next value.
 */
bool
kv_table_verify_cursor::has_next()
{
    auto i = _iterator;
    if (_count > 0)
        i++;

    /* Skip over any deleted items. */
    while (i != _data.end() && i->second.get() == NONE)
        i++;

    return (i != _data.end());
}

/*
 * kv_table_verify_cursor::verify_next --
 *     Verify the next key-value pair. This method is not thread-safe.
 */
bool
kv_table_verify_cursor::verify_next(const data_value &key, const data_value &value)
{
    /* If we have reached the end of the model's state, we failed. */
    if (_iterator == _data.end())
        return (false);

    /* Advance the cursor, if this is not the first instance - otherwise are already positioned. */
    if (_count > 0)
        _iterator++;

    /* Skip over any deleted items. */
    while (_iterator != _data.end() && _iterator->second.get() == NONE)
        _iterator++;
    if (_iterator == _data.end())
        return (false);

    _count++;

    /* Check the key. */
    if (key != _iterator->first)
        return (false);

    /* Check the value. */
    return (_iterator->second.contains_any(value));
}

/*
 * kv_table_verifier::verify --
 *     Verify the table by comparing a WiredTiger table against the model.
 */
bool
kv_table_verifier::verify(WT_CONNECTION *connection)
{
    WT_SESSION *session = NULL;
    WT_CURSOR *wt_cursor = NULL;
    int ret;
    bool success = true;
    const char *key, *value;

    if (_verbose)
        std::cout << "Verification: Verify " << _table.name() << std::endl;

    /* Get the model cursor. */
    kv_table_verify_cursor model_cursor = _table.verify_cursor();

    try {
        /* Get the database cursor. */
        ret = connection->open_session(connection, NULL, NULL, &session);
        if (ret != 0)
            throw(ret);

        std::string uri = std::string("table:") + _table.name();
        ret = session->open_cursor(session, uri.c_str(), NULL, NULL, &wt_cursor);
        if (ret != 0)
            throw(ret);

        /* Verify each key-value pair. */
        while ((ret = wt_cursor->next(wt_cursor)) == 0) {
            ret = wt_cursor->get_key(wt_cursor, &key);
            if (ret != 0)
                throw(ret);
            ret = wt_cursor->get_value(wt_cursor, &value);
            if (ret != 0)
                throw(ret);
            if (_verbose)
                std::cout << "Verification: key = " << key << ", value = " << value << std::endl;
            if (!model_cursor.verify_next(data_value(key), data_value(value)))
                throw(false);
        }

        /* Make sure that we reached the end at the same time. */
        if (_verbose)
            std::cout << "Verification: Reached the end." << std::endl;
        if (ret != WT_NOTFOUND)
            throw(ret);
        if (model_cursor.has_next())
            throw(false);
        if (_verbose)
            std::cout << "Verification: Finished." << std::endl;

    } catch (int error) {
        success = false;
        if (_verbose)
            std::cerr << "Verification: Failed with WiredTiger error " << error << std::endl;
    } catch (...) {
        success = false;
        if (_verbose)
            std::cerr << "Verification: Failed." << std::endl;
    }

    /* Clean up. */
    if (wt_cursor != NULL)
        (void)wt_cursor->close(wt_cursor);

    if (session != NULL)
        (void)session->close(session, NULL);

    return (success);
}

} /* namespace model */
