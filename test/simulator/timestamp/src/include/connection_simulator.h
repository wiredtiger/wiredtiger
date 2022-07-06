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

#ifndef CONNECTION_SIMULATOR_H
#define CONNECTION_SIMULATOR_H

#include <vector>
#include <memory>

#include "session_simulator.h"

/* connection_simulator is a singleton class (Global access of one and only one instance). */
class connection_simulator {
    /* Member variables */
    private:
    std::vector<session_simulator*> session_list;

    /* Methods */
    public:
    static connection_simulator &get_connection();
    session_simulator* open_session();
    int query_timestamp();
    int set_timestamp();
    ~connection_simulator();

    /* No copies of the singleton allowed. */
    private:
    connection_simulator();

    public:
    /* Deleted functions should generally be public as it results in better error messages. */
    connection_simulator(connection_simulator const &) = delete;
    connection_simulator &operator=(connection_simulator const &) = delete;
};

#endif
