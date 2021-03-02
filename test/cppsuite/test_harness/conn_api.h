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

#ifndef CONN_API_H
#define CONN_API_H

/* Define common resource access functions. */
namespace test_harness {

WT_CONNECTION *_conn_conn = nullptr;
std::mutex _conn_mutex;

static void
conn_api_close()
{
    if (_conn_conn != nullptr) {
        if (_conn_conn->close(_conn_conn, NULL) != 0)
            /* Failing to close connection is not blocking. */
            debug_info(
              "Failed to close connection, shutting down uncleanly", _trace_level, DEBUG_ERROR);
        _conn_conn = nullptr;
    }
}

static void
conn_api_open()
{
    std::string home;
    home = DEFAULT_DIR;
    /* Create the working dir. */
    testutil_make_work_dir(home.c_str());

    /* Open connection. */
    testutil_check(wiredtiger_open(home.c_str(), NULL, CONNECTION_CREATE, &_conn_conn));
}

static WT_SESSION *
conn_api_get_session()
{
    WT_SESSION *session;

    if (_conn_conn == nullptr) {
        debug_info(
          "Connection is NULL, did you forget to call conn_api_open ?", _trace_level, DEBUG_ERROR);
        testutil_die(CONNECTION_NULL, "Connection is NULL");
    }

    _conn_mutex.lock();
    testutil_check(_conn_conn->open_session(_conn_conn, NULL, NULL, &session));
    _conn_mutex.unlock();

    return session;
}

} // namespace test_harness

#endif
