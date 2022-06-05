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

#include "connection_manager.h"

#include "src/common/logger.h"

extern "C" {
#include "test_util.h"
}

namespace test_harness {

ConnectionManager::~ConnectionManager()
{
    Close();
}

ConnectionManager &
ConnectionManager::GetInstance()
{
    static ConnectionManager _instance;
    return (_instance);
}

void
ConnectionManager::Close()
{
    if (_connection != nullptr) {
        testutil_check(_connection->close(_connection, nullptr));
        _connection = nullptr;
    }
}

void
ConnectionManager::Create(const std::string &config, const std::string &home)
{
    if (_connection != nullptr) {
        Logger::LogMessage(LOG_ERROR, "Connection is not NULL, cannot be re-opened.");
        testutil_die(EINVAL, "Connection is not NULL");
    }
    Logger::LogMessage(LOG_INFO, "wiredtiger_open config: " + config);

    /* Create the working dir. */
    testutil_make_work_dir(home.c_str());

    /* Open connection. */
    testutil_check(wiredtiger_open(home.c_str(), nullptr, config.c_str(), &_connection));
}

ScopedSession
ConnectionManager::CreateSession()
{
    if (_connection == nullptr) {
        Logger::LogMessage(LOG_ERROR,
          "Connection is NULL, did you forget to call "
          "ConnectionManager::create ?");
        testutil_die(EINVAL, "Connection is NULL");
    }

    std::lock_guard<std::mutex> lg(_mutex);
    ScopedSession session(_connection);

    return (session);
}

WT_CONNECTION *
ConnectionManager::GetConnection()
{
    return (_connection);
}

/* SetTimestamp calls into the connection API in a thread safe manner to set global timestamps. */
void
ConnectionManager::SetTimestamp(const std::string &config)
{
    std::lock_guard<std::mutex> lg(_mutex);
    testutil_check(_connection->set_timestamp(_connection, config.c_str()));
}

ConnectionManager::ConnectionManager() {}
} // namespace test_harness
