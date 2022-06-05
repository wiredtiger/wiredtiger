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

/*
 * This file provides an example of how to create a test in C++ using a few features from the
 * framework if any. This file can be used as a template for quick testing and/or when stress
 * testing is not required. For any stress testing, it is encouraged to use the framework, see
 * test_template.cpp and create_script.sh.
 */

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/common/random_generator.h"
#include "src/common/thread_manager.h"
#include "src/storage/connection_manager.h"

extern "C" {
#include "wiredtiger.h"
#include "test_util.h"
}

using namespace test_harness;

/* Declarations to avoid the error raised by -Werror=missing-prototypes. */
void InsertOp(WT_CURSOR *cursor, int keySize, int valueSize);
void ReadOp(WT_CURSOR *cursor, int keySize);

bool doInserts = false;
bool doReads = false;

void
InsertOp(WT_CURSOR *cursor, int keySize, int valueSize)
{
    Logger::LogMessage(LOG_INFO, "called InsertOp");

    /* Insert random data. */
    std::string key, value;
    while (doInserts) {
        key = RandomGenerator::GetInstance().GenerateRandomString(keySize);
        value = RandomGenerator::GetInstance().GenerateRandomString(valueSize);
        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, value.c_str());
        testutil_check(cursor->insert(cursor));
    }
}

void
ReadOp(WT_CURSOR *cursor, int keySize)
{
    Logger::LogMessage(LOG_INFO, "called ReadOp");

    /* Read random data. */
    std::string key;
    while (doReads) {
        key = RandomGenerator::GetInstance().GenerateRandomString(keySize);
        cursor->set_key(cursor, key.c_str());
        cursor->search(cursor);
    }
}

int
main(int argc, char *argv[])
{
    /* Set the program name for error messages. */
    const std::string progname = testutil_set_progname(argv);

    /* Set the tracing level for the logger component. */
    Logger::traceLevel = LOG_INFO;

    /* Printing some messages. */
    Logger::LogMessage(LOG_INFO, "Starting " + progname);
    Logger::LogMessage(LOG_ERROR, "This could be an error.");

    /* Create a connection, set the cache size and specify the home directory. */
    const std::string connectionConfig = connectionCreate + ",cache_size=500MB";
    const std::string homeDir = std::string(DEFAULT_DIR) + '_' + progname;

    /* Create connection. */
    ConnectionManager::GetInstance().Create(connectionConfig, homeDir);
    WT_CONNECTION *connection = ConnectionManager::GetInstance().GetConnection();

    /* Open different sessions. */
    WT_SESSION *insertSession, *readSession;
    testutil_check(connection->open_session(connection, nullptr, nullptr, &insertSession));
    testutil_check(connection->open_session(connection, nullptr, nullptr, &readSession));

    /* Create a collection. */
    const std::string collectionName = "table:my_collection";
    testutil_check(
      insertSession->create(insertSession, collectionName.c_str(), defaultFrameworkSchema.c_str()));

    /* Open different cursors. */
    WT_CURSOR *insertCursor, *readCursor;
    const std::string cursor_config = "";
    testutil_check(insertSession->open_cursor(
      insertSession, collectionName.c_str(), nullptr, cursor_config.c_str(), &insertCursor));
    testutil_check(readSession->open_cursor(
      readSession, collectionName.c_str(), nullptr, cursor_config.c_str(), &readCursor));

    /* Store cursors. */
    std::vector<WT_CURSOR *> cursors;
    cursors.push_back(insertCursor);
    cursors.push_back(readCursor);

    /* Insert some data. */
    std::string key = "a";
    const std::string value = "b";
    insertCursor->set_key(insertCursor, key.c_str());
    insertCursor->set_value(insertCursor, value.c_str());
    testutil_check(insertCursor->insert(insertCursor));

    /* Read some data. */
    key = "b";
    readCursor->set_key(readCursor, key.c_str());
    testutil_assert(readCursor->search(readCursor) == WT_NOTFOUND);

    key = "a";
    readCursor->set_key(readCursor, key.c_str());
    testutil_check(readCursor->search(readCursor));

    /* Create a thread manager and spawn some threads that will work. */
    ThreadManager t;
    int keySize = 1, valueSize = 2;

    doInserts = true;
    t.addThread(InsertOp, insertCursor, keySize, valueSize);

    doReads = true;
    t.addThread(ReadOp, readCursor, keySize);

    /* Sleep for the test duration. */
    int testDurationSecs = 5;
    std::this_thread::sleep_for(std::chrono::seconds(testDurationSecs));

    /* Stop the threads. */
    doReads = false;
    doInserts = false;
    t.Join();

    /* Close cursors. */
    for (auto c : cursors)
        testutil_check(c->close(c));

    /* Another message. */
    Logger::LogMessage(LOG_INFO, "End of test.");

    return (0);
}
