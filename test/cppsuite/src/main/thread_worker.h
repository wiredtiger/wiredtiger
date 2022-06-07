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

#ifndef THREAD_WORKER_H
#define THREAD_WORKER_H

#include <string>

#include "database.h"
#include "src/component/operation_tracker.h"
#include "src/component/timestamp_manager.h"
#include "src/main/configuration.h"
#include "src/storage/scoped_cursor.h"
#include "src/storage/scoped_session.h"
#include "transaction.h"

namespace test_harness {
enum class ThreadType { kCheckpoint, kCustom, kInsert, kRead, kRemove, kUpdate };

const std::string ThreadTypeToString(ThreadType type);

/* Container class for a thread and any data types it may need to interact with the database. */
class ThreadWorker {
    public:
    ThreadWorker(uint64_t id, ThreadType type, Configuration *config,
      ScopedSession &&createdSession, TimestampManager *timestampManager,
      OperationTracker *operationTracker, Database &database);

    virtual ~ThreadWorker() = default;

    void Finish();

    /* If the value's size is less than the given size, padding of '0' is added to the value. */
    std::string PadString(const std::string &value, uint64_t size);

    /*
     * Generic update function, takes a collectionId, key and value.
     *
     * Return true if the operation was successful, a return value of false implies the transaction
     * needs to be rolled back.
     */
    bool Update(ScopedCursor &cursor, uint64_t collectionId, const std::string &key,
      const std::string &value);

    /*
     * Generic insert function, takes a collectionId, key and value.
     *
     * Return true if the operation was successful, a return value of false implies the transaction
     * needs to be rolled back.
     */
    bool Insert(ScopedCursor &cursor, uint64_t collectionId, const std::string &key,
      const std::string &value);

    /*
     * Generic remove function, takes a collectionId and key and will delete the key if it exists.
     *
     * Return true if the operation was successful, a return value of false implies the transaction
     * needs to be rolled back.
     */
    bool Remove(ScopedCursor &cursor, uint64_t collectionId, const std::string &key);
    void Sleep();
    bool Running() const;

    public:
    const int64_t collectionCount;
    const int64_t keyCount;
    const int64_t keySize;
    const int64_t valueSize;
    const int64_t threadCount;
    const ThreadType type;
    const uint64_t id;
    Database &database;
    ScopedSession session;
    ScopedCursor operationTrackingCursor;
    ScopedCursor statisticsCursor;
    TimestampManager *timestampManager;
    Transaction transaction;
    OperationTracker *operationTracker;

    private:
    bool _running = true;
    uint64_t _sleepTimeMs = 1000;
};
} // namespace test_harness

#endif
