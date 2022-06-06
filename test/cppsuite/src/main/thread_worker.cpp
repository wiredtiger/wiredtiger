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

#include "thread_worker.h"

#include <thread>

#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/common/random_generator.h"
#include "transaction.h"

namespace test_harness {

const std::string
ThreadTypeToString(ThreadType type)
{
    switch (type) {
    case ThreadType::kCheckpoint:
        return ("checkpoint");
    case ThreadType::kCustom:
        return ("custom");
    case ThreadType::kInsert:
        return ("insert");
    case ThreadType::kRead:
        return ("read");
    case ThreadType::kRemove:
        return ("remove");
    case ThreadType::kUpdate:
        return ("update");
    default:
        testutil_die(EINVAL, "unexpected ThreadType: %d", static_cast<int>(type));
    }
}

ThreadWorker::ThreadWorker(uint64_t id, ThreadType type, Configuration *config,
  ScopedSession &&createdSession, TimestampManager *timestampManager,
  OperationTracker *operationTracker, Database &database)
    : /* These won't exist for certain threads which is why we use optional here. */
      collectionCount(config->GetOptionalInt(kCollectionCount, 1)),
      keyCount(config->GetOptionalInt(kKeyCountPerCollection, 1)),
      keySize(config->GetOptionalInt(kKeySize, 1)),
      valueSize(config->GetOptionalInt(kValueSize, 1)), threadCount(config->GetInt(kThreadCount)),
      type(type), id(id), database(database), session(std::move(createdSession)),
      timestampManager(timestampManager),
      transaction(Transaction(config, timestampManager, session.Get())),
      operationTracker(operationTracker), _sleepTimeMs(config->GetThrottleMs())
{
    if (operationTracker->IsEnabled())
        operationTrackingCursor =
          session.OpenScopedCursor(operationTracker->getOperationTableName());

    testutil_assert(keySize > 0 && valueSize > 0);
}

void
ThreadWorker::Finish()
{
    _running = false;
}

std::string
ThreadWorker::PadString(const std::string &value, uint64_t size)
{
    uint64_t diff = size > value.size() ? size - value.size() : 0;
    std::string s(diff, '0');
    return (s.append(value));
}

bool
ThreadWorker::Update(
  ScopedCursor &cursor, uint64_t collection_id, const std::string &key, const std::string &value)
{
    WT_DECL_RET;

    testutil_assert(operationTracker != nullptr);
    testutil_assert(cursor.Get() != nullptr);

    wt_timestamp_t timestamp = timestampManager->GetNextTimestamp();
    ret = transaction.SetCommitTimestamp(timestamp);
    testutil_assert(ret == 0 || ret == EINVAL);
    if (ret != 0) {
        transaction.SetRollbackRequired(true);
        return (false);
    }

    cursor->set_key(cursor.Get(), key.c_str());
    cursor->set_value(cursor.Get(), value.c_str());
    ret = cursor->update(cursor.Get());

    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            transaction.SetRollbackRequired(true);
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to update a key");
    }

    uint64_t txn_id = ((WT_SESSION_IMPL *)session.Get())->txn->id;
    ret = operationTracker->save_operation(txn_id, trackingOperation::INSERT, collection_id, key,
      value, timestamp, operationTrackingCursor);

    if (ret == 0)
        transaction.IncrementOp();
    else if (ret == WT_ROLLBACK)
        transaction.SetRollbackRequired(true);
    else
        testutil_die(ret, "unhandled error while trying to save an update to the tracking table");
    return (ret == 0);
}

bool
ThreadWorker::Insert(
  ScopedCursor &cursor, uint64_t collection_id, const std::string &key, const std::string &value)
{
    WT_DECL_RET;

    testutil_assert(operationTracker != nullptr);
    testutil_assert(cursor.Get() != nullptr);

    wt_timestamp_t timestamp = timestampManager->GetNextTimestamp();
    ret = transaction.SetCommitTimestamp(timestamp);
    testutil_assert(ret == 0 || ret == EINVAL);
    if (ret != 0) {
        transaction.SetRollbackRequired(true);
        return (false);
    }

    cursor->set_key(cursor.Get(), key.c_str());
    cursor->set_value(cursor.Get(), value.c_str());
    ret = cursor->insert(cursor.Get());

    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            transaction.SetRollbackRequired(true);
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to insert a key");
    }

    uint64_t txn_id = ((WT_SESSION_IMPL *)session.Get())->txn->id;
    ret = operationTracker->save_operation(txn_id, trackingOperation::INSERT, collection_id, key,
      value, timestamp, operationTrackingCursor);

    if (ret == 0)
        transaction.IncrementOp();
    else if (ret == WT_ROLLBACK)
        transaction.SetRollbackRequired(true);
    else
        testutil_die(ret, "unhandled error while trying to save an insert to the tracking table");
    return (ret == 0);
}

bool
ThreadWorker::Remove(ScopedCursor &cursor, uint64_t collection_id, const std::string &key)
{
    WT_DECL_RET;
    testutil_assert(operationTracker != nullptr);
    testutil_assert(cursor.Get() != nullptr);

    wt_timestamp_t timestamp = timestampManager->GetNextTimestamp();
    ret = transaction.SetCommitTimestamp(timestamp);
    testutil_assert(ret == 0 || ret == EINVAL);
    if (ret != 0) {
        transaction.SetRollbackRequired(true);
        return (false);
    }

    cursor->set_key(cursor.Get(), key.c_str());
    ret = cursor->remove(cursor.Get());
    if (ret != 0) {
        if (ret == WT_ROLLBACK) {
            transaction.SetRollbackRequired(true);
            return (false);
        } else
            testutil_die(ret, "unhandled error while trying to remove a key");
    }

    uint64_t txn_id = ((WT_SESSION_IMPL *)session.Get())->txn->id;
    ret = operationTracker->save_operation(txn_id, trackingOperation::DELETE_KEY, collection_id,
      key, "", timestamp, operationTrackingCursor);

    if (ret == 0)
        transaction.IncrementOp();
    else if (ret == WT_ROLLBACK)
        transaction.SetRollbackRequired(true);
    else
        testutil_die(ret, "unhandled error while trying to save a remove to the tracking table");
    return (ret == 0);
}

void
ThreadWorker::Sleep()
{
    std::this_thread::sleep_for(std::chrono::milliseconds(_sleepTimeMs));
}

bool
ThreadWorker::Running() const
{
    return (_running);
}
} // namespace test_harness
