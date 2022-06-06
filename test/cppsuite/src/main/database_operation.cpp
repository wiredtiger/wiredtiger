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

#include "database_operation.h"

#include "src/common/thread_manager.h"
#include "src/common/constants.h"
#include "src/common/logger.h"
#include "src/common/random_generator.h"
#include "src/main/validator.h"
#include "src/storage/connection_manager.h"

namespace test_harness {
/* Static methods. */
static void
PopulateWorker(ThreadWorker *threadWorker)
{
    uint64_t collectionsPerThread = threadWorker->collectionCount / threadWorker->threadCount;

    for (int64_t i = 0; i < collectionsPerThread; ++i) {
        Collection &coll =
          threadWorker->database.GetCollection((threadWorker->id * collectionsPerThread) + i);
        /*
         * WiredTiger lets you open a cursor on a collection using the same pointer. When a session
         * is closed, WiredTiger APIs close the cursors too.
         */
        ScopedCursor cursor = threadWorker->session.OpenScopedCursor(coll.name);
        uint64_t j = 0;
        while (j < threadWorker->keyCount) {
            threadWorker->transaction.Start();
            auto key = threadWorker->PadString(std::to_string(j), threadWorker->keySize);
            auto value =
              RandomGenerator::GetInstance().GeneratePseudoRandomString(threadWorker->valueSize);
            if (threadWorker->Insert(cursor, coll.id, key, value)) {
                if (threadWorker->transaction.Commit()) {
                    ++j;
                }
            } else {
                threadWorker->transaction.Rollback();
            }
        }
    }
    Logger::LogMessage(
      LOG_TRACE, "Populate: thread {" + std::to_string(threadWorker->id) + "} finished");
}

void
DatabaseOperation::Populate(Database &database, TimestampManager *timestampManager,
  Configuration *config, OperationTracker *operationTracker)
{
    /* Validate our config. */
    int64_t collectionCount = config->GetInt(kCollectionCount);
    int64_t keyCountPerCollection = config->GetInt(kKeyCountPerCollection);
    int64_t valueSize = config->GetInt(kValueSize);
    int64_t threadCount = config->GetInt(kThreadCount);
    testutil_assert(threadCount == 0 || collectionCount % threadCount == 0);
    testutil_assert(valueSize > 0);
    int64_t keySize = config->GetInt(kKeySize);
    testutil_assert(keySize > 0);
    /* Keys must be unique. */
    testutil_assert(keyCountPerCollection <= pow(10, keySize));

    Logger::LogMessage(
      LOG_INFO, "Populate: creating " + std::to_string(collectionCount) + " collections.");

    /* Create n collections as per the configuration. */
    for (int64_t i = 0; i < collectionCount; ++i)
        /*
         * The database model will call into the API and create the collection, with its own
         * session.
         */
        database.AddCollection(keyCountPerCollection);

    Logger::LogMessage(
      LOG_INFO, "Populate: " + std::to_string(collectionCount) + " collections created.");

    /*
     * Spawn threadCount threads to populate the database, theoretically we should be IO bound here.
     */
    ThreadManager threadManager;
    std::vector<ThreadWorker *> workers;
    for (int64_t i = 0; i < threadCount; ++i) {
        ThreadWorker *threadWorker = new ThreadWorker(i, ThreadType::kInsert, config,
          ConnectionManager::GetInstance().CreateSession(), timestampManager, operationTracker,
          database);
        workers.push_back(threadWorker);
        threadManager.addThread(PopulateWorker, threadWorker);
    }

    /* Wait for our populate threads to finish and then join them. */
    Logger::LogMessage(LOG_INFO, "Populate: waiting for threads to complete.");
    threadManager.Join();

    /* Cleanup our workers. */
    for (auto &it : workers) {
        delete it;
        it = nullptr;
    }
    Logger::LogMessage(LOG_INFO, "Populate: finished.");
}

void
DatabaseOperation::CheckpointOperation(ThreadWorker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      ThreadTypeToString(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");

    while (threadWorker->Running()) {
        threadWorker->Sleep();
        testutil_check(threadWorker->session->checkpoint(threadWorker->session.Get(), nullptr));
    }
}

void
DatabaseOperation::CustomOperation(ThreadWorker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      ThreadTypeToString(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");
}

void
DatabaseOperation::InsertOperation(ThreadWorker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      ThreadTypeToString(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");

    /* Helper struct which stores a pointer to a collection and a cursor associated with it. */
    struct collection_cursor {
        collection_cursor(Collection &coll, ScopedCursor &&cursor)
            : coll(coll), cursor(std::move(cursor))
        {
        }
        Collection &coll;
        ScopedCursor cursor;
    };

    /* Collection cursor vector. */
    std::vector<collection_cursor> ccv;
    uint64_t collectionCount = threadWorker->database.GetCollectionCount();
    testutil_assert(collectionCount != 0);
    uint64_t collectionsPerThread = collectionCount / threadWorker->threadCount;
    /* Must have unique collections for each thread. */
    testutil_assert(collectionCount % threadWorker->threadCount == 0);
    for (int i = threadWorker->id * collectionsPerThread;
         i < (threadWorker->id * collectionsPerThread) + collectionsPerThread &&
         threadWorker->Running();
         ++i) {
        Collection &coll = threadWorker->database.GetCollection(i);
        ScopedCursor cursor = threadWorker->session.OpenScopedCursor(coll.name);
        ccv.push_back({coll, std::move(cursor)});
    }

    uint64_t counter = 0;
    while (threadWorker->Running()) {
        uint64_t startKey = ccv[counter].coll.GetKeyCount();
        uint64_t addedCount = 0;
        threadWorker->transaction.Start();

        /* Collection cursor. */
        auto &cc = ccv[counter];
        while (threadWorker->transaction.Active() && threadWorker->Running()) {
            /* Insert a key value pair, rolling back the transaction if required. */
            auto key =
              threadWorker->PadString(std::to_string(startKey + addedCount), threadWorker->keySize);
            auto value =
              RandomGenerator::GetInstance().GeneratePseudoRandomString(threadWorker->valueSize);
            if (!threadWorker->Insert(cc.cursor, cc.coll.id, key, value)) {
                addedCount = 0;
                threadWorker->transaction.Rollback();
            } else {
                addedCount++;
                if (threadWorker->transaction.CanCommit()) {
                    if (threadWorker->transaction.Commit()) {
                        /*
                         * We need to inform the database model that we've added these keys as some
                         * other thread may rely on the keyCount data. Only do so if we successfully
                         * committed.
                         */
                        cc.coll.IncreaseKeyCount(addedCount);
                    } else {
                        addedCount = 0;
                    }
                }
            }

            /* Sleep the duration defined by the op_rate. */
            threadWorker->Sleep();
        }
        /* Reset our cursor to avoid pinning content. */
        testutil_check(cc.cursor->reset(cc.cursor.Get()));
        counter++;
        if (counter == collectionsPerThread)
            counter = 0;
        testutil_assert(counter < collectionsPerThread);
    }
    /* Make sure the last transaction is rolled back now the work is finished. */
    if (threadWorker->transaction.Active())
        threadWorker->transaction.Rollback();
}

void
DatabaseOperation::ReadOperation(ThreadWorker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      ThreadTypeToString(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");

    std::map<uint64_t, ScopedCursor> cursors;
    while (threadWorker->Running()) {
        /* Get a collection and find a cached cursor. */
        Collection &coll = threadWorker->database.GetRandomCollection();

        if (cursors.find(coll.id) == cursors.end())
            cursors.emplace(coll.id, std::move(threadWorker->session.OpenScopedCursor(coll.name)));

        /* Do a second lookup now that we know it exists. */
        auto &cursor = cursors[coll.id];

        threadWorker->transaction.Start();
        while (threadWorker->transaction.Active() && threadWorker->Running()) {
            auto ret = cursor->next(cursor.Get());
            if (ret != 0) {
                if (ret == WT_NOTFOUND) {
                    cursor->reset(cursor.Get());
                } else if (ret == WT_ROLLBACK) {
                    threadWorker->transaction.Rollback();
                    threadWorker->Sleep();
                    continue;
                } else
                    testutil_die(ret, "Unexpected error returned from cursor->next()");
            }
            threadWorker->transaction.IncrementOp();
            threadWorker->transaction.TryRollback();
            threadWorker->Sleep();
        }
        /* Reset our cursor to avoid pinning content. */
        testutil_check(cursor->reset(cursor.Get()));
    }
    /* Make sure the last transaction is rolled back now the work is finished. */
    if (threadWorker->transaction.Active())
        threadWorker->transaction.Rollback();
}

void
DatabaseOperation::RemoveOperation(ThreadWorker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      ThreadTypeToString(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");

    /*
     * We need two types of cursors. One cursor is a random cursor to randomly select a key and the
     * other one is a standard cursor to remove the random key. This is required as the random
     * cursor does not support the remove operation.
     */
    std::map<uint64_t, ScopedCursor> randomCursors, cursors;

    /* Loop while the test is running. */
    while (threadWorker->Running()) {
        /*
         * Sleep the period defined by the op_rate in the configuration. Do this at the start of the
         * loop as it could be skipped by a subsequent continue call.
         */
        threadWorker->Sleep();

        /* Choose a random collection to update. */
        Collection &coll = threadWorker->database.GetRandomCollection();

        /* Look for existing cursors in our cursor cache. */
        if (cursors.find(coll.id) == cursors.end()) {
            Logger::LogMessage(LOG_TRACE,
              "Thread {" + std::to_string(threadWorker->id) +
                "} Creating cursor for collection: " + coll.name);
            /* Open the two cursors for the chosen collection. */
            ScopedCursor randomCursor =
              threadWorker->session.OpenScopedCursor(coll.name, "next_random=true");
            randomCursors.emplace(coll.id, std::move(randomCursor));
            ScopedCursor cursor = threadWorker->session.OpenScopedCursor(coll.name);
            cursors.emplace(coll.id, std::move(cursor));
        }

        /* Start a transaction if possible. */
        threadWorker->transaction.TryStart();

        /* Get the cursor associated with the collection. */
        ScopedCursor &randomCursor = randomCursors[coll.id];
        ScopedCursor &cursor = cursors[coll.id];

        /* Choose a random key to delete. */
        const char *key_str;
        int ret = randomCursor->next(randomCursor.Get());
        /* It is possible not to find anything if the collection is empty. */
        testutil_assert(ret == 0 || ret == WT_NOTFOUND);
        if (ret == WT_NOTFOUND) {
            /*
             * If we cannot find any record, finish the current transaction as we might be able to
             * see new records after starting a new one.
             */
            WT_IGNORE_RET_BOOL(threadWorker->transaction.Commit());
            continue;
        }
        testutil_check(randomCursor->get_key(randomCursor.Get(), &key_str));
        if (!threadWorker->Remove(cursor, coll.id, key_str)) {
            threadWorker->transaction.Rollback();
        }

        /* Reset our cursor to avoid pinning content. */
        testutil_check(cursor->reset(cursor.Get()));

        /* Commit the current transaction if we're able to. */
        if (threadWorker->transaction.CanCommit())
            WT_IGNORE_RET_BOOL(threadWorker->transaction.Commit());
    }

    /* Make sure the last operation is rolled back now the work is finished. */
    if (threadWorker->transaction.Active())
        threadWorker->transaction.Rollback();
}

void
DatabaseOperation::UpdateOperation(ThreadWorker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      ThreadTypeToString(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");
    /* Cursor map. */
    std::map<uint64_t, ScopedCursor> cursors;

    /*
     * Loop while the test is running.
     */
    while (threadWorker->Running()) {
        /*
         * Sleep the period defined by the op_rate in the configuration. Do this at the start of the
         * loop as it could be skipped by a subsequent continue call.
         */
        threadWorker->Sleep();

        /* Choose a random collection to update. */
        Collection &coll = threadWorker->database.GetRandomCollection();

        /* Look for existing cursors in our cursor cache. */
        if (cursors.find(coll.id) == cursors.end()) {
            Logger::LogMessage(LOG_TRACE,
              "Thread {" + std::to_string(threadWorker->id) +
                "} Creating cursor for collection: " + coll.name);
            /* Open a cursor for the chosen collection. */
            ScopedCursor cursor = threadWorker->session.OpenScopedCursor(coll.name);
            cursors.emplace(coll.id, std::move(cursor));
        }

        /* Start a transaction if possible. */
        threadWorker->transaction.TryStart();

        /* Get the cursor associated with the collection. */
        ScopedCursor &cursor = cursors[coll.id];

        /* Choose a random key to update. */
        testutil_assert(coll.GetKeyCount() != 0);
        auto keyId =
          RandomGenerator::GetInstance().GenerateInteger<uint64_t>(0, coll.GetKeyCount() - 1);
        auto key = threadWorker->PadString(std::to_string(keyId), threadWorker->keySize);
        auto value =
          RandomGenerator::GetInstance().GeneratePseudoRandomString(threadWorker->valueSize);
        if (!threadWorker->Update(cursor, coll.id, key, value)) {
            threadWorker->transaction.Rollback();
        }

        /* Reset our cursor to avoid pinning content. */
        testutil_check(cursor->reset(cursor.Get()));

        /* Commit the current transaction if we're able to. */
        if (threadWorker->transaction.CanCommit())
            WT_IGNORE_RET_BOOL(threadWorker->transaction.Commit());
    }

    /* Make sure the last operation is rolled back now the work is finished. */
    if (threadWorker->transaction.Active())
        threadWorker->transaction.Rollback();
}

void
DatabaseOperation::Validate(const std::string &operationTableName,
  const std::string &schemaTable_name, const std::vector<uint64_t> &knownCollectionIds)
{
    Validator wv;
    wv.Validate(operationTableName, schemaTable_name, knownCollectionIds);
}
} // namespace test_harness
