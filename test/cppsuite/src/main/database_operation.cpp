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
PopulateWorker(thread_worker *threadWorker)
{
    uint64_t collectionsPerThread = threadWorker->collection_count / threadWorker->thread_count;

    for (int64_t i = 0; i < collectionsPerThread; ++i) {
        Collection &coll =
          threadWorker->db.GetCollection((threadWorker->id * collectionsPerThread) + i);
        /*
         * WiredTiger lets you open a cursor on a collection using the same pointer. When a session
         * is closed, WiredTiger APIs close the cursors too.
         */
        scoped_cursor cursor = threadWorker->session.open_scoped_cursor(coll.name);
        uint64_t j = 0;
        while (j < threadWorker->key_count) {
            threadWorker->txn.begin();
            auto key = threadWorker->pad_string(std::to_string(j), threadWorker->key_size);
            auto value =
              RandomGenerator::GetInstance().GeneratePseudoRandomString(threadWorker->value_size);
            if (threadWorker->insert(cursor, coll.id, key, value)) {
                if (threadWorker->txn.commit()) {
                    ++j;
                }
            } else {
                threadWorker->txn.rollback();
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
    int64_t collection_count = config->GetInt(collectionCount);
    int64_t key_count_per_collection = config->GetInt(keyCountPerCollection);
    int64_t value_size = config->GetInt(valueSize);
    int64_t thread_count = config->GetInt(threadCount);
    testutil_assert(thread_count == 0 || collection_count % thread_count == 0);
    testutil_assert(value_size > 0);
    int64_t key_size = config->GetInt(keySize);
    testutil_assert(key_size > 0);
    /* Keys must be unique. */
    testutil_assert(key_count_per_collection <= pow(10, key_size));

    Logger::LogMessage(
      LOG_INFO, "Populate: creating " + std::to_string(collection_count) + " collections.");

    /* Create n collections as per the configuration. */
    for (int64_t i = 0; i < collection_count; ++i)
        /*
         * The database model will call into the API and create the collection, with its own
         * session.
         */
        database.AddCollection(key_count_per_collection);

    Logger::LogMessage(
      LOG_INFO, "Populate: " + std::to_string(collection_count) + " collections created.");

    /*
     * Spawn thread_count threads to populate the database, theoretically we should be IO bound
     * here.
     */
    ThreadManager threadManager;
    std::vector<thread_worker *> workers;
    for (int64_t i = 0; i < thread_count; ++i) {
        thread_worker *tc = new thread_worker(i, thread_type::INSERT, config,
          connection_manager::instance().create_session(), timestampManager, operationTracker,
          database);
        workers.push_back(tc);
        threadManager.addThread(PopulateWorker, tc);
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
DatabaseOperation::CheckpointOperation(thread_worker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");

    while (threadWorker->running()) {
        threadWorker->sleep();
        testutil_check(threadWorker->session->checkpoint(threadWorker->session.get(), nullptr));
    }
}

void
DatabaseOperation::CustomOperation(thread_worker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");
}

void
DatabaseOperation::InsertOperation(thread_worker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");

    /* Helper struct which stores a pointer to a collection and a cursor associated with it. */
    struct collection_cursor {
        collection_cursor(Collection &coll, scoped_cursor &&cursor)
            : coll(coll), cursor(std::move(cursor))
        {
        }
        Collection &coll;
        scoped_cursor cursor;
    };

    /* Collection cursor vector. */
    std::vector<collection_cursor> ccv;
    uint64_t collectionCount = threadWorker->db.GetCollectionCount();
    testutil_assert(collectionCount != 0);
    uint64_t collectionsPerThread = collectionCount / threadWorker->thread_count;
    /* Must have unique collections for each thread. */
    testutil_assert(collectionCount % threadWorker->thread_count == 0);
    for (int i = threadWorker->id * collectionsPerThread;
         i < (threadWorker->id * collectionsPerThread) + collectionsPerThread &&
         threadWorker->running();
         ++i) {
        Collection &coll = threadWorker->db.GetCollection(i);
        scoped_cursor cursor = threadWorker->session.open_scoped_cursor(coll.name);
        ccv.push_back({coll, std::move(cursor)});
    }

    uint64_t counter = 0;
    while (threadWorker->running()) {
        uint64_t startKey = ccv[counter].coll.GetKeyCount();
        uint64_t addedCount = 0;
        threadWorker->txn.begin();

        /* Collection cursor. */
        auto &cc = ccv[counter];
        while (threadWorker->txn.active() && threadWorker->running()) {
            /* Insert a key value pair, rolling back the transaction if required. */
            auto key = threadWorker->pad_string(
              std::to_string(startKey + addedCount), threadWorker->key_size);
            auto value =
              RandomGenerator::GetInstance().GeneratePseudoRandomString(threadWorker->value_size);
            if (!threadWorker->insert(cc.cursor, cc.coll.id, key, value)) {
                addedCount = 0;
                threadWorker->txn.rollback();
            } else {
                addedCount++;
                if (threadWorker->txn.can_commit()) {
                    if (threadWorker->txn.commit()) {
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
            threadWorker->sleep();
        }
        /* Reset our cursor to avoid pinning content. */
        testutil_check(cc.cursor->reset(cc.cursor.get()));
        counter++;
        if (counter == collectionsPerThread)
            counter = 0;
        testutil_assert(counter < collectionsPerThread);
    }
    /* Make sure the last transaction is rolled back now the work is finished. */
    if (threadWorker->txn.active())
        threadWorker->txn.rollback();
}

void
DatabaseOperation::ReadOperation(thread_worker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");

    std::map<uint64_t, scoped_cursor> cursors;
    while (threadWorker->running()) {
        /* Get a collection and find a cached cursor. */
        Collection &coll = threadWorker->db.GetRandomCollection();

        if (cursors.find(coll.id) == cursors.end())
            cursors.emplace(
              coll.id, std::move(threadWorker->session.open_scoped_cursor(coll.name)));

        /* Do a second lookup now that we know it exists. */
        auto &cursor = cursors[coll.id];

        threadWorker->txn.begin();
        while (threadWorker->txn.active() && threadWorker->running()) {
            auto ret = cursor->next(cursor.get());
            if (ret != 0) {
                if (ret == WT_NOTFOUND) {
                    cursor->reset(cursor.get());
                } else if (ret == WT_ROLLBACK) {
                    threadWorker->txn.rollback();
                    threadWorker->sleep();
                    continue;
                } else
                    testutil_die(ret, "Unexpected error returned from cursor->next()");
            }
            threadWorker->txn.add_op();
            threadWorker->txn.try_rollback();
            threadWorker->sleep();
        }
        /* Reset our cursor to avoid pinning content. */
        testutil_check(cursor->reset(cursor.get()));
    }
    /* Make sure the last transaction is rolled back now the work is finished. */
    if (threadWorker->txn.active())
        threadWorker->txn.rollback();
}

void
DatabaseOperation::RemoveOperation(thread_worker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");

    /*
     * We need two types of cursors. One cursor is a random cursor to randomly select a key and the
     * other one is a standard cursor to remove the random key. This is required as the random
     * cursor does not support the remove operation.
     */
    std::map<uint64_t, scoped_cursor> randomCursors, cursors;

    /* Loop while the test is running. */
    while (threadWorker->running()) {
        /*
         * Sleep the period defined by the op_rate in the configuration. Do this at the start of the
         * loop as it could be skipped by a subsequent continue call.
         */
        threadWorker->sleep();

        /* Choose a random collection to update. */
        Collection &coll = threadWorker->db.GetRandomCollection();

        /* Look for existing cursors in our cursor cache. */
        if (cursors.find(coll.id) == cursors.end()) {
            Logger::LogMessage(LOG_TRACE,
              "Thread {" + std::to_string(threadWorker->id) +
                "} Creating cursor for collection: " + coll.name);
            /* Open the two cursors for the chosen collection. */
            scoped_cursor randomCursor =
              threadWorker->session.open_scoped_cursor(coll.name, "next_random=true");
            randomCursors.emplace(coll.id, std::move(randomCursor));
            scoped_cursor cursor = threadWorker->session.open_scoped_cursor(coll.name);
            cursors.emplace(coll.id, std::move(cursor));
        }

        /* Start a transaction if possible. */
        threadWorker->txn.try_begin();

        /* Get the cursor associated with the collection. */
        scoped_cursor &randomCursor = randomCursors[coll.id];
        scoped_cursor &cursor = cursors[coll.id];

        /* Choose a random key to delete. */
        const char *key_str;
        int ret = randomCursor->next(randomCursor.get());
        /* It is possible not to find anything if the collection is empty. */
        testutil_assert(ret == 0 || ret == WT_NOTFOUND);
        if (ret == WT_NOTFOUND) {
            /*
             * If we cannot find any record, finish the current transaction as we might be able to
             * see new records after starting a new one.
             */
            WT_IGNORE_RET_BOOL(threadWorker->txn.commit());
            continue;
        }
        testutil_check(randomCursor->get_key(randomCursor.get(), &key_str));
        if (!threadWorker->remove(cursor, coll.id, key_str)) {
            threadWorker->txn.rollback();
        }

        /* Reset our cursor to avoid pinning content. */
        testutil_check(cursor->reset(cursor.get()));

        /* Commit the current transaction if we're able to. */
        if (threadWorker->txn.can_commit())
            WT_IGNORE_RET_BOOL(threadWorker->txn.commit());
    }

    /* Make sure the last operation is rolled back now the work is finished. */
    if (threadWorker->txn.active())
        threadWorker->txn.rollback();
}

void
DatabaseOperation::UpdateOperation(thread_worker *threadWorker)
{
    Logger::LogMessage(LOG_INFO,
      type_string(threadWorker->type) + " thread {" + std::to_string(threadWorker->id) +
        "} commencing.");
    /* Cursor map. */
    std::map<uint64_t, scoped_cursor> cursors;

    /*
     * Loop while the test is running.
     */
    while (threadWorker->running()) {
        /*
         * Sleep the period defined by the op_rate in the configuration. Do this at the start of the
         * loop as it could be skipped by a subsequent continue call.
         */
        threadWorker->sleep();

        /* Choose a random collection to update. */
        Collection &coll = threadWorker->db.GetRandomCollection();

        /* Look for existing cursors in our cursor cache. */
        if (cursors.find(coll.id) == cursors.end()) {
            Logger::LogMessage(LOG_TRACE,
              "Thread {" + std::to_string(threadWorker->id) +
                "} Creating cursor for collection: " + coll.name);
            /* Open a cursor for the chosen collection. */
            scoped_cursor cursor = threadWorker->session.open_scoped_cursor(coll.name);
            cursors.emplace(coll.id, std::move(cursor));
        }

        /* Start a transaction if possible. */
        threadWorker->txn.try_begin();

        /* Get the cursor associated with the collection. */
        scoped_cursor &cursor = cursors[coll.id];

        /* Choose a random key to update. */
        testutil_assert(coll.GetKeyCount() != 0);
        auto keyId =
          RandomGenerator::GetInstance().GenerateInteger<uint64_t>(0, coll.GetKeyCount() - 1);
        auto key = threadWorker->pad_string(std::to_string(keyId), threadWorker->key_size);
        auto value =
          RandomGenerator::GetInstance().GeneratePseudoRandomString(threadWorker->value_size);
        if (!threadWorker->update(cursor, coll.id, key, value)) {
            threadWorker->txn.rollback();
        }

        /* Reset our cursor to avoid pinning content. */
        testutil_check(cursor->reset(cursor.get()));

        /* Commit the current transaction if we're able to. */
        if (threadWorker->txn.can_commit())
            WT_IGNORE_RET_BOOL(threadWorker->txn.commit());
    }

    /* Make sure the last operation is rolled back now the work is finished. */
    if (threadWorker->txn.active())
        threadWorker->txn.rollback();
}

void
DatabaseOperation::Validate(const std::string &operationTableName,
  const std::string &schemaTable_name, const std::vector<uint64_t> &knownCollectionIds)
{
    validator wv;
    wv.Validate(operationTableName, schemaTable_name, knownCollectionIds);
}
} // namespace test_harness
