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

#include "validator.h"

#include <algorithm>

#include "src/common/logger.h"
#include "src/storage/connection_manager.h"

namespace test_harness {
void
Validator::Validate(const std::string &operationTableName, const std::string &schemaTableName,
  const std::vector<uint64_t> &knownCollectionIds)
{
    WT_DECL_RET;
    wt_timestamp_t trackedTimestamps;
    uint64_t trackedCollectionId;
    const char *trackedKey, *trackedValue;
    int trackedOpType;
    uint64_t currentCollectionId = 0;

    Logger::LogMessage(LOG_INFO, "Beginning validation.");

    ScopedSession session = ConnectionManager::GetInstance().CreateSession();
    ScopedCursor cursor = session.OpenScopedCursor(operationTableName);

    /*
     * Default validation depends on specific fields being present in the tracking table. If the
     * tracking table schema has been modified the user must define their own validation.
     */
    const std::string keyFormat(cursor->key_format);
    const std::string valueFormat(cursor->value_format);
    if (keyFormat != OPERATION_TRACKING_KEY_FORMAT ||
      valueFormat != OPERATION_TRACKING_VALUE_FORMAT) {
        testutil_die(EINVAL,
          "Attempting to perform default validation on a test with a user-defined tracking "
          "table. Please define validation for your test");
    }

    /* Retrieve the collections that were created and deleted during the test. */
    std::vector<uint64_t> createdCollections, deletedCollections;
    parseSchemaTrackingTable(session, schemaTableName, createdCollections, deletedCollections);

    /*
     * Make sure the deleted collections do not exist on disk. The created collections are checked
     * in check_reference.
     */
    for (auto const &it : deletedCollections) {
        if (!VerifyCollectionFileState(session, it, false))
            testutil_die(LOG_ERROR,
              "Validation failed: collection %s present on disk while it has been tracked as "
              "deleted.",
              Database::GenerateCollectionName(it).c_str());
    }

    /*
     * All collections in memory should match those created in the schema tracking table. Dropping
     * is currently not supported.
     */
    std::sort(createdCollections.begin(), createdCollections.end());
    auto onDiskCollectionId = createdCollections.begin();
    if (createdCollections.size() != knownCollectionIds.size())
        testutil_die(LOG_ERROR,
          "Validation failed: collection state mismatch, expected %lu"
          " collections to exist but have %lu on disk",
          createdCollections.size(), knownCollectionIds.size());
    for (const auto id : knownCollectionIds) {
        if (id != *onDiskCollectionId)
            testutil_die(LOG_ERROR,
              "Validation failed: collection state mismatch expected "
              "collection id %lu but got %lu.",
              id, *onDiskCollectionId);
        onDiskCollectionId++;
    }

    /* Parse the tracking table. */
    validation_collection currentCollectionRecords;
    while ((ret = cursor->next(cursor.Get())) == 0) {
        testutil_check(
          cursor->get_key(cursor.Get(), &trackedCollectionId, &trackedKey, &trackedTimestamps));
        testutil_check(cursor->get_value(cursor.Get(), &trackedOpType, &trackedValue));

        Logger::LogMessage(LOG_TRACE,
          "Retrieved tracked values. \n Collection id: " + std::to_string(trackedCollectionId) +
            "\n Key: " + std::string(trackedKey) +
            "\n Timestamp: " + std::to_string(trackedTimestamps) + "\n Operation type: " +
            std::to_string(trackedOpType) + "\n Value: " + std::string(trackedValue));

        /*
         * Check if we've stepped over to the next collection. The tracking table is sorted by
         * collection_id so this is correct.
         */
        if (trackedCollectionId != currentCollectionId) {
            if (std::find(knownCollectionIds.begin(), knownCollectionIds.end(),
                  trackedCollectionId) == knownCollectionIds.end())
                testutil_die(LOG_ERROR,
                  "Validation failed: The collection id %lu is not part of the known "
                  "collection set.",
                  trackedCollectionId);
            if (trackedCollectionId < currentCollectionId)
                testutil_die(LOG_ERROR, "Validation failed: The collection id %lu is out of order.",
                  trackedCollectionId);

            /*
             * Given that we've stepped over to the next collection we've built a full picture of
             * the current collection and can now validate it.
             */
            VerifyCollection(session, currentCollectionId, currentCollectionRecords);

            /* Begin processing the next collection. */
            currentCollectionId = trackedCollectionId;
            currentCollectionRecords.clear();
        }

        /*
         * Add the values from the tracking table to the current collection model.
         */
        UpdateDataModel(static_cast<trackingOperation>(trackedOpType), currentCollectionRecords,
          currentCollectionId, trackedKey, trackedValue);
    };

    /* The value of ret should be WT_NOTFOUND once the cursor has read all rows. */
    if (ret != WT_NOTFOUND)
        testutil_die(
          LOG_ERROR, "Validation failed: cursor->next() return an unexpected error %d.", ret);

    /*
     * We still need to validate the last collection. But we can also end up here if there aren't
     * any collections, check for that.
     */
    if (knownCollectionIds.size() != 0)
        VerifyCollection(session, currentCollectionId, currentCollectionRecords);
}

void
Validator::parseSchemaTrackingTable(ScopedSession &session, const std::string &trackingTableName,
  std::vector<uint64_t> &createdCollections, std::vector<uint64_t> &deletedCollections)
{
    wt_timestamp_t keyTimestamp;
    uint64_t keyCollectionId;
    int valueOperationType;

    ScopedCursor cursor = session.OpenScopedCursor(trackingTableName);

    while (cursor->next(cursor.Get()) == 0) {
        testutil_check(cursor->get_key(cursor.Get(), &keyCollectionId, &keyTimestamp));
        testutil_check(cursor->get_value(cursor.Get(), &valueOperationType));

        Logger::LogMessage(LOG_TRACE, "Collection id is " + std::to_string(keyCollectionId));
        Logger::LogMessage(LOG_TRACE, "Timestamp is " + std::to_string(keyTimestamp));
        Logger::LogMessage(LOG_TRACE, "Operation type is " + std::to_string(valueOperationType));

        if (static_cast<trackingOperation>(valueOperationType) ==
          trackingOperation::CREATE_COLLECTION) {
            deletedCollections.erase(
              std::remove(deletedCollections.begin(), deletedCollections.end(), keyCollectionId),
              deletedCollections.end());
            createdCollections.push_back(keyCollectionId);
        } else if (static_cast<trackingOperation>(valueOperationType) ==
          trackingOperation::DELETE_COLLECTION) {
            createdCollections.erase(
              std::remove(createdCollections.begin(), createdCollections.end(), keyCollectionId),
              createdCollections.end());
            deletedCollections.push_back(keyCollectionId);
        }
    }
}

void
Validator::UpdateDataModel(const trackingOperation &operation, validation_collection &collection,
  const uint64_t collectionId, const char *key, const char *value)
{
    if (operation == trackingOperation::DELETE_KEY) {
        /* Search for the key validating that it exists. */
        const auto it = collection.find(key);
        if (it == collection.end())
            testutil_die(LOG_ERROR,
              "Validation failed: key deleted that doesn't exist. Collection id: %lu Key: %s",
              collectionId, key);
        else if (it->second.exists == false)
            /* The key has been deleted twice. */
            testutil_die(LOG_ERROR,
              "Validation failed: deleted key deleted again. Collection id: %lu Key: %s",
              collectionId, it->first.c_str());

        /* Update the deleted key. */
        it->second.exists = false;
    } else if (operation == trackingOperation::INSERT)
        collection[key_value_t(key)] = KeyState{true, key_value_t(value)};
    else
        testutil_die(LOG_ERROR, "Validation failed: unexpected operation in the tracking table: %d",
          static_cast<trackingOperation>(operation));
}

void
Validator::VerifyCollection(
  ScopedSession &session, const uint64_t collectionId, validation_collection &collection)
{
    /* Check the collection exists on disk. */
    if (!VerifyCollectionFileState(session, collectionId, true))
        testutil_die(LOG_ERROR,
          "Validation failed: collection %lu not present on disk while it has been tracked as "
          "created.",
          collectionId);

    /* Walk through each key/value pair of the current collection. */
    for (const auto &record : collection)
        VerifyKeyValue(session, collectionId, record.first, record.second);
}

bool
Validator::VerifyCollectionFileState(
  ScopedSession &session, const uint64_t collectionId, bool exists) const
{
    /*
     * We don't necessarily expect to successfully open the cursor so don't create a scoped cursor.
     */
    WT_CURSOR *cursor;
    int ret = session->open_cursor(session.Get(),
      Database::GenerateCollectionName(collectionId).c_str(), nullptr, nullptr, &cursor);
    if (ret == 0)
        testutil_check(cursor->close(cursor));
    return (exists ? (ret == 0) : (ret != 0));
}

void
Validator::VerifyKeyValue(ScopedSession &session, const uint64_t collectionId,
  const std::string &key, const KeyState &keyState)
{
    ScopedCursor cursor = session.OpenScopedCursor(Database::GenerateCollectionName(collectionId));
    cursor->set_key(cursor.Get(), key.c_str());
    int ret = cursor->search(cursor.Get());
    testutil_assertfmt(ret == 0 || ret == WT_NOTFOUND,
      "Validation failed: Unexpected error returned %d while searching for a key. Key: %s, "
      "collectionId: %lu",
      ret, key.c_str(), collectionId);
    if (ret == WT_NOTFOUND && keyState.exists)
        testutil_die(LOG_ERROR,
          "Validation failed: Search failed to find key that should exist. Key: %s, "
          "collectionId: %lu",
          key.c_str(), collectionId);
    else if (ret == 0 && keyState.exists == false) {
        testutil_die(LOG_ERROR,
          "Validation failed: Key exists when it is expected to be deleted. Key: %s, "
          "collectionId: %lu",
          key.c_str(), collectionId);
    }

    if (keyState.exists == false)
        return;

    const char *retrievedValue;
    testutil_check(cursor->get_value(cursor.Get(), &retrievedValue));
    if (keyState.value != key_value_t(retrievedValue))
        testutil_die(LOG_ERROR,
          "Validation failed: Value mismatch for key. Key: %s, collectionId: %lu, Expected "
          "value: %s, Found value: %s",
          key.c_str(), collectionId, keyState.value.c_str(), retrievedValue);
}
} // namespace test_harness
