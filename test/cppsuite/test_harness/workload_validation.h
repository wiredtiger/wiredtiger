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

#ifndef WORKLOAD_VALIDATION_H
#define WORKLOAD_VALIDATION_H

#include <string>

extern "C" {
#include "wiredtiger.h"
}

namespace test_harness {

/* A key has an associated value if it exists. */
struct Key {
    bool exists;
    std::string value;
};

/* A collection is made of Key objects. */
struct Collection {
    std::map<std::string, Key> keys;
};

/* Representation of the collections in memory. */
struct Database {
    std::map<std::string, Collection> collections;
};

/*
 * Class that can validate database state and collection data.
 */
class workload_validation {
    public:
    /*
     * Validate the on disk data against what has been tracked during the test. The first step is to
     * replay the tracked operations so a representation in memory of the collections is created.
     * This representation is then compared to what is on disk. The second step is to go through
     * what has been saved on disk and make sure the memory representation has the same data.
     * operation_table_name is the collection that contains all the operations about the key/value
     * pairs in the different collections used during the test. schema_table_name is the collection
     * that contains all the operations about the creation or deletion of collections during the
     * test.
     */
    bool
    validate(const std::string &operation_table_name, const std::string &schema_table_name)
    {
        WT_SESSION *session;
        Database database;
        std::string collection_name;
        /* Existing collections after the test. */
        std::vector<std::string> created_collections;
        bool is_valid;

        session = connection_manager::instance().create_session();

        /* Retrieve the collections that were created during the test. */
        collection_name = schema_table_name;
        created_collections = parse_schema_tracking_table(session, collection_name);
        /*
         * Build in memory the final state of each created collection according to the tracked
         * operations.
         */
        collection_name = operation_table_name;
        for (auto const &active_collection : created_collections)
            parse_operation_tracking_table(session, collection_name, active_collection, database);

        /* Check all tracked operations in memory against the database on disk. */
        is_valid = check_reference(session, database);

        /* Check what has been saved on disk against what has been tracked. */
        if (is_valid) {
            for (auto const &collection : created_collections) {
                is_valid = check_disk_state(session, collection, database);
                if (!is_valid) {
                    debug_print(
                      "check_disk_state failed for collection " + collection, DEBUG_ERROR);
                    break;
                }
            }

        } else
            debug_print("check_reference failed!", DEBUG_ERROR);

        return (is_valid);
    }

    private:
    /*
     * collection_name is the collection that contains the operations on the different collections
     * during the test.
     */
    const std::vector<std::string>
    parse_schema_tracking_table(WT_SESSION *session, const std::string &collection_name)
    {
        WT_CURSOR *cursor;
        wt_timestamp_t key_timestamp;
        const char *key_collection_name;
        int value_operation_type;
        std::vector<std::string> created_collections;

        testutil_check(session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor));

        while (cursor->next(cursor) == 0) {
            testutil_check(cursor->get_key(cursor, &key_collection_name, &key_timestamp));
            testutil_check(cursor->get_value(cursor, &value_operation_type));

            debug_print("Collection name is " + std::string(key_collection_name), DEBUG_TRACE);
            debug_print("Timestamp is " + std::to_string(key_timestamp), DEBUG_TRACE);
            debug_print("Operation type is " + std::to_string(value_operation_type), DEBUG_TRACE);

            if (static_cast<tracking_operation>(value_operation_type) ==
              tracking_operation::CREATE) {
                created_collections.push_back(key_collection_name);
            } else if (static_cast<tracking_operation>(value_operation_type) ==
              tracking_operation::DELETE_COLLECTION) {
                created_collections.erase(std::remove(created_collections.begin(),
                                            created_collections.end(), key_collection_name),
                  created_collections.end());
            }
        }

        return (created_collections);
    }

    /*
     * Parse the tracked operations to build a representation in memory of the collections at the
     * end of the test. tracking_collection_name is the tracking collection used to save the
     * operations performed on the collections during the test. collection_name is the collection
     * that needs to be represented in memory.
     */
    void
    parse_operation_tracking_table(WT_SESSION *session, const std::string &tracking_collection_name,
      const std::string &collection_name, Database &database)
    {
        WT_CURSOR *cursor;
        wt_timestamp_t key_timestamp;
        int exact, value_operation_type;
        const char *key, *key_collection_name, *value;
        /* Keys start at 0. */
        const std::string key_str = "0";

        testutil_check(
          session->open_cursor(session, tracking_collection_name.c_str(), NULL, NULL, &cursor));

        cursor->set_key(cursor, collection_name.c_str(), key_str.c_str());
        testutil_check(cursor->search_near(cursor, &exact));
        /*
         * Since the timestamp which is part of the key is not provided, exact is expected to be
         * greater than 0.
         */
        testutil_assert(exact >= 0);

        do {
            testutil_check(cursor->get_key(cursor, &key_collection_name, &key, &key_timestamp));
            testutil_check(cursor->get_value(cursor, &value_operation_type, &value));

            debug_print("Collection name is " + std::string(key_collection_name), DEBUG_TRACE);
            debug_print("Key is " + std::string(key), DEBUG_TRACE);
            debug_print("Timestamp is " + std::to_string(key_timestamp), DEBUG_TRACE);
            debug_print("Operation type is " + std::to_string(value_operation_type), DEBUG_TRACE);
            debug_print("Value is " + std::string(value), DEBUG_TRACE);

            /*
             * If the cursor is reading an operation for a different collection, we know all the
             * operations have been parsed for the collection we were interested in.
             */
            if (std::string(key_collection_name) != collection_name)
                break;

            /* Replay the current operation. */
            switch (static_cast<tracking_operation>(value_operation_type)) {
            case tracking_operation::DELETE_KEY:
                /*
                 * Operations are parsed from the oldest to the most recent one. It is safe to
                 * assume the key has been inserted previously in an existing collection and can be
                 * deleted safely.
                 */
                database.collections.at(key_collection_name).keys.at(key).exists = false;
                break;
            case tracking_operation::INSERT: {
                /* Keys are unique, it is safe to assume the key has not been encountered before. */
                Key k;
                k.exists = true;
                k.value = value;
                database.collections[key_collection_name].keys[key] = k;
                break;
            }
            default:
                testutil_die(DEBUG_ABORT, "Unexpected operation in the tracking table: %d",
                  value_operation_type);
                break;
            }

        } while (cursor->next(cursor) == 0);

        if (cursor->reset(cursor) != 0)
            debug_print("Cursor could not be reset !", DEBUG_ERROR);
    }

    /*
     * Compare the tracked operations against what has been saved on disk. database is the
     * representation in memory of the collections after the test according to the tracking table.
     */
    bool
    check_reference(WT_SESSION *session, Database &database)
    {
        bool is_valid = true;
        Collection collection;
        std::string collection_name;
        Key key;
        key_value_t key_str, *value;

        for (const auto &it_collections : database.collections) {
            collection_name = it_collections.first;
            /*
             * Check the collection exists on disk.
             */
            is_valid = verify_database_state(session, collection_name, true);

            if (is_valid) {
                collection = it_collections.second;
                /* Walk through each key/value pair of the current collection. */
                for (const auto &keys : collection.keys) {
                    key_str = keys.first;
                    key = keys.second;
                    /* The key/value pair exists. */
                    if (key.exists)
                        is_valid =
                          (is_key_present(session, collection_name, key_str.c_str()) == true);
                    /* The key has been deleted. */
                    else
                        is_valid =
                          (is_key_present(session, collection_name, key_str.c_str()) == false);

                    /* Check the associated value is valid. */
                    if (is_valid && key.exists)
                        is_valid =
                          verify_value(session, collection_name, key_str.c_str(), key.value);

                    if (!is_valid) {
                        debug_print("check_reference failed for key " + key_str, DEBUG_ERROR);
                        break;
                    }
                }
            }

            if (!is_valid) {
                debug_print(
                  "check_reference failed for collection " + collection_name, DEBUG_ERROR);
                break;
            }
        }

        return (is_valid);
    }

    /* Check what is present on disk against what has been tracked. */
    bool
    check_disk_state(WT_SESSION *session, const std::string &collection_name, Database &database)
    {
        WT_CURSOR *cursor;
        Collection collection;
        bool is_valid;
        /* Key/value pairs on disk. */
        const char *key_on_disk, *value_on_disk;
        key_value_t key_str, value_str;

        testutil_check(session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor));

        /* Check the collection has been tracked. */
        is_valid = database.collections.count(collection_name);

        if (!is_valid)
            debug_print(
              "Collection " + collection_name + " has not been tracked or has been deleted",
              DEBUG_ERROR);
        else
            collection = database.collections.at(collection_name);

        /* Read the collection on disk. */
        while (is_valid && (cursor->next(cursor) == 0)) {
            testutil_check(cursor->get_key(cursor, &key_on_disk));
            testutil_check(cursor->get_value(cursor, &value_on_disk));

            key_str = std::string(key_on_disk);

            debug_print("Key on disk is " + key_str, DEBUG_TRACE);
            debug_print("Value on disk is " + std::string(value_on_disk), DEBUG_TRACE);

            /* Check the key on disk has been saved in memory too. */
            if ((collection.keys.count(key_str) > 0) && collection.keys.at(key_str).exists) {
                value_str = collection.keys.at(key_str).value;
                /*
                 * Check the key/value pair on disk matches the one in memory from the tracked
                 * operations.
                 */
                is_valid = (value_str == key_value_t(value_on_disk));
                if (!is_valid)
                    debug_print(" Key/Value pair mismatch.\n Disk key: " + key_str +
                        "\n Disk value: " + std ::string(value_on_disk) +
                        "\n Tracking table key: " + key_str + "\n Tracking table value exists: " +
                        std::to_string(collection.keys.at(key_str).exists) +
                        "\n Tracking table value: " + value_str,
                      DEBUG_ERROR);
            } else {
                is_valid = false;
                debug_print(
                  "The key " + std::string(key_on_disk) + " present on disk has not been tracked",
                  DEBUG_ERROR);
            }
        }

        return (is_valid);
    }

    /*
     * Check whether a collection exists on disk. collection_name is the collection to check. exists
     * needs to be set to true if the collection is expected to be existing, false otherwise.
     */
    bool
    verify_database_state(
      WT_SESSION *session, const std::string &collection_name, bool exists) const
    {
        WT_CURSOR *cursor;
        int ret = session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor);
        return (exists ? (ret == 0) : (ret != 0));
    }

    template <typename K>
    bool
    is_key_present(WT_SESSION *session, const std::string &collection_name, const K &key)
    {
        WT_CURSOR *cursor;
        testutil_check(session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor));
        cursor->set_key(cursor, key);
        return (cursor->search(cursor) == 0);
    }

    /* Verify the given expected value is the same on disk. */
    template <typename K, typename V>
    bool
    verify_value(WT_SESSION *session, const std::string &collection_name, const K &key,
      const V &expected_value)
    {
        WT_CURSOR *cursor;
        const char *value;

        testutil_check(session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor));
        cursor->set_key(cursor, key);
        testutil_check(cursor->search(cursor));
        testutil_check(cursor->get_value(cursor, &value));

        return (value == expected_value);
    }
};
} // namespace test_harness

#endif
