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

#ifndef WORKLOAD_GENERATOR_H
#define WORKLOAD_GENERATOR_H

#include <algorithm>
#include <map>

#include "random_generator.h"
#include "workload_tracking.h"
#include "workload_validation.h"

namespace test_harness {
/*
 * Class that can execute operations based on a given configuration.
 */
class workload_generator : public component {
    public:
    workload_generator(configuration *configuration) : _configuration(configuration) {}

    ~workload_generator()
    {
        delete _workload_tracking;
        _workload_tracking = nullptr;
        for (auto &it : _workers)
            delete it;
    }

    /*
     * Function that performs the following steps using the configuration that is defined by the
     * test:
     *  - Create the working dir.
     *  - Open a connection.
     *  - Open a session.
     *  - Create n collections as per the configuration.
     *      - Open a cursor on each collection.
     *      - Insert m key/value pairs in each collection. Values are random strings which size is
     * defined by the configuration.
     */
    void
    load()
    {
        WT_CURSOR *cursor;
        WT_SESSION *session;
        int64_t collection_count, key_count, value_size;
        std::string collection_name, home;

        cursor = nullptr;
        collection_count = key_count = value_size = 0;
        collection_name = "";

        /* Create the activity tracker if required. */
        testutil_check(_configuration->get_bool(ENABLE_TRACKING, _enable_tracking));
        if (_enable_tracking) {
            _workload_tracking = new workload_tracking(TABLE_SCHEMA_TRACKING);
            _workload_tracking->load();
        }

        /* Get a session. */
        session = connection_manager::instance().create_session();

        /* Create n collections as per the configuration and store each collection name. */
        testutil_check(_configuration->get_int(COLLECTION_COUNT, collection_count));
        for (int i = 0; i < collection_count; ++i) {
            collection_name = "table:collection" + std::to_string(i);
            testutil_check(session->create(session, collection_name.c_str(), DEFAULT_TABLE_SCHEMA));
            if (_enable_tracking)
                testutil_check(
                  _workload_tracking->save(tracking_operation::CREATE, collection_name, 0, ""));
            _collection_names.push_back(collection_name);
        }
        debug_info(
          std::to_string(collection_count) + " collections created", _trace_level, DEBUG_INFO);

        /* Open a cursor on each collection and use the configuration to insert key/value pairs. */
        testutil_check(_configuration->get_int(KEY_COUNT, key_count));
        testutil_check(_configuration->get_int(VALUE_SIZE, value_size));
        for (const auto &collection_name : _collection_names) {
            /* WiredTiger lets you open a cursor on a collection using the same pointer. When a
             * session is closed, WiredTiger APIs close the cursors too. */
            testutil_check(
              session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor));
            for (size_t j = 0; j < key_count; ++j) {
                /* Generation of a random string value using the size defined in the test
                 * configuration. */
                std::string generated_value =
                  random_generator::random_generator::instance().generate_string(value_size);
                testutil_check(insert(
                  cursor, collection_name, j + 1, generated_value.c_str(), _enable_tracking));
            }
        }
        debug_info("Load stage done", _trace_level, DEBUG_INFO);
    }

    /* Do the work of the main part of the workload. */
    void
    run()
    {
        WT_SESSION *session;
        int64_t duration_seconds, read_threads;

        session = nullptr;
        duration_seconds = read_threads = 0;

        testutil_check(_configuration->get_int(DURATION_SECONDS, duration_seconds));
        testutil_check(_configuration->get_int(READ_THREADS, read_threads));
        /* Generate threads to execute read operations on the collections. */
        for (int i = 0; i < read_threads; ++i) {
            thread_context *tc = new thread_context(_collection_names, thread_operation::READ);
            _workers.push_back(tc);
            _thread_manager.add_thread(tc, &execute_operation);
        }
    }

    void
    finish()
    {
        int error_code = 0;

        debug_info("Workload generator stage done", _trace_level, DEBUG_INFO);
        for (const auto &it : _workers) {
            it->finish();
        }
        _thread_manager.join();

        /* Validation stage. */
        if (!_enable_tracking || validate())
            std::cout << "SUCCESS" << std::endl;
        else
            std::cout << "FAILED" << std::endl;
    }

    /* Workload threaded operations. */
    static void
    execute_operation(thread_context &context)
    {
        WT_SESSION *session;

        session = connection_manager::instance().create_session();

        switch (context.get_thread_operation()) {
        case thread_operation::READ:
            read_operation(context, session);
            break;
        case thread_operation::REMOVE:
        case thread_operation::INSERT:
        case thread_operation::UPDATE:
            /* Sleep until it is implemented. */
            while (context.is_running())
                std::this_thread::sleep_for(std::chrono::seconds(1));
            break;
        default:
            testutil_die(DEBUG_ABORT, "system: thread_operation is unknown : %d",
              static_cast<int>(context.get_thread_operation()));
            break;
        }
    }

    /* Basic read operation that walks a cursors across all collections. */
    static void
    read_operation(thread_context &context, WT_SESSION *session)
    {
        WT_CURSOR *cursor;
        WT_DECL_RET;
        std::vector<WT_CURSOR *> cursors;

        /* Get a cursor for each collection in collection_names. */
        for (const auto &it : context.get_collection_names()) {
            testutil_check(session->open_cursor(session, it.c_str(), NULL, NULL, &cursor));
            cursors.push_back(cursor);
        }

        while (context.is_running()) {
            /* Walk each cursor. */
            for (const auto &it : cursors) {
                if ((ret = it->next(it)) != 0)
                    it->reset(it);
            }
        }
    }

    /* WiredTiger APIs wrappers for single operations. */
    template <typename K, typename V>
    int
    insert(WT_CURSOR *cursor, const std::string &collection_name, K key, V value, bool save)
    {
        int error_code;

        if (cursor == nullptr)
            throw std::invalid_argument("Failed to call insert, invalid cursor");

        cursor->set_key(cursor, key);
        cursor->set_value(cursor, value);
        error_code = cursor->insert(cursor);

        if (error_code == 0) {
            debug_info("key/value inserted", _trace_level, DEBUG_INFO);
            if (save)
                error_code =
                  _workload_tracking->save(tracking_operation::INSERT, collection_name, key, value);
        } else
            debug_info("key/value insertion failed", _trace_level, DEBUG_ERROR);

        return error_code;
    }

    static int
    search(WT_CURSOR *cursor)
    {
        if (cursor == nullptr)
            throw std::invalid_argument("Failed to call search, invalid cursor");
        return (cursor->search(cursor));
    }

    static int
    search_near(WT_CURSOR *cursor, int *exact)
    {
        if (cursor == nullptr)
            throw std::invalid_argument("Failed to call search_near, invalid cursor");
        return (cursor->search_near(cursor, exact));
    }

    static int
    update(WT_CURSOR *cursor)
    {
        if (cursor == nullptr)
            throw std::invalid_argument("Failed to call update, invalid cursor");
        return (cursor->update(cursor));
    }

    /*
     * Validate the on disk data against what has been tracked during the test. The first step is to
     * replay the tracked operations so a representation in memory of the collections is created.
     * This representation is then compared to what is on disk. The second step is to go through
     * what has been saved on disk and make sure the memory representation has the same data.
     */
    bool
    validate()
    {
        WT_SESSION *session;
        std::string collection_name;
        /*
         * Representation in memory of the collections at the end of the test. The first level is a
         * map that contains collection names as keys. The second level is another map that contains
         * the different key/value pairs within a given collection. If a collection yields to a null
         * map of key/value pairs, this means the collection should not be present on disk. If a
         * value from a key/value pair is null, this means the key should not be present in the
         * collection on disk.
         */
        std::map<std::string, std::map<int, std::string *> *> collections;
        /* Existing collections after the test. */
        std::vector<std::string> created_collections;
        bool is_valid;

        session = connection_manager::instance().create_session();

        /* Retrieve the created collections that need to be checked. */
        collection_name = _workload_tracking->get_collection_tracking_operations_name();
        created_collections = parse_operations_on_collections(session, collection_name);

        /* Allocate memory to the operations performed on the created collections. */
        for (auto const &it : created_collections) {
            std::map<int, std::string *> *map = new std::map<int, std::string *>();
            collections[it] = map;
        }

        /*
         * Build in memory the final state of each created collection according to the tracked
         * operations.
         */
        collection_name = _workload_tracking->get_collection_tracking_name();
        for (auto const &active_collection : created_collections)
            parse_reference(session, collection_name, active_collection, collections);

        /* Check all tracked operations in memory against the database on disk. */
        is_valid = check_reference(session, collections);

        /* Check what has been saved on disk against what has been tracked. */
        if (is_valid) {
            for (auto const &collection : created_collections) {
                is_valid = check_disk_state(session, collection, collections);
                if (!is_valid) {
                    debug_info("check_disk_state failed for collection " + collection, _trace_level,
                      DEBUG_INFO);
                    break;
                }
            }

        } else
            debug_info("check_reference failed!", _trace_level, DEBUG_INFO);

        /* Clean the allocated memory. */
        clean_memory(collections);

        return is_valid;
    }

    /* Check what is present on disk against what has been tracked. */
    bool
    check_disk_state(WT_SESSION *session, const std::string &collection_name,
      std::map<std::string, std::map<int, std::string *> *> &collections)
    {
        WT_CURSOR *cursor;
        int key;
        const char *value;
        bool is_valid;
        std::string *value_str;
        std::map<int, std::string *> *collection;

        testutil_check(session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor));

        /* Check the collection has been tracked and contains data. */
        is_valid =
          ((collections.count(collection_name) > 0) && (collections[collection_name] != nullptr));

        if (!is_valid)
            debug_info(
              "Collection " + collection_name + " has not been tracked or has been deleted",
              _trace_level, DEBUG_INFO);
        else
            collection = collections[collection_name];

        /* Read the collection on disk. */
        while (is_valid && (cursor->next(cursor) == 0)) {
            error_check(cursor->get_key(cursor, &key));
            error_check(cursor->get_value(cursor, &value));

            debug_info("Key is " + std::to_string(key), _trace_level, DEBUG_INFO);
            debug_info("Value is " + std::string(value), _trace_level, DEBUG_INFO);

            value_str = (*collection)[key];

            /* Check the key/value pair on disk matches the one in memory from the tracked
             * operations. */
            is_valid = (value_str != nullptr) && (*value_str == std::string(value));

            if (!is_valid)
                debug_info(" Key/Value pair mismatch.\n Disk key: " + std::to_string(key) +
                    "\n Disk value: " + std ::string(value) +
                    "\n Tracking table key: " + std::to_string(key) +
                    "\n Tracking table value: " + (value_str == nullptr ? "NULL" : *value_str),
                  _trace_level, DEBUG_INFO);
        }

        return is_valid;
    }

    /* Clean the memory used to represent the collections after the test. */
    void
    clean_memory(std::map<std::string, std::map<int, std::string *> *> &collections)
    {
        for (const auto &it_collections : collections) {
            if (collections[it_collections.first] != nullptr) {
                std::map<int, std::string *> *operations = collections[it_collections.first];
                for (const auto &it_operations : *operations) {
                    delete (*operations)[it_operations.first];
                    (*operations)[it_operations.first] = nullptr;
                }
                delete collections[it_collections.first];
                collections[it_collections.first] = nullptr;
            }
        }
    }

    /*
     * collection_name is the collection that contains the operations on the different collections
     * during the test.
     */
    const std::vector<std::string>
    parse_operations_on_collections(WT_SESSION *session, const std::string &collection_name)
    {
        WT_CURSOR *cursor;

        const char *key_collection_name;
        int key_timestamp, value_operation_type;

        std::vector<std::string> created_collections;

        testutil_check(session->open_cursor(session, collection_name.c_str(), NULL, NULL, &cursor));

        while (cursor->next(cursor) == 0) {
            error_check(cursor->get_key(cursor, &key_collection_name, &key_timestamp));
            error_check(cursor->get_value(cursor, &value_operation_type));

            debug_info(
              "Collection name is " + std::string(key_collection_name), _trace_level, DEBUG_INFO);
            debug_info("Timestamp is " + std::to_string(key_timestamp), _trace_level, DEBUG_INFO);
            debug_info("Operation type is " + std::to_string(value_operation_type), _trace_level,
              DEBUG_INFO);

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

        return created_collections;
    }

    /*
     * Parse the tracked operations to build a representation in memory of the collections at the
     * end of the test. tracking_collection_name is the tracking collection used to save the
     * operations performed on the collections during the test. collection_name is the collection
     * that needs to be represented in memory.
     */
    void
    parse_reference(WT_SESSION *session, const std::string &tracking_collection_name,
      const std::string &collection_name,
      std::map<std::string, std::map<int, std::string *> *> &collections)
    {
        WT_CURSOR *cursor;
        int exact;
        int error_code;
        const char *key_collection_name, *value;
        int key, key_timestamp, value_operation_type;

        testutil_check(
          session->open_cursor(session, tracking_collection_name.c_str(), NULL, NULL, &cursor));

        /* Our keys start at 0. */
        cursor->set_key(cursor, collection_name.c_str(), 0);
        error_code = cursor->search_near(cursor, &exact);

        while (error_code == 0) {
            error_check(cursor->get_key(cursor, &key_collection_name, &key, &key_timestamp));
            error_check(cursor->get_value(cursor, &value_operation_type, &value));

            debug_info(
              "Collection name is " + std::string(key_collection_name), _trace_level, DEBUG_INFO);
            debug_info("Key is " + std::to_string(key), _trace_level, DEBUG_INFO);
            debug_info("Timestamp is " + std::to_string(key_timestamp), _trace_level, DEBUG_INFO);
            debug_info("Operation type is " + std::to_string(value_operation_type), _trace_level,
              DEBUG_INFO);
            debug_info("Value is " + std::string(value), _trace_level, DEBUG_INFO);

            if (std::string(key_collection_name) != collection_name)
                break;

            /* Replay the current operation. */
            switch (static_cast<tracking_operation>(value_operation_type)) {

                break;
            case tracking_operation::DELETE_KEY: {
                /* Check if the collection exists. */
                if (collections.count(key_collection_name) < 1)
                    testutil_die(DEBUG_ABORT,
                      "Collection %s does not exist! The key %d cannot be removed.",
                      key_collection_name, key);
                /* The collection should not be null. */
                else if (collections[key_collection_name] == nullptr)
                    testutil_die(DEBUG_ABORT, "Collection %s is null!", key_collection_name);
                else {
                    delete (*(collections[key_collection_name]))[key];
                    (*(collections[key_collection_name]))[key] = nullptr;
                }
            } break;
            case tracking_operation::INSERT:
                (*(collections[key_collection_name]))[key] = new std::string(value);
                break;
            case tracking_operation::CREATE:
            case tracking_operation::DELETE_COLLECTION:
                testutil_die(DEBUG_ABORT, "Unexpected operation in the tracking table: %d",
                  static_cast<tracking_operation>(value_operation_type));
            default:
                testutil_die(
                  DEBUG_ABORT, "tracking operation is unknown : %d", value_operation_type);
                break;
            }

            error_code = cursor->next(cursor);
        }

        error_code = cursor->reset(cursor);
    }

    /*
     * Compare the tracked operations against what has been saved on disk. collections is the
     * representation in memory of the collections after the test according to the tracking table.
     */
    bool
    check_reference(
      WT_SESSION *session, std::map<std::string, std::map<int, std::string *> *> &collections)
    {

        bool collection_exists, is_valid;
        std::map<int, std::string *> *collection;
        workload_validation wv;
        std::string *value;

        for (const auto &it_collections : collections) {
            /* Check the collection is in the correct state. */
            collection_exists = (it_collections.second != nullptr);
            is_valid = wv.verify_database_state(session, it_collections.first, collection_exists);

            if (is_valid && collection_exists) {
                collection = it_collections.second;
                for (const auto &it_operations : *collection) {
                    value = (*collection)[it_operations.first];
                    /* The key/value pair exists. */
                    if (value != nullptr)
                        is_valid = (wv.is_key_present(
                                      session, it_collections.first, it_operations.first) == true);
                    /* The key has been deleted. */
                    else
                        is_valid = (wv.is_key_present(
                                      session, it_collections.first, it_operations.first) == false);

                    /* Check the associated value is valid. */
                    if (is_valid && (value != nullptr)) {
                        is_valid = (wv.verify_value(
                          session, it_collections.first, it_operations.first, *value));
                    }

                    if (!is_valid) {
                        debug_info(
                          "check_reference failed for key " + std::to_string(it_operations.first),
                          _trace_level, DEBUG_INFO);
                        break;
                    }
                }
            }

            if (!is_valid) {
                debug_info("check_reference failed for collection " + it_collections.first,
                  _trace_level, DEBUG_INFO);
                break;
            }
        }

        return is_valid;
    }

    private:
    std::vector<std::string> _collection_names;
    configuration *_configuration = nullptr;
    bool _enable_tracking = false;
    thread_manager _thread_manager;
    std::vector<thread_context *> _workers;
    workload_tracking *_workload_tracking;
};
} // namespace test_harness

#endif
