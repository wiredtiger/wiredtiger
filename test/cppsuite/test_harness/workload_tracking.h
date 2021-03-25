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

#ifndef WORKLOAD_TRACKING_H
#define WORKLOAD_TRACKING_H

#include <algorithm>
#include <map>

/*
 * Default schema for tracking operations on collections (key_format: Collection name / Key /
 * Timestamp, value_format: Operation type / Value)
 */
#define OPERATION_TRACKING_KEY_FORMAT WT_UNCHECKED_STRING(Sii)
#define OPERATION_TRACKING_VALUE_FORMAT WT_UNCHECKED_STRING(iS)
#define OPERATION_TRACKING_TABLE_CONFIG \
    "key_format=" OPERATION_TRACKING_KEY_FORMAT ",value_format=" OPERATION_TRACKING_VALUE_FORMAT

/*
 * Default schema for tracking schema operations on collections (key_format: Collection name /
 * Timestamp, value_format: Operation type)
 */
#define SCHEMA_TRACKING_KEY_FORMAT WT_UNCHECKED_STRING(Si)
#define SCHEMA_TRACKING_VALUE_FORMAT WT_UNCHECKED_STRING(i)
#define SCHEMA_TRACKING_TABLE_CONFIG \
    "key_format=" SCHEMA_TRACKING_KEY_FORMAT ",value_format=" SCHEMA_TRACKING_VALUE_FORMAT

namespace test_harness {

/* Tracking operations. */
enum class tracking_operation { CREATE_COLLECTION, DELETE_COLLECTION, DELETE_KEY, INSERT, UPDATE };
/* Class used to track operations performed on collections */
template <typename K, typename V> class workload_tracking : public component {

    public:
    workload_tracking(configuration *_config, const std::string &operation_table_config,
      const std::string &operation_table_name, const std::string &schema_table_config,
      const std::string &schema_table_name)
        : component(_config), _cursor_operations(nullptr), _cursor_schema(nullptr),
          _operation_table_config(operation_table_config),
          _operation_table_name(operation_table_name), _schema_table_config(schema_table_config),
          _schema_table_name(schema_table_name)
    {
    }

    const std::string &
    get_schema_table_name() const
    {
        return _schema_table_name;
    }

    const std::string &
    get_operation_table_name() const
    {
        return _operation_table_name;
    }

    void
    load()
    {
        WT_SESSION *session;

        testutil_check(_config->get_bool(ENABLED, _enabled));
        if (!_enabled)
            return;

        /* Initiate schema tracking. */
        session = connection_manager::instance().create_session();
        testutil_check(
          session->create(session, _schema_table_name.c_str(), _schema_table_config.c_str()));
        testutil_check(
          session->open_cursor(session, _schema_table_name.c_str(), NULL, NULL, &_cursor_schema));
        debug_print("Schema tracking initiated", DEBUG_TRACE);

        /* Initiate operations tracking. */
        testutil_check(
          session->create(session, _operation_table_name.c_str(), _operation_table_config.c_str()));
        testutil_check(session->open_cursor(
          session, _operation_table_name.c_str(), NULL, NULL, &_cursor_operations));
        debug_print("Operations tracking created", DEBUG_TRACE);
    }

    void
    run()
    {
        /* Does not do anything. */
    }

    const std::vector<std::string> &
    get_created_collections() const
    {
        return _created_collections;
    }

    const std::vector<K> &
    get_created_keys(const std::string &collection_name) const
    {
        return _created_keys.at(collection_name);
    }

    /*
     * Keep track of the creation or deletion of a given collection.
     */
    int
    save_operation_on_collection(
      const tracking_operation &operation, const std::string &collection_name, wt_timestamp_t ts)
    {
        int error_code = 0;

        if (!_enabled)
            return (error_code);

        if ((operation == tracking_operation::CREATE_COLLECTION) ||
          (operation == tracking_operation::DELETE_COLLECTION)) {
            _cursor_schema->set_key(_cursor_schema, collection_name.c_str(), ts);
            _cursor_schema->set_value(_cursor_schema, static_cast<int>(operation));

            error_code = _cursor_schema->insert(_cursor_schema);

            if (error_code == 0) {
                debug_print(
                  "save_operation_on_collection: saved operation on collection.", DEBUG_TRACE);

                /* Update the collection state in memory. */
                if (operation == tracking_operation::CREATE_COLLECTION)
                    _created_collections.push_back(collection_name);
                else if (operation == tracking_operation::DELETE_COLLECTION) {
                    _created_collections.erase(std::remove(_created_collections.begin(),
                                                 _created_collections.end(), collection_name),
                      _created_collections.end());
                    /* Keys associated to deleted collection can be removed. */
                    _created_keys[collection_name].clear();
                }
            } else
                debug_print(
                  "save_operation_on_collection: failed to save operation on collection !",
                  DEBUG_ERROR);
        } else {
            error_code = -1;
            debug_print(
              "save_operation_on_collection: invalid operation: " + static_cast<int>(operation),
              DEBUG_ERROR);
        }

        return (error_code);
    }

    /*
     * Keep track of the operations on the key/value pairs on a given collection.
     */
    int
    save_operation(const tracking_operation &operation, const std::string &collection_name,
      const K &key, const V &value, wt_timestamp_t ts)
    {
        int error_code = 0;

        if (!_enabled)
            return (error_code);

        /* Select the correct cursor to save in the collection associated to specific operations. */
        if ((operation == tracking_operation::CREATE_COLLECTION) ||
          (operation == tracking_operation::DELETE_COLLECTION)) {
            error_code = -1;
            debug_print(
              "save_operation: invalid operation: " + static_cast<int>(operation), DEBUG_ERROR);
        } else {
            _cursor_operations->set_key(_cursor_operations, collection_name.c_str(), key, ts);
            _cursor_operations->set_value(_cursor_operations, static_cast<int>(operation), value);

            error_code = _cursor_operations->insert(_cursor_operations);

            if (error_code == 0) {
                debug_print("Workload tracking saved operation.", DEBUG_TRACE);

                /* Update the key in memory. */
                if (operation == tracking_operation::DELETE_KEY)
                    _created_keys[collection_name].erase(
                      std::remove(_created_keys[collection_name].begin(),
                        _created_keys[collection_name].end(), key),
                      _created_keys[collection_name].end());
                else if (operation == tracking_operation::INSERT)
                    _created_keys[collection_name].push_back(key);
            } else
                debug_print("Workload tracking failed to save operation !", DEBUG_ERROR);
        }

        return (error_code);
    }

    private:
    /* Created collections during the test. */
    std::vector<std::string> _created_collections;
    /* Created keys in each collection during the test. */
    std::map<std::string, std::vector<K>> _created_keys;
    /* Cursor associated to the operations on the key/value pairs of the different collections. */
    WT_CURSOR *_cursor_operations;
    /* Cursor associated to the creation and deletion of collection. */
    WT_CURSOR *_cursor_schema;
    const std::string _operation_table_config;
    const std::string _operation_table_name;
    const std::string _schema_table_config;
    const std::string _schema_table_name;
};
} // namespace test_harness

#endif
