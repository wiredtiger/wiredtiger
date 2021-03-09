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

/*
 * Default schema for tracking table key_format : Collection name / Key / Timestamp value_format :
 * Operation type / Value
 */
#define DEFAULT_TRACKING_COLLECTION_KEY_FORMAT WT_UNCHECKED_STRING(Sii)
#define DEFAULT_TRACKING_COLLECTION_VALUE_FORMAT WT_UNCHECKED_STRING(iS)
#define DEFAULT_TRACKING_COLLECTION_TABLE_SCHEMA         \
    "key_format=" DEFAULT_TRACKING_COLLECTION_KEY_FORMAT \
    ",value_format=" DEFAULT_TRACKING_COLLECTION_VALUE_FORMAT

/*
 * Default schema for tracking operation on collection table key_format : Collection name /
 * Timestamp value_format : Operation type
 */
#define DEFAULT_TRACKING_OPERATION_KEY_FORMAT WT_UNCHECKED_STRING(Si)
#define DEFAULT_TRACKING_OPERATION_VALUE_FORMAT WT_UNCHECKED_STRING(i)
#define DEFAULT_TRACKING_OPERATION_TABLE_SCHEMA         \
    "key_format=" DEFAULT_TRACKING_OPERATION_KEY_FORMAT \
    ",value_format=" DEFAULT_TRACKING_OPERATION_VALUE_FORMAT

namespace test_harness {
/* Tracking operations. */
enum class tracking_operation { CREATE, DELETE_COLLECTION, DELETE_KEY, INSERT };
/* Class used to track operations performed on collections */
class workload_tracking : public component {

    public:
    workload_tracking(
      const std::string &collections_schema = DEFAULT_TRACKING_COLLECTION_TABLE_SCHEMA,
      const std::string &collection_tracking_name = TABLE_SCHEMA_TRACKING,
      const std::string &collection_tracking_operations_name = TABLE_OPERATION_TRACKING,
      const std::string &operations_schema = DEFAULT_TRACKING_OPERATION_TABLE_SCHEMA)
        : _collections_schema(collections_schema),
          _collection_tracking_name(collection_tracking_name),
          _collection_tracking_operations_name(collection_tracking_operations_name),
          _operations_schema(operations_schema), _cursor_collections(nullptr),
          _cursor_operations(nullptr), _timestamp(0U)
    {
    }

    const std::string &
    get_collection_tracking_name() const
    {
        return _collection_tracking_name;
    }

    const std::string &
    get_collection_tracking_operations_name() const
    {
        return _collection_tracking_operations_name;
    }

    void
    load()
    {
        WT_SESSION *session;

        /* Create tracking collection for collection states. */
        session = connection_manager::instance().create_session();
        testutil_check(
          session->create(session, _collection_tracking_name.c_str(), _collections_schema.c_str()));
        testutil_check(session->open_cursor(
          session, _collection_tracking_name.c_str(), NULL, NULL, &_cursor_collections));
        debug_info("Tracking collection states created", _trace_level, DEBUG_INFO);

        /* Create tracking collection operations. */
        testutil_check(session->create(
          session, _collection_tracking_operations_name.c_str(), _operations_schema.c_str()));
        testutil_check(session->open_cursor(
          session, _collection_tracking_operations_name.c_str(), NULL, NULL, &_cursor_operations));
        debug_info("Tracking operations created", _trace_level, DEBUG_INFO);
    }

    void
    run()
    {
        /* Does not do anything. */
    }

    template <typename K, typename V>
    int
    save(const tracking_operation &operation, const std::string &collection_name, const K &key,
      const V &value)
    {
        int error_code;
        WT_CURSOR *cursor;

        /* Select the correct cursor to save in the collection associated to specific operations. */
        switch (operation) {
        case tracking_operation::CREATE:
        case tracking_operation::DELETE_COLLECTION:
            cursor = _cursor_operations;
            cursor->set_key(cursor, collection_name.c_str(), _timestamp++);
            cursor->set_value(cursor, static_cast<int>(operation));
            break;

        default:
            cursor = _cursor_collections;
            cursor->set_key(cursor, collection_name.c_str(), key, _timestamp++);
            cursor->set_value(cursor, static_cast<int>(operation), value);
            break;
        }

        error_code = cursor->insert(cursor);

        if (error_code == 0) {
            debug_info("Workload tracking saved operation.", _trace_level, DEBUG_INFO);
        } else {
            debug_info("Workload tracking failed to save operation !", _trace_level, DEBUG_ERROR);
        }

        return error_code;
    }

    private:
    const std::string _collections_schema;
    const std::string _collection_tracking_name;
    const std::string _collection_tracking_operations_name;
    const std::string _operations_schema;
    WT_CURSOR *_cursor_collections = nullptr;
    WT_CURSOR *_cursor_operations = nullptr;
    uint64_t _timestamp;
};
} // namespace test_harness

#endif
