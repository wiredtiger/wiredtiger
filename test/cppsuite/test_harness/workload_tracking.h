#ifndef WORKLOAD_TRACKING_H
#define WORKLOAD_TRACKING_H

#include <iostream>

#include "time_utils.h"

/* Default schema for tracking table
 * key_format : Collection name / Key
 * value_format : Operation type / Value / Time
 */
#define DEFAULT_TRACKING_TABLE_SCHEMA "key_format=Si,value_format=iSi"

namespace test_harness {
/* Tracking operations. */
enum class tracking_operation { INSERT };
/* Class used to track operations performed on collections */
class workload_tracking {

    public:
    workload_tracking(WT_CONNECTION *conn, const std::string &collection_name)
        : _collection_name(collection_name), _conn(conn)
    {
    }

    ~workload_tracking()
    {
        if (_session != nullptr) {
            if (_session->close(_session, NULL) != 0)
                /* Failing to close session is not blocking. */
                debug_info(
                  "Failed to close session, shutting down uncleanly", _trace_level, DEBUG_ERROR);
            _session = nullptr;
        }
    }

    int
    start(const std::string &table_schema = DEFAULT_TRACKING_TABLE_SCHEMA)
    {
        /* Open session. */
        testutil_check(_conn->open_session(_conn, NULL, NULL, &_session));

        /* Create tracking collection. */
        testutil_check(_session->create(_session, _collection_name.c_str(), table_schema.c_str()));
        testutil_check(
          _session->open_cursor(_session, _collection_name.c_str(), NULL, NULL, &_cursor));
        debug_info("Tracking collection created", _trace_level, DEBUG_INFO);

        return (0);
    }

    template <typename K, typename V>
    int
    save(const tracking_operation &operation, const std::string &collection_name, const K &key,
      const V &value)
    {
        int error_code;

        _cursor->set_key(_cursor, collection_name.c_str(), key);
        _cursor->set_value(_cursor, static_cast<int>(operation), value, get_time());
        error_code = _cursor->insert(_cursor);

        if (error_code == 0) {
            debug_info("Workload tracking saved operation.", _trace_level, DEBUG_INFO);
        } else {
            debug_info("Workload tracking failed to save operation !", _trace_level, DEBUG_ERROR);
        }

        return error_code;
    }

    private:
    WT_CONNECTION *_conn = nullptr;
    std::string _collection_name;
    WT_CURSOR *_cursor = nullptr;
    WT_SESSION *_session = nullptr;
};
} // namespace test_harness

#endif