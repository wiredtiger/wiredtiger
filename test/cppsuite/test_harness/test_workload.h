/* Include guard. */
#ifndef TEST_WORKLOAD_H
#define TEST_WORKLOAD_H

#include <cstdint>
#include <map>
#include <vector>

extern "C" {
#include "test_util.h"
}

#include "api_const.h"
#include "random_generator.h"
#include "configuration_settings.h"

#define DEBUG_ERROR 1
#define DEBUG_INFO 2

namespace test_workload {
class workload {

    public:
    workload() {}
    workload(test_harness::configuration *configuration)
    {
        _configuration = configuration;
    }

    ~workload()
    {
        clean();
    }

    int
    clean()
    {
        WT_RET(_session->close(_session, NULL));
        _session = nullptr;

        WT_RET(_conn->close(_conn, NULL));
        _conn = nullptr;

        return 0;
    }

    int
    load(const char *home = DEFAULT_DIR)
    {
        int64_t collection_count = 0;
        int64_t value_size = 0;
        int64_t key_count = 0;
        std::string collection_name_tmp = "";
        WT_CURSOR *cursor = nullptr;

        // Create the working dir
        testutil_make_work_dir(home);

        // Open connection
        WT_RET(wiredtiger_open(home, NULL, test_harness::api_const::CONNECTION_CREATE, &_conn));

        // Open session
        WT_RET(_conn->open_session(_conn, NULL, NULL, &_session));

        // Create collections
        WT_RET(
          _configuration->get_int(test_harness::api_const::COLLECTION_COUNT, collection_count));
        for (int i = 0; i < collection_count; ++i) {
            collection_name_tmp = "table:collection" + std::to_string(i);
            const char *collection_name = collection_name_tmp.c_str();
            WT_RET(_session->create(_session, collection_name, DEFAULT_TABLE_SCHEMA));
            collection_names.push_back(collection_name_tmp);
        }

        debug_info(std::to_string(collection_count) + " collections created", DEBUG_INFO);

        // Generate keys
        _configuration->get_int(test_harness::api_const::KEY_COUNT, key_count);
        _configuration->get_int(test_harness::api_const::VALUE_SIZE, value_size);
        for (const auto &collection_name : collection_names) {
            WT_RET(_session->open_cursor(_session, collection_name.c_str(), NULL, NULL, &cursor));
            for (size_t j = 0; j < key_count; ++j) {
                cursor->set_key(cursor, j);
                std::string str_tmp =
                  random_generator::random_generator::getInstance()->generate_string(value_size);
                const char *str = str_tmp.c_str();
                cursor->set_value(cursor, str);
                WT_RET(cursor->insert(cursor));
            }
        }

        debug_info(std::to_string(collection_count) + " key/value inserted", DEBUG_INFO);

        return 0;
    }

    int
    run()
    {
        // Empty until thread management lib is implemented
        return 0;
    }

    int
    insert(WT_CURSOR *cursor, const char *value)
    {
        if (cursor == nullptr) {
            throw std::invalid_argument("failed to call insert, invalid cursor");
        }
        return (cursor->insert(cursor));
    }

    int
    search(WT_CURSOR *cursor)
    {
        if (cursor == nullptr) {
            throw std::invalid_argument("failed to call search, invalid cursor");
        }
        return cursor->search(cursor);
    }

    int
    search_near(WT_CURSOR *cursor, int *exact)
    {
        if (cursor == nullptr) {
            throw std::invalid_argument("failed to call search_near, invalid cursor");
        }
        return cursor->search_near(cursor, exact);
    }

    int
    update(WT_CURSOR *cursor)
    {
        if (cursor == nullptr) {
            throw std::invalid_argument("failed to call update, invalid cursor");
        }
        return (cursor->update(cursor));
    }

    static int64_t _trace_level;

    private:
    static void
    debug_info(const std::string &str, int64_t trace_type)
    {
        if (_trace_level >= trace_type) {
            std::cout << str << std::endl;
        }
    }

    std::vector<std::string> collection_names;
    test_harness::configuration *_configuration = nullptr;
    WT_CONNECTION *_conn = nullptr;
    WT_SESSION *_session = nullptr;
};
} // namespace test_workload

#endif
