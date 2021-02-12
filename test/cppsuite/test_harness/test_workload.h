/* Include guard. */
#ifndef TEST_WORKLOAD_H
#define TEST_WORKLOAD_H

#include <map>
#include <vector>

#include <cstdint>

#include "test_util.h"

#include "random_generator.h"
#include "configuration_settings.h"

// uint64_t

// Questions :
// Should I check if input arg is null ? (session, cursor) => Throw exception
// What defines a session as a unique session, is there an ID ? => Custom ID
// Same question for cursor ? Is it the uri ? => Custom ID
// How to store things locally and what to store ?
// map of sessions, each session has a map of cursors ? Or list of cursors

// Do we

// Random, there is a rand.c file in support
// Can we go through a random inset key/value example ?

namespace test_workload {
class workload {

    public:
    workload(const char *name, const char *cfg)
    {
        // TODO: process the configuration
        // TODO: Set _home here ?
        // const char *name = "poc_test";
        // _configuration = new configuration(name, cfg);
        test_harness::configuration *configuration = new test_harness::configuration(name, cfg);
        _configuration = configuration;
        std::cout << configuration->get_config() << std::endl;

        int64_t value;
        if (configuration->get_int("collection_count", value) == 0) {
            std::cout << "Value collection_count is " << value << std::endl;
        } else {
            std::cout << "No value for collection_count" << std::endl;
        }
        if (configuration->get_int("key_size", value) == 0) {
            std::cout << "Value key_size is " << value << std::endl;
        } else {
            std::cout << "No value for key_size" << std::endl;
        }
    }

    ~workload()
    {
        // TODO delete all dyn var
        // delete _conn;
        // delete _home;

        // TODO
        // Close connection
    }

    int
    load()
    {
        // TODO
        // Open connection
        // Create collection
        // Save sessions, cursors, etc
        // Populate collections
        int64_t collection_count;
        int64_t key_count;

        collection_count = 0;
        key_count = 0;

        // Create the working dir
        testutil_make_work_dir(DEFAULT_DIR);

        // Open connection
        WT_RET(wiredtiger_open(DEFAULT_DIR, NULL, NULL, &_conn));

        // Create collection/s
        WT_RET(_conn->open_session(_conn, NULL, NULL, &_session));

        WT_RET(_configuration->get_int("collection_count", collection_count));
        for (int i = 0; i < collection_count; ++i) {
            const char *collection_name = ("table:collection_" + std::to_string(i)).c_str();
            WT_RET(_session->create(_session, collection_name, DEFAULT_TABLE_SCHEMA));
            collection_names.push_back(collection_name);
        }

        WT_RET(_configuration->get_int("key_count", key_count));

        // TODO
        // auto
        WT_CURSOR *cursor;
        cursor = NULL;
        
        for (size_t i = 0; i < collection_names.size(); i++) {
            WT_RET(_session->open_cursor(_session, collection_names[i], NULL, NULL, &cursor));
            for (size_t j = 0; j < key_count; ++j) {
                cursor->set_key(cursor, j);
                cursor->set_value(cursor, random_generator::random_generator::getInstance()->generate_string());
                WT_RET(cursor->insert(cursor));
            }
        }

        return 0;
    }

    int
    run()
    {
        // Empty until thread management lib is implemented
        return 0;
    }

    int
    close_connection(const char *config)
    {
        // TODO: What if _conn is NULL ?
        _conn->close(_conn, config);
        _conn = nullptr;
    }

    int
    open_connection(const char *config)
    {
        return wiredtiger_open(_home, NULL, config, &_conn);
    }

    int
    open_session(WT_SESSION *session)
    {
        if (_conn == NULL) {
            // Throw Exception
        }

        int return_code = _conn->open_session(_conn, NULL, NULL, &session);
        if (return_code == 0) {
            // TODO Save locally ?
            // map[key]
        }
        return return_code;
    }

    int
    drop(WT_SESSION *session, const char *name, const char *config)
    {
        return session->drop(session, name, config);
    }

    int
    create(WT_SESSION *session, const char *name, const char *config)
    {
        int return_code = session->create(session, name, config);
        if (return_code == 0) {
            // TODO Save locally session ?
        }
        return return_code;
    }

    int
    create(WT_SESSION *session, const std::vector<const char *> names,
      const std::vector<const char *> configs)
    {
        int return_code = 0;

        if (names.size() != configs.size()) {
            // TODO
            return_code = -1;
        }

        for (int i = 0; (i < names.size()) && (return_code == 0); i++) {
            return_code = create(session, names[i], configs[i]);
        }

        return return_code;
    }

    int
    reconfigure_cursor(WT_CURSOR *cursor, const char *config)
    {
        int return_code = cursor->reconfigure(cursor, config);
        if (return_code == 0) {
            // TODO Update locally cursor ?
        }
        return return_code;
    }

    int
    reconfigure_session(WT_SESSION *session, const char *config)
    {
        int return_code = session->reconfigure(session, config);
        if (return_code == 0) {
            // TODO Update locally session ?
        }
        return return_code;
    }

    int
    open_cursor(WT_SESSION *session, const char *uri, WT_CURSOR *to_dup, const char *config,
      WT_CURSOR *cursor)
    {
        int return_code = session->open_cursor(session, uri, to_dup, config, &cursor);
        if (return_code == 0) {
            // TODO save cursor locally ?
        }
        return return_code;
    }

    int
    insert(WT_CURSOR *cursor, const char *value)
    {
        return (cursor->insert(cursor));
    }

    void
    set_key(WT_CURSOR *cursor, const char *key)
    {
        // TODO: What if cursor is NULL ?
        cursor->set_key(cursor, key);
    }

    void
    set_value(WT_CURSOR *cursor, const char *value)
    {
        // TODO: What if cursor is NULL ?
        cursor->set_value(cursor, value);
    }

    int
    search(WT_CURSOR *cursor)
    {
        // TODO: What if cursor is NULL ?
        return cursor->search(cursor);
    }

    int
    search_near(WT_CURSOR *cursor, int *exact)
    {
        // TODO: What if cursor is NULL ?
        return cursor->search_near(cursor, exact);
    }

    private:
    const char *_home;
    WT_CONNECTION *_conn;
    WT_SESSION *_session;
    test_harness::configuration *_configuration;

    std::vector<const char *> collection_names;

    // TODO
    // Save sessions and cursors
};
} // namespace test_workload

#endif
