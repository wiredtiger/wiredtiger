/* Include guard. */
#ifndef TEST_WORKLOAD_H
#define TEST_WORKLOAD_H

#include <map>
#include <vector>

#include <cstdint>

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
    // configuration *_configuration;

    // TODO
    // Save sessions and cursors
};
} // namespace test_workload

#endif
