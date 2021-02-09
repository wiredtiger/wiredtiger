/* Include guard. */
#ifndef TEST_WORKLOAD_H
#define TEST_WORKLOAD_H

#include <vector>

namespace test_workload {
class workload {

    public:
    workload(const char *configuration)
    {
        // TODO: process the configuration
    }

    ~workload()
    {
        delete _conn;
        delete _cursor;
        delete _home;
        delete _session;
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
    open_session()
    {
        // TODO: What if _conn is NULL ?
        return (_conn->open_session(_conn, NULL, NULL, &_session));
    }

    int
    create(const char *name, const char *config)
    {
        // TODO: What if _session is NULL ?
        return _session->create(_session, name, config);
    }

    int
    create(const std::vector<const char *> names, const std::vector<const char *> configs)
    // create(const char *name, const char *config)
    {
        int return_code = 0;

        if (names.size() != configs.size()) {
            // TODO
            return_code = -1;
        }

        for (int i = 0; (i < names.size()) && (return_code == 0); i++) {
            // TODO: What if _session is NULL ?
            return_code = create(names[i], configs[i]);
            // return_code = _session->create(_session, names[i], configs[i]);
        }

        return return_code;
        // return (_session->create(_session, name, config));
    }

    int
    open_cursor(const char *uri, WT_CURSOR *to_dup, const char *config)
    {
        // TODO: What if _session is NULL ?
        return (_session->open_cursor(_session, uri, to_dup, config, &_cursor));
    }

    int
    insert(const char *value)
    {
        // TODO: What if cursor is NULL ?
        return (_cursor->insert(_cursor));
    }

    void
    set_key(const char *key)
    {
        // TODO: What if cursor is NULL ?
        _cursor->set_key(_cursor, key);
    }

    void
    set_value(const char *value)
    {
        // TODO: What if cursor is NULL ?
        _cursor->set_value(_cursor, value);
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
    WT_CURSOR *_cursor;
};
} // namespace test_workload

#endif
