#ifndef CONN_API_H
#define CONN_API_H

/* Define common resource access functions. */
namespace test_harness {

WT_CONNECTION *_conn_conn = nullptr;
std::mutex _conn_mutex;

static void
conn_api_open()
{
    std::string home;
    home = DEFAULT_DIR;
    /* Create the working dir. */
    testutil_make_work_dir(home.c_str());

    /* Open connection. */
    testutil_check(wiredtiger_open(home.c_str(), NULL, CONNECTION_CREATE, &_conn_conn));
}

static void
conn_api_close()
{
    if (_conn_conn != nullptr) {
        if (_conn_conn->close(_conn_conn, NULL) != 0)
            /* Failing to close connection is not blocking. */
            debug_info(
              "Failed to close connection, shutting down uncleanly", _trace_level, DEBUG_ERROR);
        _conn_conn = nullptr;
    }
}

static WT_SESSION *
conn_api_get_session()
{
    WT_SESSION *session;

    if (_conn_conn == nullptr) {
        debug_info(
          "Connection is NULL, did you forget to call conn_api_open ?", _trace_level, DEBUG_ERROR);
        testutil_die(CONNECTION_NULL, "Connection is NULL");
    }

    _conn_mutex.lock();
    testutil_check(_conn_conn->open_session(_conn_conn, NULL, NULL, &session));
    _conn_mutex.unlock();

    return session;
}

} // namespace test_harness

#endif
