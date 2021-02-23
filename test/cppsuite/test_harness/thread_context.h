#ifndef THREAD_CONTEXT_H
#define THREAD_CONTEXT_H

#include <thread>

#include "wiredtiger.h"

namespace test_harness {
enum class thread_operation { INSERT, UPDATE, READ, REMOVE, CHECKPOINT, TIMESTAMP, MONITOR };
class thread_context {
    public:
    thread_context(
      WT_SESSION *session, std::vector<std::string> collection_names, thread_operation type)
        : _collection_names(collection_names), _running(false), _type(type)
    {
        _session = session;
        _thread = nullptr;
    }

    thread_context(thread_operation type) : _running(false), _type(type)
    {
        _session = nullptr;
        _thread = nullptr;
    }

    ~thread_context()
    {
        if (_session != nullptr) {
            /* Seg fault here. */
            std::cout << "need to delete the session" << std::endl;
            if (_session->close(_session, NULL) != 0) {
                debug_info("Failed to close session for current thread", _trace_level, DEBUG_ERROR);
            }
            _session = nullptr;
            std::cout << "Session deleted" << std::endl;
        }

        delete _thread;
        _thread = nullptr;
    }

    void
    finish()
    {
        _running = false;
        if (_thread != nullptr && _thread->joinable()) {
            _thread->join();
        }
    }

    const std::vector<std::string> &
    get_collection_names() const
    {
        return _collection_names;
    }

    WT_SESSION *
    get_session() const
    {
        return _session;
    }

    thread_operation
    get_thread_operation() const
    {
        return _type;
    }

    bool
    is_running() const
    {
        return _running;
    }

    void
    set_thread(std::thread *thread)
    {
        _thread = thread;
        _running = true;
    }

    private:
    std::vector<std::string> _collection_names;
    bool _running;
    WT_SESSION *_session;
    std::thread *_thread;
    thread_operation _type;
};
} // namespace test_harness

#endif