#ifndef THREAD_HELPER_H
#define THREAD_HELPER_H

#include <chrono>
#include <functional>
#include <thread>
#include <set>

#include "configuration_settings.h"
/*
 * Class to support threading in the test harness. Primarily used to ease creation and deletion of
 * threads.
 */

namespace test_harness {
enum thread_operation { INSERT, UPDATE, READ, REMOVE, CHECKPOINT, TIMESTAMP, UNINITIALIZED };
class thread_context {
    public:
    void
    join()
    {
        _running = false;
        if (_thread != nullptr)
            _thread->join();
    }

    bool
    running()
    {
        return _running;
    }

    /* We need at least 1 virtual fuction to enable polymorphism. */
    virtual void
    add_thread(std::thread &thread)
    {
        _thread = &thread;
    }

    thread_context(thread_operation type) : _type(type), _running(true), _thread(nullptr) {}

    ~thread_context()
    {
        delete _thread;
    }

    private:
    bool _running;
    std::thread *_thread;
    thread_operation _type;
};

class thread_manager {
    public:
    /* No copies of the singleton allowed. */
    thread_manager(thread_manager const &) = delete;
    void operator=(thread_manager const &) = delete;

    template <class Function>
    void
    create_thread(Function &&fn, thread_context &tc)
    {
        std::thread &t = *new std::thread(fn, std::ref(tc));
        tc.add_thread(t);
        _thread_workers.insert(&tc);
        _thread_count++;
    }

    static thread_manager &
    get_instance()
    {
        static thread_manager _instance;
        return _instance;
    }

    private:
    thread_manager() {}

    ~thread_manager()
    {
        for (const auto &it : _thread_workers) {
            if (it->running())
                it->join();
        }
    }

    size_t _thread_count;
    /* Keep our own set of thread_workers to clean them up later. */
    std::set<thread_context *> _thread_workers;
};
} // namespace test_harness

#endif
