#ifndef THREAD_MANAGER_H
#define THREAD_MANAGER_H

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
enum thread_operation { INSERT, UPDATE, READ, REMOVE, CHECKPOINT, TIMESTAMP, MONITOR };
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

    void
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

/*
 * TODO: I'd like to make this template class T implements thread_context? Then we can clean up in
 * the thread manager.
 */
template <class T> class thread_manager {
    public:
    template <class Function>
    void
    create_thread(Function &&fn, T &tc)
    {
        std::thread &t = *new std::thread(fn, std::ref(tc));
        tc.add_thread(t);
        thread_workers.insert(&tc);
    }

    /* Keep our own set of thread_workers to clean them up later. */
    std::set<T *> thread_workers;
    thread_manager() {}
};
} // namespace test_harness

#endif
