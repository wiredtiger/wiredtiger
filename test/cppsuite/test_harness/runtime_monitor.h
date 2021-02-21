#ifndef RUNTIME_MONITOR_H
#define RUNTIME_MONITOR_H

#include <cstdint>
#include <map>
#include <vector>

#include "thread_manager.h"

namespace test_harness {
class runtime_monitor {
    public:
    runtime_monitor()
    {
        thread_context &tc = *new thread_context(thread_operation::MONITOR);
        _thread_manager.create_thread(monitor, tc);
    }

    ~runtime_monitor()
    {
        for (const auto &it : _thread_manager.thread_workers) {
            it->join();
            delete it;
        }
    }

    private:
    static void
    monitor(thread_context &context)
    {
        while (context.running()) {
            /* Junk operation to demonstrate thread_contexts. */
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    thread_manager<thread_context> _thread_manager;
};
} // namespace test_harness

#endif
