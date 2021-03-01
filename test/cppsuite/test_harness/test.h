#ifndef TEST_H
#define TEST_H

#include <cinttypes>
#include <thread>
#include <functional>

extern "C" {
    #include "wiredtiger.h"
    #include "wt_internal.h"
}

#include "component.h"
#include "configuration_settings.h"
#include "workload_generator.h"
#include "runtime_monitor.h"
#include "timestamp_manager.h"

namespace test_harness {
class test : public component {
    public:
    test(const std::string &config)
    {
        _configuration = new configuration(_name, config);
        _workload_generator = new workload_generator(_configuration);
        _runtime_monitor = new runtime_monitor();
        _timestamp_manager = new timestamp_manager();
        /*
         * Ordering is not important here, any dependencies between components should be resolved
         * internally by the components.
         */
        _components = {_workload_generator, _timestamp_manager, _runtime_monitor};
    }

    test() = delete;

    ~test()
    {
        delete _configuration;
        _configuration = nullptr;
    }

    void load() {
        for (const auto &it : _components) {
            it->load();
        }
        _loaded = true;
    }

    /*
     * The primary run function that most tests will be able to utilise without much other code.
     */
    void run() {
        int64_t duration_seconds;

        duration_seconds = 0;

        testutil_check(_configuration->get_int(DURATION_SECONDS, duration_seconds));

        if (!_loaded)
            load();

        /* Spawn threads for all component::run() functions. */
        for (const auto &it : _components) {
            thread_context *tc =
              new thread_context(thread_operation::COMPONENT);
            _thread_manager.add_member_function_thread(tc, &component::run, it);
        }

        /* Sleep duration seconds, then end the test. */
        std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));
        finish();
    }

    void finish() {
        for (const auto &it : _components) {
            it->finish();
        }
    }

    configuration *_configuration = nullptr;
    std::vector<component*> _components;
    static const std::string _name;
    static const std::string _default_config;
    bool _loaded = false;
    thread_manager _thread_manager;
    workload_generator *_workload_generator;
    runtime_monitor *_runtime_monitor;
    timestamp_manager *_timestamp_manager;
};
} // namespace test_harness

#endif
