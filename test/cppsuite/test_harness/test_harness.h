/* Include guard. */
#ifndef TEST_HARNESS_H
#define TEST_HARNESS_H

/* Required to build using older versions of g++. */
#include <cinttypes>

/* Include various wiredtiger libs. */
#include "wiredtiger.h"
#include "wt_internal.h"

#include "configuration_settings.h"

namespace test_harness {
class test {
    public:
    /*
     * All tests will implement this initially, the return value from it will indicate whether the
     * test was successful or not.
     */
    virtual int run() = 0;
    std::string _config;
    configuration *_configuration;
    test(std::string config) : _config(config)
    {
        _configuration = new configuration(config.c_str());
    }
};
} // namespace test_harness

#endif
