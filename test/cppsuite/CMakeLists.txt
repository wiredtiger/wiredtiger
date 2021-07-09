#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

project(cppsuite CXX)

create_test_executable(run
    SOURCES test_harness/core/component.cxx 
            test_harness/core/configuration.cxx 
            test_harness/core/throttle.cxx 
            test_harness/util/debug_utils.cxx
            test_harness/util/scoped_types.cxx 
            test_harness/workload/database_model.cxx 
            test_harness/workload/database_operation.cxx 
            test_harness/workload/random_generator.cxx 
            test_harness/workload/thread_context.cxx 
            test_harness/workload/workload_tracking.cxx 
            test_harness/workload/workload_validation.cxx 
            test_harness/checkpoint_manager.cxx 
            test_harness/connection_manager.cxx 
            test_harness/runtime_monitor.cxx 
            test_harness/test.cxx 
            test_harness/thread_manager.cxx 
            test_harness/timestamp_manager.cxx 
            test_harness/workload_generator.cxx 
            tests/run.cxx
    FLAGS -std=c++11
    INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}
    ADDITIONAL_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/configs
    CXX
)

add_test(NAME cppsuite COMMAND ${CMAKE_CURRENT_BINARY_DIR}/run)

# Run this during a "ctest check" smoke test.
set_tests_properties(cppsuite PROPERTIES LABELS "check")
