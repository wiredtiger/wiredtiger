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

# Create an intermediate static lib.
add_library(cppsuite_test_harness STATIC
    test_harness/core/component.cxx 
    test_harness/core/configuration.cxx 
    test_harness/core/throttle.cxx 
    test_harness/util/api_const.cxx
    test_harness/util/logger.cxx
    test_harness/util/scoped_connection.cxx 
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
)

target_include_directories(cppsuite_test_harness PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(
    cppsuite_test_harness
    PUBLIC -std=c++11
    PRIVATE ${COMPILER_DIAGNOSTIC_CPP_FLAGS}
)
target_link_libraries(cppsuite_test_harness PRIVATE test_util)

# Create the different executables.
create_test_executable(run 
    SOURCES tests/run.cxx
    LIBS cppsuite_test_harness
    ADDITIONAL_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/configs
    CXX
)

# If you prefer to not use the run binary you can add a test via this mechanism but it is generally
# frowned upon.
create_test_executable(csuite_style_example_test 
    SOURCES tests/csuite_style_example_test.cxx
    LIBS cppsuite_test_harness
    CXX
)

if(ENABLE_SNAPPY)
    # We use Snappy compression to avoid excessive disk spage usage.
    target_compile_options(run PRIVATE -DSNAPPY_PATH=\"$<TARGET_FILE:wiredtiger_snappy>\")
endif()

# Test definitions.
add_test(NAME cppsuite COMMAND ${CMAKE_CURRENT_BINARY_DIR}/run)

add_test(NAME csuite_style_example COMMAND csuite_style_example_test)
set_tests_properties(csuite_style_example PROPERTIES LABELS "check")

# Run this during a "ctest check" smoke test.
set_tests_properties(cppsuite PROPERTIES LABELS "check")
