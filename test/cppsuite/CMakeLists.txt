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
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)
target_link_libraries(cppsuite_test_harness PRIVATE test_util)
# There's no special sub-path to access a given extension.
target_compile_options(cppsuite_test_harness PUBLIC -DEXTSUBPATH="")

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

if(ENABLE_TCMALLOC)
    target_link_libraries(run wt::tcmalloc)
    target_link_libraries(csuite_style_example_test wt::tcmalloc)
endif()

# Test definitions.
add_test(NAME cppsuite COMMAND ${CMAKE_CURRENT_BINARY_DIR}/run)

add_test(NAME csuite_style_example COMMAND csuite_style_example_test)
set_tests_properties(csuite_style_example PROPERTIES LABELS "check;cppsuite")

# Run this during a "ctest check" smoke test.
set_tests_properties(cppsuite PROPERTIES LABELS "check;cppsuite")
