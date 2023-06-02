project(cppsuite CXX)

# Create an intermediate static lib.
add_library(cppsuite_test_harness STATIC
    src/bound/bound.cpp
    src/bound/bound_set.cpp
    src/common/constants.cpp
    src/common/logger.cpp
    src/common/random_generator.cpp
    src/common/thread_manager.cpp
    src/component/component.cpp
    src/component/metrics_monitor.cpp
    src/component/metrics_writer.cpp
    src/component/operation_tracker.cpp
    src/component/statistics/cache_limit.cpp
    src/component/statistics/database_size.cpp
    src/component/statistics/statistics.cpp
    src/component/timestamp_manager.cpp
    src/component/workload_manager.cpp
    src/main/collection.cpp
    src/main/configuration.cpp
    src/main/database.cpp
    src/main/database_operation.cpp
    src/main/operation_configuration.cpp
    src/main/test.cpp
    src/main/thread_worker.cpp
    src/main/transaction.cpp
    src/main/validator.cpp
    src/storage/connection_manager.cpp
    src/storage/scoped_cursor.cpp
    src/storage/scoped_session.cpp
    src/util/barrier.cpp
    src/util/execution_timer.cpp
)

target_include_directories(cppsuite_test_harness PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(
    cppsuite_test_harness
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)
target_link_libraries(cppsuite_test_harness PRIVATE test_util)
# There's no special sub-path to access a given extension.
target_compile_options(cppsuite_test_harness PUBLIC -DEXTSUBPATH="")

# Create the different executables.
create_test_executable(run
    SOURCES tests/run.cpp
    LIBS cppsuite_test_harness
    ADDITIONAL_DIRECTORIES ${CMAKE_CURRENT_SOURCE_DIR}/configs
    CXX
)

if(ENABLE_ANTITHESIS)
    target_link_libraries(run wt::voidstar)
endif()

# Test definitions.
add_test(NAME cppsuite COMMAND ${CMAKE_CURRENT_BINARY_DIR}/run)

# Run this during a "ctest check" smoke test.
set_tests_properties(cppsuite PROPERTIES LABELS "check;cppsuite")
