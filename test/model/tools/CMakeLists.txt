project(model_tools)

include(${CMAKE_SOURCE_DIR}/cmake/helpers.cmake)

# Build the tools.
create_test_executable(model_verify_debug_log
    SOURCES model_verify_debug_log/main.cpp
    LIBS wiredtiger_model wiredtiger_model_test_common
    CXX
)
