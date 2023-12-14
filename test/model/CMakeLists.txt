project(wiredtiger_model CXX)

# Create a model library.
add_library(wiredtiger_model SHARED
    src/core/core.cpp
    src/core/data_value.cpp
    src/core/kv_database.cpp
    src/core/kv_table_item.cpp
    src/core/kv_table.cpp
    src/core/kv_transaction.cpp
    src/core/kv_transaction_snapshot.cpp
    src/core/util.cpp
    src/core/verify.cpp
    src/driver/debug_log_parser.cpp
)

target_include_directories(
    wiredtiger_model
    PUBLIC src/include
    PRIVATE
        ${CMAKE_BINARY_DIR}/config
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_SOURCE_DIR}/test/3rdparty
)

target_include_directories(
    wiredtiger_model
    PUBLIC src/include
    PRIVATE
        ${CMAKE_BINARY_DIR}/config
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_SOURCE_DIR}/test/3rdparty
)

target_compile_options(
    wiredtiger_model
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(wiredtiger_model wt::wiredtiger)

# Build the tests and the tools.
add_subdirectory(test)
add_subdirectory(tools)
