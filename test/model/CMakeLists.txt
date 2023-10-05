project(wiredtiger_model CXX)

# Create a model library.
add_library(wiredtiger_model SHARED
    src/core/data_value.cpp
    src/core/kv_table_item.cpp
    src/core/kv_table.cpp
    src/core/verify.cpp
)
target_include_directories(wiredtiger_model PUBLIC src/include)

target_compile_options(
    wiredtiger_model
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(wiredtiger_model wt::wiredtiger)

# Build tests.
add_subdirectory(test)
