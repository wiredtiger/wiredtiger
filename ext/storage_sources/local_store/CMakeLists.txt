project(local_store C)

set(sources "local_store.c")
add_library(wiredtiger_local_store MODULE ${sources})

target_include_directories(
    wiredtiger_local_store
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
target_compile_options(
    wiredtiger_local_store
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
