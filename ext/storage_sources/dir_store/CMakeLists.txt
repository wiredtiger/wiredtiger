project(dir_store C)

set(sources "dir_store.c")
add_library(wiredtiger_dir_store MODULE ${sources})

target_include_directories(
    wiredtiger_dir_store
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
target_compile_options(
    wiredtiger_dir_store
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
