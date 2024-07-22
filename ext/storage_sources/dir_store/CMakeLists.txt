project(dir_store C)

set(sources "dir_store.c")

if("${CMAKE_BUILD_TYPE}" MATCHES "^MSan$")
    list(APPEND sources ${CMAKE_SOURCE_DIR}/src/support/msan_fstat_suppression_wrappers.c)
endif()

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
