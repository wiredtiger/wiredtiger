project(fail_fs C)

set(sources "fail_fs.c")
add_library(wiredtiger_fail_fs MODULE ${sources})

target_include_directories(
    wiredtiger_fail_fs
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
	${CMAKE_SOURCE_DIR}/test/utility
)
target_compile_options(
    wiredtiger_fail_fs
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
target_link_libraries(wiredtiger_fail_fs test_util)
