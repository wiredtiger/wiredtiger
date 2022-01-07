project(libtest C)

set(sources
    misc.c
    modify.c
    parse_opts.c
    thread.c
)

set(link_type)
add_library(test_util STATIC ${sources})
set_property(TARGET test_util PROPERTY POSITION_INDEPENDENT_CODE ON)

target_include_directories(
    test_util
    PUBLIC
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_BINARY_DIR}/config
        ${CMAKE_SOURCE_DIR}/src/include
)
target_link_libraries(test_util PRIVATE wt::wiredtiger)

if(WT_WIN)
    target_include_directories(
        test_util
        PUBLIC ${CMAKE_SOURCE_DIR}/test/windows
    )
    target_link_libraries(test_util PRIVATE windows_shim)
endif()

target_compile_options(
    test_util
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
