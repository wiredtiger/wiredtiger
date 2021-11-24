#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

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
