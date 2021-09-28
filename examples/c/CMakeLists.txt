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

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

set(c_tests)

define_c_test(
    TARGET ex_access
    SOURCES ex_access.c
    DIR_NAME ex_access
)

define_c_test(
    TARGET ex_all
    SOURCES ex_all.c
    DIR_NAME ex_all
)

define_c_test(
    TARGET ex_backup
    SOURCES ex_backup.c
    DIR_NAME ex_backup
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET ex_backup_block
    SOURCES ex_backup_block.c
    DIR_NAME ex_backup_block
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET ex_call_center
    SOURCES ex_call_center.c
    DIR_NAME ex_call_center
)

define_c_test(
    TARGET ex_col_store
    SOURCES ex_col_store.c
    DIR_NAME ex_col_store
)

define_c_test(
    TARGET ex_config_parse
    SOURCES ex_config_parse.c
    DIR_NAME ex_config_parse
)

define_c_test(
    TARGET ex_cursor
    SOURCES ex_cursor.c
    DIR_NAME ex_cursor
)

define_c_test(
    TARGET ex_data_source
    SOURCES ex_data_source.c
    DIR_NAME ex_data_source
)

define_c_test(
    TARGET ex_encrypt
    SOURCES ex_encrypt.c
    DIR_NAME ex_encrypt
)

define_c_test(
    TARGET ex_event_handler
    SOURCES ex_event_handler.c
    DIR_NAME ex_event_handler
)
define_c_test(
    TARGET ex_extending
    SOURCES ex_extending.c
    DIR_NAME ex_extending
)

define_c_test(
    TARGET ex_extractor
    SOURCES ex_extractor.c
    DIR_NAME ex_extractor
)

define_c_test(
    TARGET ex_file_system
    SOURCES ex_file_system.c
    DIR_NAME ex_file_system
)

define_c_test(
    TARGET ex_hello
    SOURCES ex_hello.c
    DIR_NAME ex_hello
)

define_c_test(
    TARGET ex_log
    SOURCES ex_log.c
    DIR_NAME ex_log
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET ex_pack
    SOURCES ex_pack.c
    DIR_NAME ex_pack
)

define_c_test(
    TARGET ex_process
    SOURCES ex_process.c
    DIR_NAME ex_process
)

define_c_test(
    TARGET ex_schema
    SOURCES ex_schema.c
    DIR_NAME ex_schema
)

define_c_test(
    TARGET ex_smoke
    SOURCES ex_smoke.c
    DIR_NAME ex_smoke
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET ex_stat
    SOURCES ex_stat.c
    DIR_NAME ex_stat
)

define_c_test(
    TARGET ex_thread
    SOURCES ex_thread.c
    DIR_NAME ex_thread
)

if(NOT "${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC")
    # On GCC and Clang built targets we want to pass the '-rdynamic' linker option.
    target_link_libraries(ex_encrypt "-rdynamic")
    target_link_libraries(ex_file_system "-rdynamic")
endif()

# Run this during a "ctest check" or "ctest examples" smoke test
set_tests_properties(${c_tests} PROPERTIES LABELS "check;examples")
