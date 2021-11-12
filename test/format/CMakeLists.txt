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

project(test_format C)

set(format_sources
    alter.c
    backup.c
    bulk.c
    checkpoint.c
    compact.c
    config.c
    config_compat.c
    config_def.c
    hs.c
    import.c
    kv.c
    ops.c
    random.c
    salvage.c
    snap.c
    t.c
    trace.c
    ts.c
    util.c
    verify.c
    wts.c
)

create_test_executable(test_format
    SOURCES ${format_sources}
    EXECUTABLE_NAME "t"
    ADDITIONAL_FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/CONFIG.stress
        ${CMAKE_CURRENT_SOURCE_DIR}/format.sh
        ${CMAKE_CURRENT_SOURCE_DIR}/smoke.sh
)

if(ENABLE_LZ4)
    target_compile_options(test_format PRIVATE -DLZ4_PATH=\"$<TARGET_FILE:wiredtiger_lz4>\")
endif()

if(ENABLE_SNAPPY)
    target_compile_options(test_format PRIVATE -DSNAPPY_PATH=\"$<TARGET_FILE:wiredtiger_snappy>\")
endif()

if(ENABLE_SODIUM)
    target_compile_options(test_format PRIVATE -DSODIUM_PATH=\"$<TARGET_FILE:wiredtiger_sodium>\")
endif()

if(ENABLE_ZLIB)
    target_compile_options(test_format PRIVATE -DZLIB_PATH=\"$<TARGET_FILE:wiredtiger_zlib>\")
endif()

if(ENABLE_ZSTD)
    target_compile_options(test_format PRIVATE -DZSTD_PATH=\"$<TARGET_FILE:wiredtiger_zstd>\")
endif()
target_compile_options(test_format PRIVATE -DREVERSE_PATH=\"$<TARGET_FILE:wiredtiger_reverse_collator>\")
target_compile_options(test_format PRIVATE -DROTN_PATH=\"$<TARGET_FILE:wiredtiger_rotn_encrypt>\")

add_test(NAME test_format COMMAND ${CMAKE_CURRENT_BINARY_DIR}/smoke.sh)
set_tests_properties(test_format PROPERTIES LABELS "check")
