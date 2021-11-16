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

project(snappy C)

include(GNUInstallDirs)

config_bool(
    HAVE_BUILTIN_EXTENSION_SNAPPY
    "Builtin snappy compression library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBSNAPPY"
    DEPENDS_ERROR ON "Failed to find snappy library."
)

if (HAVE_BUILTIN_EXTENSION_SNAPPY AND ENABLE_SNAPPY)
    message(FATAL_ERROR "Only one of 'ENABLE_SNAPPY' or 'HAVE_BUILTIN_EXTENSION_SNAPPY' can be enabled.")
endif()

set(sources "snappy_compress.c")
set(link_type)
if(HAVE_BUILTIN_EXTENSION_SNAPPY)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_SNAPPY OR ENABLE_SNAPPY)
    add_library(wiredtiger_snappy ${link_type} ${sources})
    target_include_directories(
        wiredtiger_snappy
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
            ${HAVE_LIBSNAPPY_INCLUDES}
        )
    target_compile_options(
        wiredtiger_snappy
        PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
    )
    set_property(TARGET wiredtiger_snappy PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_SNAPPY)
    target_link_libraries(wiredtiger_snappy wt::snappy)
    install(TARGETS wiredtiger_snappy
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
