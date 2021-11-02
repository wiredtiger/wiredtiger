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

project(lz4 C)

include(GNUInstallDirs)

config_bool(
    HAVE_BUILTIN_EXTENSION_LZ4
    "Builtin lz4 compression library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBLZ4"
    DEPENDS_ERROR ON "Failed to find lz4 library."
)

if (HAVE_BUILTIN_EXTENSION_LZ4 AND ENABLE_LZ4)
    message(FATAL_ERROR "Only one of 'ENABLE_LZ4' or 'HAVE_BUILTIN_EXTENSION_LZ4' can be enabled.")
endif()

set(sources "lz4_compress.c")
set(link_type)

if(HAVE_BUILTIN_EXTENSION_LZ4)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_LZ4 OR ENABLE_LZ4)
    add_library(wiredtiger_lz4 ${link_type} ${sources})
    target_include_directories(
        wiredtiger_lz4
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
            ${HAVE_LIBLZ4_INCLUDES}
        )
    target_compile_options(
        wiredtiger_lz4
        PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
    )

    set_property(TARGET wiredtiger_lz4 PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_LZ4)
    target_link_libraries(wiredtiger_lz4 wt::lz4)
    install(TARGETS wiredtiger_lz4
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )

endif()
