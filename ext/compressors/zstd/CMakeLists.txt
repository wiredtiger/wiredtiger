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

project(zstd C)

include(GNUInstallDirs)

config_bool(
    HAVE_BUILTIN_EXTENSION_ZSTD
    "Builtin zstd compression library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBZSTD"
    DEPENDS_ERROR ON "Failed to find zstd library."
)

if (HAVE_BUILTIN_EXTENSION_ZSTD AND ENABLE_ZSTD)
    message(FATAL_ERROR "Only one of 'ENABLE_ZSTD' or 'HAVE_BUILTIN_EXTENSION_ZSTD' can be enabled.")
endif()

set(sources "zstd_compress.c")
set(link_type)
if(HAVE_BUILTIN_EXTENSION_ZSTD)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_ZSTD OR ENABLE_ZSTD)
    add_library(wiredtiger_zstd ${link_type} ${sources})
    target_include_directories(
        wiredtiger_zstd
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
        )
    target_compile_options(
        wiredtiger_zstd
        PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
    )

    set_property(TARGET wiredtiger_zstd PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_ZSTD)
    # HAVE_LIBZSTD contains the full pathname to the lib, which is
    # what the cmake docs recommend using.
    target_link_libraries(wiredtiger_zstd zstd)
    install(TARGETS wiredtiger_zstd
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
