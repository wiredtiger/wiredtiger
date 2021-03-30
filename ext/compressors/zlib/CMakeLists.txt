#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

project(zlib C)

config_bool(
    HAVE_BUILTIN_EXTENSION_ZLIB
    "Builtin zlib compression library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBZ"
    DEPENDS_ERROR ON "Failed to find zlib library."
)

if (HAVE_BUILTIN_EXTENSION_ZLIB AND ENABLE_ZLIB)
    message(FATAL_ERROR "Only one of 'ENABLE_ZLIB' or 'HAVE_BUILTIN_EXTENSION_ZLIB' can be enabled.")
endif()

set(sources "zlib_compress.c")
set(link_type)
if(HAVE_BUILTIN_EXTENSION_ZLIB)
    if(ENABLE_STATIC)
        set(link_type "STATIC")
    else()
        set(link_type "SHARED")
    endif()
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_ZLIB OR ENABLE_ZLIB)
    add_library(wiredtiger_zlib ${link_type} ${sources})
    target_link_libraries(wiredtiger_zlib "z")
    target_include_directories(
        wiredtiger_zlib
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
        )
endif()
