#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

cmake_minimum_required(VERSION 3.11.0)

project(zstd C)

config_bool(
    HAVE_BUILTIN_EXTENSION_ZSTD
    "Builtin zstd compression library"
    DEFAULT OFF
    DEPENDS "HAVE_LIBZSTD"
    DEPENDS_ERROR ON "Failed to find zstd library"
)

if (HAVE_BUILTIN_EXTENSION_ZSTD AND ENABLE_LIBZSTD)
    message(FATAL_ERROR "Only one of 'ENABLE_LIBZSTD' or 'HAVE_BUILTIN_EXTENSION_ZSTD' can be enabled")
endif()

set(sources "zstd_compress.c")
if(HAVE_BUILTIN_EXTENSION_LZ4)
    if(ENABLE_STATIC)
        set(link_type "STATIC")
    elseif(HAVE_BUILTIN_EXTENSION_LZ4)
        set(link_type "SHARED")
    endif()
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_ZSTD OR ENABLE_LIBZSTD)
    add_library(wiredtiger_zstd ${link_type} ${sources})
    target_link_libraries(wiredtiger_zstd "zstd")
    target_include_directories(
        wiredtiger_zstd
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
        )
endif()
