#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

cmake_minimum_required(VERSION 3.11.0)

project(lz4 C)

config_bool(
    HAVE_BUILTIN_EXTENSION_LZ4
    "Builtin lz4 compression library"
    DEFAULT OFF
    DEPENDS "HAVE_LIBLZ4"
    DEPENDS_ERROR ON "Failed to find lz4 library"
)

if (HAVE_BUILTIN_EXTENSION_LZ4 AND ENABLE_LIBLZ4)
    message(FATAL_ERROR "Only one of 'ENABLE_LIBLZ4' or 'HAVE_BUILTIN_EXTENSION_LZ4' can be enabled")
endif()

set(sources "lz4_compress.c")
set(link_type)

if(HAVE_BUILTIN_EXTENSION_LZ4)
    if(ENABLE_STATIC)
        set(link_type "STATIC")
    elseif(HAVE_BUILTIN_EXTENSION_LZ4)
        set(link_type "SHARED")
    endif()
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_LZ4 OR ENABLE_LIBLZ4)
    add_library(wiredtiger_lz4 ${link_type} ${sources})
    target_link_libraries(wiredtiger_lz4 "lz4")
    target_include_directories(
        wiredtiger_lz4
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
        )
endif()
