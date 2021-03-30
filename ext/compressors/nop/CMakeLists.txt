#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

project(nop_compression C)

set(sources "nop_compress.c")
add_library(wiredtiger_nop_compress SHARED ${sources})

target_include_directories(
    wiredtiger_nop_compress
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
