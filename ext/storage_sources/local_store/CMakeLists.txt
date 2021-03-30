#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

project(local_store C)

set(sources "local_store.c")
add_library(wiredtiger_local_store MODULE ${sources})

target_include_directories(
    wiredtiger_local_store
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
