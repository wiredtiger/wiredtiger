#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

cmake_minimum_required(VERSION 3.11.0)

project(fail_fs C)

set(sources "fail_fs.c")
add_library(wiredtiger_fail_fs MODULE ${sources})

target_include_directories(
    wiredtiger_fail_fs
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
