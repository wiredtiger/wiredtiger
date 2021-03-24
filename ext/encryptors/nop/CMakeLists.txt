#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

cmake_minimum_required(VERSION 3.11.0)

project(nop_encrypt C)

set(sources "nop_encrypt.c")
add_library(wiredtiger_nop_encrypt MODULE ${sources})

target_include_directories(
    wiredtiger_nop_encrypt
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
