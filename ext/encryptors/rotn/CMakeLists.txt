#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

cmake_minimum_required(VERSION 3.11.0)

project(rotn_encrypt C)

set(sources "rotn_encrypt.c")
add_library(wiredtiger_rotn_encrypt MODULE ${sources})

target_include_directories(
    wiredtiger_rotn_encrypt
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
