#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
#  See the file LICENSE for redistribution information
#

cmake_minimum_required(VERSION 3.11.0)

set(WT_ARCH "x86" CACHE STRING "")
set(WT_OS "linux" CACHE STRING "")
set(WT_POSIX ON CACHE BOOL "")

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_GNU_SOURCE" CACHE STRING "" FORCE)

# Linux requires buffers aligned to 4KB boundaries for O_DIRECT to work.
set(WT_BUFFER_ALIGNMENT_DEFAULT "4096" CACHE STRING "")
