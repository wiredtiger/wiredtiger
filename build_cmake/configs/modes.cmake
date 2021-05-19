#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
#  See the file LICENSE for redistribution information
#

# Establishes build configuration modes we can use when compiling.

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "None" CACHE STRING "Choose the type of build, options are: None Debug Release." FORCE)
endif()

set(CMAKE_CONFIGURATION_TYPES None Debug Release)
