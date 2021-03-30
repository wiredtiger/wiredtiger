#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

project(snappy C)

config_bool(
    HAVE_BUILTIN_EXTENSION_SNAPPY
    "Builtin snappy compression library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBSNAPPY"
    DEPENDS_ERROR ON "Failed to find snappy library."
)

if (HAVE_BUILTIN_EXTENSION_SNAPPY AND ENABLE_SNAPPY)
    message(FATAL_ERROR "Only one of 'ENABLE_SNAPPY' or 'HAVE_BUILTIN_EXTENSION_SNAPPY' can be enabled.")
endif()

set(sources "snappy_compress.c")
set(link_type)
if(HAVE_BUILTIN_EXTENSION_SNAPPY)
    if(ENABLE_STATIC)
        set(link_type "STATIC")
    else()
        set(link_type "SHARED")
    endif()
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_SNAPPY OR ENABLE_SNAPPY)
    add_library(wiredtiger_snappy ${link_type} ${sources})
    target_link_libraries(wiredtiger_snappy "snappy")
    target_include_directories(
        wiredtiger_snappy
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
        )
endif()
