#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

cmake_minimum_required(VERSION 3.11.0)

project(WiredTiger C ASM)

include(build_cmake/helpers.cmake)

# If the user doesn't manually specify the target ARCH and OS (i.e not cross-compiling)
# we will infer the target from the host
if(NOT WT_ARCH)
    # Defer to our hosts architecture as our target architecture
    if ("${CMAKE_HOST_SYSTEM_PROCESSOR}" MATCHES "^(x86_64|i686|i386)$")
        set(WT_ARCH "x86")
    else()
        set(WT_ARCH "${CMAKE_HOST_SYSTEM_PROCESSOR}")
    endif()
endif()
if(NOT WT_OS)
    # Defer to our hosts OS as our target OS
    string(TOLOWER "${CMAKE_HOST_SYSTEM_NAME}" host_os)
    set(WT_OS "${host_os}")
endif()

if(NOT EXISTS "${CMAKE_CURRENT_LIST_DIR}/build_cmake/configs/${WT_ARCH}/${WT_OS}/config.cmake")
    message(FATAL_ERROR "build_cmake/configs/${WT_ARCH}/${WT_OS}/config.cmake does not exist")
endif()

# Load any configuration variables special to our target
include(build_cmake/configs/${WT_ARCH}/${WT_OS}/config.cmake)
# Load auto-configure variables (e.g. environment-related configs)
include(build_cmake/configs/auto.cmake)
# Load WiredTiger related configuration options
include(build_cmake/configs/base.cmake)

if(ENABLE_STRICT)
    if("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
        include(build_cmake/strict/gcc_strict.cmake)
    elseif("${CMAKE_C_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_C_COMPILER_ID}" STREQUAL "AppleClang")
        include(build_cmake/strict/clang_strict.cmake)
    endif()
endif()

set(link_type "STATIC")
if(NOT ENABLE_STATIC)
    set(link_type "SHARED")
    SET_PROPERTY(GLOBAL PROPERTY TARGET_SUPPORTS_SHARED_LIBS TRUE)
endif()

# Include the extensions to the build
add_subdirectory(ext)

# Collect all the library sources we need to compile from the source filelist
parse_filelist_source(${CMAKE_CURRENT_LIST_DIR}/dist/filelist wt_sources)
# Establish wiredtiger library target
add_library(wiredtiger ${link_type} ${wt_sources})

# Generate wiredtiger.h
configure_file(src/include/wiredtiger.in "include/wiredtiger.h" @ONLY)
# Generate our wiredtiger_config.h
configure_file(build_cmake/configs/wiredtiger_config.h.in "config/wiredtiger_config.h" @ONLY)
# Set our targets public and private includes
target_include_directories(
    wiredtiger
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/include
    PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/config ${CMAKE_CURRENT_LIST_DIR}/src/include
)

# Ensure we link any available library dependencies to our wiredtiger target
if(HAVE_LIBPTHREAD)
    target_link_libraries(wiredtiger "pthread")
endif()

if(HAVE_LIBRT)
    target_link_libraries(wiredtiger "rt")
endif()

if(HAVE_LIBDL)
    target_link_libraries(wiredtiger "dl")
endif()

if(HAVE_BUILTIN_EXTENSION_LZ4)
    target_link_libraries(wiredtiger wiredtiger_lz4)
endif()

if(HAVE_BUILTIN_EXTENSION_SNAPPY)
    target_link_libraries(wiredtiger wiredtiger_snappy)
endif()

if(HAVE_BUILTIN_EXTENSION_ZLIB)
    target_link_libraries(wiredtiger wiredtiger_zlib)
endif()

if(HAVE_BUILTIN_EXTENSION_ZSTD)
    target_link_libraries(wiredtiger wiredtiger_zstd)
endif()
