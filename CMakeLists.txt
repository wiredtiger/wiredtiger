#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

cmake_minimum_required(VERSION 3.10.0)

# Try to use CCACHE if available - speeds up build times.
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

project(WiredTiger C CXX ASM)

# Import our available build types prior to initialing the
# project.
include(cmake/configs/modes.cmake)
include(cmake/helpers.cmake)
# Import third party library definitions.
include(cmake/third_party/gperftools.cmake)
include(cmake/third_party/lz4.cmake)
include(cmake/third_party/zstd.cmake)
include(cmake/third_party/snappy.cmake)
include(cmake/third_party/zlib.cmake)
include(cmake/third_party/sodium.cmake)

# If the user doesn't manually specify the target ARCH and OS (i.e not cross-compiling)
# we will infer the target from the host.
if(NOT WT_ARCH)
    # Defer to our hosts architecture as our target architecture.
    if ("${CMAKE_HOST_SYSTEM_PROCESSOR}" MATCHES "^(x86_64|i686|i386|AMD64)$")
        set(WT_ARCH "x86")
    else()
        set(WT_ARCH "${CMAKE_HOST_SYSTEM_PROCESSOR}")
    endif()
endif()
if(NOT WT_OS)
    # Defer to our hosts OS as our target OS.
    string(TOLOWER "${CMAKE_HOST_SYSTEM_NAME}" host_os)
    set(WT_OS "${host_os}")
endif()

if(NOT EXISTS "${CMAKE_CURRENT_LIST_DIR}/cmake/configs/${WT_ARCH}/${WT_OS}/config.cmake")
    message(FATAL_ERROR "cmake/configs/${WT_ARCH}/${WT_OS}/config.cmake does not exist")
endif()

# Load any configuration variables special to our target.
include(cmake/configs/${WT_ARCH}/${WT_OS}/config.cmake)
# Load auto-configure variables (e.g. environment-related configs).
include(cmake/configs/auto.cmake)
# Load WiredTiger related configuration options.
include(cmake/configs/base.cmake)

if(ENABLE_STRICT)
    if("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
        include(cmake/strict/gcc_strict.cmake)
    elseif("${CMAKE_C_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_C_COMPILER_ID}" STREQUAL "AppleClang")
        include(cmake/strict/clang_strict.cmake)
    elseif("${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC")
        include(cmake/strict/cl_strict.cmake)
    endif()
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        include(cmake/strict/gxx_strict.cmake)
    elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_C_COMPILER_ID}" STREQUAL "AppleClang")
        include(cmake/strict/clangxx_strict.cmake)
    elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
        include(cmake/strict/clxx_strict.cmake)
    endif()
endif()

set(link_type)
if(ENABLE_STATIC)
    set(link_type "STATIC")
else()
    set(link_type "SHARED")
    set_property(GLOBAL PROPERTY TARGET_SUPPORTS_SHARED_LIBS TRUE)
endif()

# Include the extensions to the build.
add_subdirectory(ext)

# Collect all the library sources we need to compile from the source filelist.
parse_filelist_source(${CMAKE_CURRENT_LIST_DIR}/dist/filelist wt_sources)
if(WT_WIN)
    list(APPEND wt_source ${CMAKE_SOURCE_DIR}/build_win/wiredtiger.def)
    set_source_files_properties(${CMAKE_SOURCE_DIR}/build_win/wiredtiger.def PROPERTIES HEADER_FILE_ONLY TRUE)
endif()

set(builtin_objs)
if(HAVE_BUILTIN_EXTENSION_LZ4)
    list(APPEND builtin_objs $<TARGET_OBJECTS:wiredtiger_lz4>)
endif()

if(HAVE_BUILTIN_EXTENSION_SNAPPY)
    list(APPEND builtin_objs $<TARGET_OBJECTS:wiredtiger_snappy>)
endif()

if(HAVE_BUILTIN_EXTENSION_SODIUM)
    list(APPEND builtin_objs $<TARGET_OBJECTS:wiredtiger_sodium>)
endif()

if(HAVE_BUILTIN_EXTENSION_ZLIB)
    list(APPEND builtin_objs $<TARGET_OBJECTS:wiredtiger_zlib>)
endif()

if(HAVE_BUILTIN_EXTENSION_ZSTD)
    list(APPEND builtin_objs $<TARGET_OBJECTS:wiredtiger_zstd>)
endif()

# Establish wiredtiger library target.
add_library(wiredtiger ${link_type} ${wt_sources} ${builtin_objs})

# Generate wiredtiger.h include file.
configure_file(src/include/wiredtiger.in "include/wiredtiger.h" @ONLY)
# Generate our wiredtiger_config.h include file.
configure_file(cmake/configs/wiredtiger_config.h.in "config/wiredtiger_config.h" @ONLY)
# Set our targets public and private includes.
target_include_directories(
    wiredtiger
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/include
    PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/config ${CMAKE_CURRENT_LIST_DIR}/src/include
)
target_compile_options(
    wiredtiger
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)

if(ENABLE_STATIC AND NOT ENABLE_PYTHON)
    # Avoid compiling with fPIC for static builds, this can often lead to
    # GCC disabling many optimizations e.g. inlining, possibly introducing
    # an overhead. We additionally can't turn off fPIC if we are compiling
    # with python support as the python API library depends on shared library
    # support.
    set_property(TARGET wiredtiger PROPERTY POSITION_INDEPENDENT_CODE OFF)
else()
    set_property(TARGET wiredtiger PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

# Ensure we link any available library dependencies to our wiredtiger target.
if(HAVE_LIBPTHREAD)
    target_link_libraries(wiredtiger "pthread")
endif()

if(HAVE_LIBRT)
    target_link_libraries(wiredtiger "rt")
endif()

if(HAVE_LIBDL)
    target_link_libraries(wiredtiger "dl")
endif()

if(ENABLE_TCMALLOC)
    target_link_libraries(wiredtiger gperftools_tcmalloc)
endif()

# We want to capture any transitive dependencies associated with the builtin library
# target and ensure we are explicitly linking the 3rd party libraries.
if(HAVE_BUILTIN_EXTENSION_LZ4)
    target_link_libraries(wiredtiger lz4)
endif()

if(HAVE_BUILTIN_EXTENSION_SNAPPY)
    target_link_libraries(wiredtiger snappy)
endif()

if(HAVE_BUILTIN_EXTENSION_SODIUM)
    target_link_libraries(wiredtiger sodium)
endif()

if(HAVE_BUILTIN_EXTENSION_ZLIB)
    target_link_libraries(wiredtiger zlib)
endif()

if(HAVE_BUILTIN_EXTENSION_ZSTD)
    target_link_libraries(wiredtiger zstd)
endif()

# Build the wt utility.
add_subdirectory(src/utilities)

# Establish our install target configuration.
include(cmake/install/install.cmake)

if(ENABLE_PYTHON)
   add_subdirectory(lang/python)
endif()

# Build the wiredtiger test suites.
enable_testing()
add_subdirectory(test)
add_subdirectory(examples)
add_subdirectory(bench/wtperf)
