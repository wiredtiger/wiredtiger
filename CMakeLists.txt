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

# Import our available build types prior to initializing the
# project.
include(cmake/configs/modes.cmake)
# Import our helpers.
include(cmake/helpers.cmake)
include(cmake/define_libwiredtiger.cmake)

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

# Import our third party library definitions.
include(cmake/third_party/gperftools.cmake)
include(cmake/third_party/lz4.cmake)
include(cmake/third_party/zstd.cmake)
include(cmake/third_party/snappy.cmake)
include(cmake/third_party/zlib.cmake)
include(cmake/third_party/sodium.cmake)

if(NOT ENABLE_SHARED AND NOT ENABLE_STATIC)
    message(FATAL_ERROR "Both ENABLE_SHARED & ENABLE_STATIC are disabled. Need to enable at least one build flavour.")
endif()

if(ENABLE_STRICT)
    if("${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
        include(cmake/strict/gcc_strict.cmake)
    elseif("${CMAKE_C_COMPILER_ID}" MATCHES "^(Apple)?(C|c?)lang")
        include(cmake/strict/clang_strict.cmake)
    elseif("${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC")
        include(cmake/strict/cl_strict.cmake)
    endif()
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        include(cmake/strict/gxx_strict.cmake)
    elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "^(Apple)?(C|c?)lang")
        include(cmake/strict/clangxx_strict.cmake)
    elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
        include(cmake/strict/clxx_strict.cmake)
    endif()
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

# Generate wiredtiger.h include file.
configure_file(src/include/wiredtiger.in "include/wiredtiger.h" @ONLY)
# Generate our wiredtiger_config.h include file.
configure_file(cmake/configs/wiredtiger_config.h.in "config/wiredtiger_config.h" @ONLY)

# If compiling with fPIC, we can create a intermediate library of
# position independent objects, that both the static and shared builds
# can use. This saving on the cost of recompiling the WiredTiger sources
# two times over.
if(WITH_PIC OR ENABLE_SHARED)
    add_library(wt_objs OBJECT ${wt_sources})
    target_include_directories(wt_objs
        PRIVATE
        ${CMAKE_CURRENT_BINARY_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR}/config
        ${CMAKE_CURRENT_LIST_DIR}/src/include
    )
    # Append any provided C flags.
    if(COMPILER_DIAGNOSTIC_C_FLAGS)
        target_compile_options(wt_objs PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS})
    endif()
    if(ENABLE_TCMALLOC)
        target_include_directories(wt_objs PRIVATE ${HAVE_LIBTCMALLOC_INCLUDES})
    endif()
    if(HAVE_LIBPTHREAD)
        target_include_directories(wt_objs PRIVATE ${HAVE_LIBPTHREAD_INCLUDES})
    endif()
    if(HAVE_LIBRT)
        target_include_directories(wt_objs PRIVATE ${HAVE_LIBRT_INCLUDES})
    endif()
    if(HAVE_LIBDL)
        target_include_directories(wt_objs PRIVATE ${HAVE_LIBDL_INCLUDES})
    endif()
    set_property(TARGET wt_objs PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_STATIC)
    if(WITH_PIC)
        define_wiredtiger_library(wiredtiger_static STATIC
            SOURCES $<TARGET_OBJECTS:wt_objs> ${builtin_objs}
            PUBLIC_INCLUDES ${CMAKE_CURRENT_BINARY_DIR}/include
            PRIVATE_INCLUDES ${CMAKE_CURRENT_BINARY_DIR}/config ${CMAKE_CURRENT_LIST_DIR}/src/include
        )
    else()
        define_wiredtiger_library(wiredtiger_static STATIC
            SOURCES ${wt_sources} ${builtin_objs}
            PUBLIC_INCLUDES ${CMAKE_CURRENT_BINARY_DIR}/include
            PRIVATE_INCLUDES ${CMAKE_CURRENT_BINARY_DIR}/config ${CMAKE_CURRENT_LIST_DIR}/src/include
        )
    endif()
endif()

if(ENABLE_SHARED)
    define_wiredtiger_library(wiredtiger_shared SHARED
        SOURCES $<TARGET_OBJECTS:wt_objs> ${builtin_objs}
        PUBLIC_INCLUDES ${CMAKE_CURRENT_BINARY_DIR}/include
        PRIVATE_INCLUDES ${CMAKE_CURRENT_BINARY_DIR}/config ${CMAKE_CURRENT_LIST_DIR}/src/include
    )
    # Set the SOVERSION property of the wiredtiger library, so we can export the appropriately versioned symlinks.
    set_target_properties(wiredtiger_shared PROPERTIES SOVERSION "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")
endif()

# Define an alias target for the WiredTiger library. This being a general target other
# executables can use to link against. Preference the static library build if available
# otherwise fallback to the shared library. We also namespace (::) the target to
# tell CMake that the target name is associated with an ALIAS target, allowing CMake
# to issue a diagnostic message if the target isn't found on subsequent linking commands.
if (ENABLE_STATIC)
    add_library(wt::wiredtiger ALIAS wiredtiger_static)
else()
    add_library(wt::wiredtiger ALIAS wiredtiger_shared)
endif()

# Build the wt utility.
add_subdirectory(src/utilities)

# Establish our install target configuration.
include(cmake/install/install.cmake)

# Enable CTest. Subsequent targets may additionally define their own ctest definitions.
enable_testing()

if(ENABLE_PYTHON)
    add_subdirectory(lang/python)
    add_subdirectory(bench/workgen)
endif()

# Build the wiredtiger test suites.
add_subdirectory(bench/wtperf)
add_subdirectory(examples)
add_subdirectory(test)
