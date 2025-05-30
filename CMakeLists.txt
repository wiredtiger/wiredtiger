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
include(cmake/gdb_autoloader_setup.cmake)

# If the user doesn't manually specify the target ARCH and OS (i.e not cross-compiling)
# we will infer the target from the host. This is checked prior to the config option
# being defined (in base.cmake) since it will prevent it having a default value.
if(NOT WT_ARCH)
    # Defer to our hosts architecture as our target architecture.
    # PROCESSOR_ARCHITECTURE env variable could be unset on certain Windows systems,
    # in that case map the architecture to x86.
    if ("${CMAKE_HOST_SYSTEM_PROCESSOR}" MATCHES "^(x86_64|AMD64|)$")
        set(WT_ARCH "x86")
    elseif ("${CMAKE_HOST_SYSTEM_PROCESSOR}" MATCHES "^(arm64|aarch64)$")
        set(WT_ARCH "aarch64")
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
include(cmake/third_party/aws_sdk.cmake)
include(cmake/third_party/gcp_sdk.cmake)
include(cmake/third_party/lazyfs.cmake)
include(cmake/third_party/lz4.cmake)
include(cmake/third_party/memkind.cmake)
include(cmake/third_party/snappy.cmake)
include(cmake/third_party/sodium.cmake)
include(cmake/third_party/zlib.cmake)
include(cmake/third_party/zstd.cmake)
include(cmake/third_party/iaa.cmake)
include(cmake/third_party/voidstar.cmake)

# Skip the AZURE SDK build step if the extension is not enabled.
if(ENABLE_AZURE)
    include(cmake/third_party/azure_sdk.cmake)
endif()

if(NOT ENABLE_SHARED AND NOT ENABLE_STATIC)
    message(FATAL_ERROR "Both ENABLE_SHARED & ENABLE_STATIC are disabled. Need to enable at least one build flavour.")
endif()

# Set the C++ 17 standard. Have to use this method to make it consistent across the platforms
# as cmake is doing funny things with the versions we have (including setting -std on MSVC).
if(MSVC)
    set(CMAKE_CXX_STANDARD)
    add_cmake_flag(CMAKE_CXX_FLAGS /std:c++17)
else()
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif()

if(ENABLE_COLORIZE_OUTPUT)
    # Colorize the build error output
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_C_COMPILER_ID}" STREQUAL "GNU")
        add_compile_options(-fdiagnostics-color=always)
    elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_C_COMPILER_ID}" STREQUAL "Clang")
        add_compile_options(-fcolor-diagnostics)
    endif()
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

if(HAVE_UNITTEST)
    Include(FetchContent)

    FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG        v2.13.8
    )

    # Note, the preferred solution of using FetchContent_MakeAvailable()
    # is not available in 3.11.0 (it arrived in 3.11.4), so we do some extra steps to achieve the same result.
    #
    # See https://cmake.org/cmake/help/v3.23/module/FetchContent.html
    #
    # Once we can rely on having 3.11.4 or greater, then the following call can be restored, and the
    # code between the Start and End comments below can be removed.

    # FetchContent_MakeAvailable(Catch2)

    # Start of code that achieves what FetchContent_MakeAvailable(Catch2) would do.
    FetchContent_Populate(Catch2)
    add_subdirectory(${catch2_SOURCE_DIR} ${catch2_BINARY_DIR})
    # End of code that achieves what FetchContent_MakeAvailable(Catch2) would do.
endif()

# Enable CTest. Subsequent targets may additionally define their own ctest definitions.
enable_testing()

# Include the extensions to the build.
add_subdirectory(ext)

# Collect all the library sources we need to compile from the source filelist.
parse_filelist_source(${CMAKE_CURRENT_LIST_DIR}/dist/filelist wt_sources)


if(WT_WIN)
    list(APPEND wt_source ${CMAKE_SOURCE_DIR}/cmake/configs/wiredtiger.def)
    set_source_files_properties(${CMAKE_SOURCE_DIR}/cmake/configs/wiredtiger.def PROPERTIES HEADER_FILE_ONLY TRUE)
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

if(HAVE_BUILTIN_EXTENSION_IAA)
    list(APPEND builtin_objs $<TARGET_OBJECTS:wiredtiger_iaa>)
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
    if(ENABLE_MEMKIND)
        target_include_directories(wt_objs PRIVATE ${HAVE_LIBMEMKIND_INCLUDES})
    endif()
    if(ENABLE_ANTITHESIS)
        target_include_directories(wt_objs PRIVATE ${CMAKE_SOURCE_DIR}/tools/voidstar/include)
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

    setup_gdb_autoloader()

    # For MacOS builds we need to generate a dSYM bundle that contains the debug symbols for the 
    # WiredTiger library.
    if (WT_DARWIN)
        add_custom_command(
            TARGET wiredtiger_shared POST_BUILD
            COMMAND dsymutil libwiredtiger.${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}.dylib
            COMMENT "Running dsymutil on libwiredtiger.${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}.dylib"
            VERBATIM
        )
    endif()
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


if(ENABLE_PYTHON)
    add_subdirectory(lang/python)
    add_subdirectory(bench/workgen)
endif()

# Build the wiredtiger test suites.
add_subdirectory(bench/wtperf)
add_subdirectory(bench/tiered)
add_subdirectory(bench/wt2853_perf)
add_subdirectory(examples)
add_subdirectory(test)
if(ENABLE_LLVM)
    add_subdirectory(tools/xray_to_optrack)
endif()

# Build other tools.
add_subdirectory(tools/checksum_bitflip)
