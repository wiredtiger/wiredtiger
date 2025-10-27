# Check compilers for C++20 support
if(GNU_CXX_COMPILER)
    set(CXX_COMPILER_MIN_VERSION 13)
elseif(CLANG_CXX_COMPILER)
    set(CXX_COMPILER_MIN_VERSION 17)
elseif(MSVC_CXX_COMPILER)
    set(CXX_COMPILER_MIN_VERSION 16.10)
else()
    set(CXX_COMPILER_MIN_VERSION ${CMAKE_CXX_COMPILER_VERSION})
    message(WARNING "Unknown C++ compiler; assuming it supports C++20")
endif()

if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS CXX_COMPILER_MIN_VERSION)
    message(WARNING "Skipping PALite build: C++ compiler does not support C++20."
        "Required version: ${CXX_COMPILER_MIN_VERSION}; found: ${CMAKE_CXX_COMPILER_VERSION}")
    return()
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(wiredtiger_palite MODULE
    "palite.cpp"
)

target_link_libraries(
    wiredtiger_palite
    PRIVATE
        wt::sqlite3
)

target_include_directories(
    wiredtiger_palite
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)

target_compile_options(
    wiredtiger_palite
    PRIVATE
        ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

# For MacOS builds we need to generate a dSYM bundle that contains the debug
# symbols for each executable. The name of the binary will either be the name
# of the target or some other name passed to this function. We need to use the
# correct one for the dsymutil.
if (WT_DARWIN)
    add_custom_command(
        TARGET wiredtiger_palite POST_BUILD
        COMMAND dsymutil $<TARGET_FILE:wiredtiger_palite>
        WORKING_DIRECTORY $<TARGET_FILE_DIR:wiredtiger_palite>
        COMMENT "Running dsymutil on wiredtiger_palite"
        VERBATIM
    )
endif()
