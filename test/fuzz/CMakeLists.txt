#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

project(libfuzz C)

# Import the LLVM package to determine if we have an llvm install
# with the necessary libraries i.e. MacOSX clang SDK installs don't
# provide the libraries by default (would need to use a custom LLVM
# install).
find_package(LLVM CONFIG)
set(libclangfuzzer "")
if(LLVM_PACKAGE_VERSION)
    llvm_map_components_to_libnames(libclangfuzzer fuzzer)
endif()

if(NOT libclangfuzzer)
    # We require clangs libFuzzer to be available to run this test.
    # Avoid building the fuzz test in this case.
    return()
endif()

set(fuzz_c_flags "${COMPILER_DIAGNOSTIC_C_FLAGS};-fsanitize=fuzzer-no-link")

# Compile the fuzz util library.
add_library(fuzz_util SHARED fuzz_util.c)
target_include_directories(
    fuzz_util
    PUBLIC ${CMAKE_BINARY_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}
    PRIVATE
        ${CMAKE_BINARY_DIR}/config
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_SOURCE_DIR}/test/utility
)
target_link_libraries(fuzz_util wt::wiredtiger test_util)
target_link_libraries(fuzz_util "-fsanitize=fuzzer")
target_compile_options(
    fuzz_util
    PRIVATE ${fuzz_c_flags}
)

# Compile the fuzz test programs.
create_test_executable(test_fuzz_modify
    SOURCES modify/fuzz_modify.c
    FLAGS -fsanitize=fuzzer-no-link
    ADDITIONAL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/fuzz_run.sh
    LIBS fuzz_util "-fsanitize=fuzzer"
)
create_test_executable(test_fuzz_config
    SOURCES config/fuzz_config.c
    FLAGS -fsanitize=fuzzer-no-link
    ADDITIONAL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/fuzz_run.sh
    LIBS fuzz_util "-fsanitize=fuzzer"
)

add_test(NAME test_fuzz_modify
    COMMAND bash ${CMAKE_CURRENT_BINARY_DIR}/fuzz_run.sh $<TARGET_FILE:test_fuzz_modify>)
add_test(NAME test_fuzz_config
    COMMAND bash ${CMAKE_CURRENT_BINARY_DIR}/fuzz_run.sh $<TARGET_FILE:test_fuzz_config>)
