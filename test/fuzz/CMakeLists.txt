project(libfuzz C)

include(CheckCSourceCompiles)

set(CMAKE_REQUIRED_LIBRARIES "-fsanitize=fuzzer")
set(CMAKE_REQUIRED_FLAGS "-fsanitize=fuzzer-no-link")
check_c_source_compiles("
    #include <inttypes.h>
    #include <stddef.h>
    int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
        return 0;
    }
" HAVE_LIBCLANG_FUZZER)
unset(CMAKE_REQUIRED_LIBRARIES)
unset(CMAKE_REQUIRED_FLAGS)

if(NOT HAVE_LIBCLANG_FUZZER)
    # We require clangs libFuzzer to be available to run this test.
    # Avoid building the fuzz test in this case.
    return()
endif()

# We require either a PIC-enabled or shared library build to build
# our dynamic fuzz_util library. Avoid building the fuzz test if not available.
set(wiredtiger_target)
if(ENABLE_STATIC AND WITH_PIC)
    set(wiredtiger_target wiredtiger_static)
elseif(ENABLE_SHARED)
    set(wiredtiger_target wiredtiger_shared)
else()
    message(STATUS "Skipping fuzz test compilation: Requires either a shared library (ENABLE_SHARED)"
                    "or a PIC-enabled (WITH_PIC) build of wiredtiger")
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
target_link_libraries(fuzz_util ${wiredtiger_target} test_util)
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
