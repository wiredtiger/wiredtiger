project(checksum_bitflip C)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

create_test_executable(checksum_bitflip
    SOURCES
        checksum_bitflip.c
)
