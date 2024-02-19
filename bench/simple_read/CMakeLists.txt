project(simple_read CXX)

include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

create_test_executable(simple_read
    SOURCES simple_read.cpp cxxopts.hpp
    EXECUTABLE_NAME "simple_read"
    CXX
)

target_compile_options(
    simple_read
    PUBLIC -std=c++17 -Wno-missing-declarations -Wno-unused-const-variable -Wno-double-promotion
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)