project(unittest CXX)

Include(FetchContent)

FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v2.13.8
)

FetchContent_MakeAvailable(Catch2)

add_library(unittest_harness STATIC
    tests/extent_list.cxx
)

target_include_directories(unittest_harness PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(
    unittest_harness
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

create_test_executable(unittests
    SOURCES tests/main.cxx
    LIBS unittest_harness
    CXX
)

target_compile_options(
    unittests
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(unittests Catch2::Catch2)
target_link_libraries(unittest_harness Catch2::Catch2)
if(ENABLE_TCMALLOC)
    target_link_libraries(unittests wt::tcmalloc)
endif()

add_test(NAME unittest COMMAND ${CMAKE_CURRENT_BINARY_DIR}/unittests)
set_tests_properties(unittest PROPERTIES LABELS "check;unittest")
