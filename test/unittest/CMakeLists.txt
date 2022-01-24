project(unittest CXX)

Include(FetchContent)

FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v2.13.8
)

FetchContent_MakeAvailable(Catch2)

create_test_executable(unittests
    SOURCES tests/main.cxx
    CXX
)

target_compile_options(
    unittests
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(unittests Catch2::Catch2)
if(ENABLE_TCMALLOC)
    target_link_libraries(unittests wt::tcmalloc)
endif()

add_test(NAME unittest COMMAND ${CMAKE_CURRENT_BINARY_DIR}/unittests)
set_tests_properties(unittest PROPERTIES LABELS "check;unittest")
