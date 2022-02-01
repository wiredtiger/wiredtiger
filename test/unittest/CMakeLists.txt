project(unittest CXX)

Include(FetchContent)

FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG        v2.13.8
)

FetchContent_MakeAvailable(Catch2)

set(unittest_sources
    tests/main.cxx
    tests/extent_list.cxx
)

create_test_executable(unittests
    SOURCES ${unittest_sources}
    FLAGS -Wno-missing-declarations
    CXX
)

set_target_properties(unittests PROPERTIES CXX_STANDARD 17)
target_compile_options(
    unittests
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(unittests Catch2::Catch2)
if(ENABLE_TCMALLOC)
    target_link_libraries(unittests wt::tcmalloc)
endif()

add_test(NAME unittest COMMAND ${CMAKE_CURRENT_BINARY_DIR}/unittests)
set_tests_properties(unittest PROPERTIES LABELS "check;unittest")
