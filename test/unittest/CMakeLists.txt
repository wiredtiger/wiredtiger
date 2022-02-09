project(unittest CXX)

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
