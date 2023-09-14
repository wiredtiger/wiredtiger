project(modules_tests CXX)

set(modules_tests
    tests/os_module/test_sample.cpp
)

set(modules_tests_sources
    tests/main.cpp
    ${CMAKE_SOURCE_DIR}/src/os_module/os_module.c
    ${modules_tests}
)

create_test_executable(modules_tests
    SOURCES ${modules_tests_sources}
    INCLUDES ${CMAKE_SOURCE_DIR}/src/os_module
    CXX
)

target_compile_options(
    modules_tests
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(modules_tests Catch2::Catch2)

add_test(NAME modules_tests COMMAND ${CMAKE_CURRENT_BINARY_DIR}/modules_tests)
set_tests_properties(modules_tests PROPERTIES LABELS "check;modules_tests")
