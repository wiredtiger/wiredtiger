project(test_readonly C)

# Create packing-test executable.
create_test_executable(test_readonly
    SOURCES readonly.c
    EXECUTABLE_NAME "t"
    ADDITIONAL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/smoke.sh
)

add_test(NAME test_readonly COMMAND ${CMAKE_CURRENT_BINARY_DIR}/smoke.sh)
set_tests_properties(test_readonly PROPERTIES LABELS "check")
