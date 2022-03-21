project(test_packing C)

# Create packing-test executable.
create_test_executable(test_packing
    SOURCES packing-test.c
    EXECUTABLE_NAME "packing-test"
)

# Create intpack test executable.
create_test_executable(test_intpack
    SOURCES intpack-test3.c
    EXECUTABLE_NAME "intpack-test3"
)

add_test(NAME test_packing COMMAND test_packing)
add_test(NAME test_intpack COMMAND test_intpack)
set_tests_properties(test_packing PROPERTIES LABELS "check")
