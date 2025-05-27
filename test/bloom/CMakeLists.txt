project(test_bloom C)

create_test_executable(test_bloom
    SOURCES test_bloom.c
)

add_test(NAME test_bloom COMMAND test_bloom)

# Run this during a "ctest check" smoke test.
set_tests_properties(test_bloom PROPERTIES LABELS "check")
