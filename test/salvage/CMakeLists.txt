project(test_salvage C)

create_test_executable(test_salvage
    SOURCES salvage.c
)

add_test(NAME test_salvage COMMAND test_salvage)

# Run this during a "ctest check" smoke test.
set_tests_properties(test_salvage PROPERTIES LABELS "check")
