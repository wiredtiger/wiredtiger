project(test_packing C)

# Create packing-test executable.
create_test_executable(test_packing
    SOURCES packing-test.c
    EXECUTABLE_NAME "packing-test"
)

if(WT_WIN)
    # Create packing-direct-test executable.
    create_test_executable(test_packing_direct
        SOURCES packing-direct-test.c
        EXECUTABLE_NAME "packing-direct-test"
    )

    # Create packing-direct-test-all executable.
    create_test_executable(test_packing_direct_all
        SOURCES packing-direct-test-all.c
        EXECUTABLE_NAME "packing-direct-test-all"
    )
else()
    # Create packing-direct-test executable.
    create_test_executable(test_packing_direct
        SOURCES packing-direct-test.c
        EXECUTABLE_NAME "packing-direct-test"
        FLAGS -Wno-used-but-marked-unused -Wno-error=pragmas -Wno-error=unknown-pragmas -Wno-pragmas -Wno-error=unused-but-set-variable -Wno-error=unused-parameter -Wno-error
    )
    # Create packing-direct-test-all executable.
    create_test_executable(test_packing_direct_all
        SOURCES packing-direct-test-all.c
        EXECUTABLE_NAME "packing-direct-test-all"
        FLAGS -Wno-used-but-marked-unused -Wno-error=pragmas -Wno-error=unknown-pragmas -Wno-pragmas -Wno-error=unused-but-set-variable -Wno-error=unused-parameter -Wno-error
    )
endif()

# Create intpack test executable.
create_test_executable(test_intpack
    SOURCES intpack-test3.c
    EXECUTABLE_NAME "intpack-test3"
)

add_test(NAME test_packing COMMAND test_packing)
add_test(NAME test_intpack COMMAND test_intpack)
set_tests_properties(test_packing PROPERTIES LABELS "check")
