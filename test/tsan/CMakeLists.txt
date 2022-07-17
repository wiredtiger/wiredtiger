project(test_tsan C)

set(tsan_sources
    main.c
)

create_test_executable(test_tsan
    SOURCES ${tsan_sources}
    EXECUTABLE_NAME "t"
)

add_test(NAME test_tsan COMMAND ./${CMAKE_CURRENT_BINARY_DIR}/t)
set_tests_properties(test_tsan PROPERTIES LABELS "check")
