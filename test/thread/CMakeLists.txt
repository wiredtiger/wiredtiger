project(test_thread C)

create_test_executable(test_thread
    SOURCES
        file.c
        rw.c
        stats.c
        t.c
    # smoke.sh expects the output binary name is 't'.
    EXECUTABLE_NAME "t"
    ADDITIONAL_FILES ${CMAKE_CURRENT_SOURCE_DIR}/smoke.sh
)

add_test(NAME test_thread COMMAND ${CMAKE_CURRENT_BINARY_DIR}/smoke.sh)
set_tests_properties(test_thread PROPERTIES LABELS "check")
