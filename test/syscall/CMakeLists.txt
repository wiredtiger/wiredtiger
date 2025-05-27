project(test_syscall C)

create_test_executable(test_wt2336_base
    SOURCES wt2336_base/main.c
    # Copy over syscall test runner.
    ADDITIONAL_FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/syscall.py
    ADDITIONAL_DIRECTORIES
        ${CMAKE_CURRENT_SOURCE_DIR}/wt2336_base
)

add_test(NAME test_syscall COMMAND python3 ${CMAKE_CURRENT_BINARY_DIR}/syscall.py)
