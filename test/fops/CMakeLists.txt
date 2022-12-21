project(test_fops C)

create_test_executable(test_fops
    SOURCES
        fops_file.c
        fops.c
        t.c
)

# If on windows, explicitly run the test under a separate process, fops
# has issues running under a ctest process (possibly resource-bound).
if(WT_WIN)
    add_test(NAME test_fops COMMAND powershell.exe $<TARGET_FILE:test_fops>)
else()
    add_test(NAME test_fops COMMAND test_fops)
endif()


# Run this during a "ctest check" smoke test.
set_tests_properties(test_fops PROPERTIES LABELS "check")
