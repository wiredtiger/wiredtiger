project(test_manydbs C)

create_test_executable(test_manydbs
    SOURCES manydbs.c
)


# If on windows, explicitly run the test under a separate process, manydbs
# has issues running under a ctest process (possibly resource-bound).
if(WT_WIN)
    add_test(NAME test_manydbs COMMAND powershell.exe $<TARGET_FILE:test_manydbs>)
else()
    add_test(NAME test_manydbs COMMAND test_manydbs)
endif()

# Run this during a "ctest check" smoke test.
set_tests_properties(test_manydbs PROPERTIES LABELS "check")
