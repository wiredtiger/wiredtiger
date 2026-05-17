add_executable(test_truncate_list test_truncate_list.cpp)
target_link_libraries(test_truncate_list PRIVATE catch2-helpers wt-fakes)

include(CTest)
add_test(NAME test_truncate_list COMMAND test_truncate_list)
