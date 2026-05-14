add_executable(test_cur_layered test_cur_layered.cpp)
target_link_libraries(test_cur_layered PRIVATE catch2-helpers wt-fakes)

include(CTest)
add_test(NAME test_cur_layered COMMAND test_cur_layered)
