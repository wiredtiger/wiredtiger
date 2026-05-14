file(GLOB _sources "helpers/*.cpp")
add_library(catch2_helpers ${_sources})
target_link_libraries(catch2_helpers PUBLIC wt::wiredtiger Catch2::Catch2)

target_include_directories(catch2_helpers PUBLIC ${CMAKE_SOURCE_DIR}/src/include
                                                 ${CMAKE_BINARY_DIR}/config)
