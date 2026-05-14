file(GLOB _sources *.cpp)
add_library(catch2-helpers ${_sources})
target_link_libraries(catch2-helpers PUBLIC wt::wiredtiger Catch2::Catch2)

target_include_directories(catch2-helpers PUBLIC ${CMAKE_SOURCE_DIR}/src/include
                                                 ${CMAKE_BINARY_DIR}/config)
