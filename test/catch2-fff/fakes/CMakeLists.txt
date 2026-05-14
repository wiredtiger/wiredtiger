include(FetchContent)

FetchContent_Declare(
  fff
  GIT_REPOSITORY https://github.com/meekrosoft/fff.git
  GIT_TAG v1.1
  GIT_SHALLOW TRUE)

FetchContent_MakeAvailable(fff)

add_library(fff INTERFACE)
add_library(fff::fff ALIAS fff)
target_include_directories(fff INTERFACE ${fff_SOURCE_DIR})

file(GLOB _sources *.cpp)
add_library(wt-fakes ${_sources})
target_link_libraries(wt-fakes PUBLIC wt::wiredtiger fff::fff)

target_include_directories(wt-fakes PUBLIC ${CMAKE_SOURCE_DIR}/src/include
                                           ${CMAKE_BINARY_DIR}/config)
