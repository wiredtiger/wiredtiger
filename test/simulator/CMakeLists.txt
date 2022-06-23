project(simulator CXX)

# Define SIMULATOR_ROOT_DIR to store the current path.
set(SIMULATOR_ROOT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

# Build timestamp simulator
add_subdirectory(timestamp)
