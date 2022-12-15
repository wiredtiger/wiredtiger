project(simulator_interface CXX)

# Create an executable for the simulator interface.
add_executable(
    simulator_interface
    simulator_interface.cpp
)
target_include_directories(
    simulator_interface
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link the timestamp simulator library with the executable.
target_link_libraries(simulator_interface PUBLIC timestamp_simulator)

target_compile_options(
    simulator_interface
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)
