project(timestamp_simulator CXX)

# Create a timestamp_simulator library.
add_library(timestamp_simulator SHARED
    src/connection_simulator.cpp
    src/session_simulator.cpp
    src/timestamp_manager.cpp
)
target_include_directories(timestamp_simulator PUBLIC src/include)

target_compile_options(
    timestamp_simulator
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

# Build call log manager.
add_subdirectory(call_log_manager)
add_subdirectory(simulator_interface)
