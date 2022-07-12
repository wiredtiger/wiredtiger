project(timestamp_simulator CXX)

# Create a simulator library.
add_library(simulator SHARED
    src/connection_simulator.cpp
    src/session_simulator.cpp
    src/timestamp_manager.cpp
)
target_include_directories(simulator PUBLIC src/include)

target_compile_options(
    simulator
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

# Build call log manager.
add_subdirectory(call_log_manager)
