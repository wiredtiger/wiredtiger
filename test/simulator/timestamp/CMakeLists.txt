project(timestamp_simulator CXX)

# create a simulator library.
add_library(simulator SHARED
    src/connection_simulator.cpp
    src/session_simulator.cpp
)
target_include_directories(simulator PUBLIC src/include)

target_compile_options(
    simulator
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

# build call log manager.
add_subdirectory(call_log_manager)
