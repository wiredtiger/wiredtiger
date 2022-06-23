project(timestamp_simulator CXX)

add_executable(timestamp_simulator timestamp_simulator.cpp)
target_include_directories(timestamp_simulator PUBLIC ${SIMULATOR_ROOT_DIR}/third_party/nlohmann)

target_compile_options(
    timestamp_simulator
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)
