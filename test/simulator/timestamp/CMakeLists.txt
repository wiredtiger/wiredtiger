project(timestamp_simulator CXX)

add_executable(timestamp_simulator timestamp_simulator.cpp)
target_include_directories(timestamp_simulator PUBLIC ${SIMULATOR_ROOT_DIR}/third_party/nlohmann)
