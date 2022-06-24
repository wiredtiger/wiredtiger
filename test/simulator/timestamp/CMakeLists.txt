project(timestamp_simulator CXX)

add_library(Connection_sim connection_sim.cpp)
add_library(Session_sim session_sim.cpp)
add_library(Session_sim cursor_sim.cpp)
add_executable(timestamp_simulator timestamp_simulator.cpp)
target_include_directories(timestamp_simulator PUBLIC ${SIMULATOR_ROOT_DIR}/third_party/nlohmann)
target_link_libraries(timestamp_simulator LINK_PUBLIC Connection_sim)
target_link_libraries(timestamp_simulator LINK_PUBLIC Session_sim)
target_link_libraries(timestamp_simulator LINK_PUBLIC Cursor_sim)

target_compile_options(
    timestamp_simulator
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)
