project(call_log_manager CXX)

# Create an executable for the call log manager.
add_executable(call_log_manager call_log_interface.cpp call_log_manager.cpp)
target_include_directories(simulator PUBLIC ${SIMULATOR_ROOT_DIR}/third_party)

# Link the simulator library with the executable.
target_link_libraries(call_log_manager PUBLIC simulator)

target_compile_options(
    call_log_manager
    PUBLIC -std=c++17
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)
