
# Set the C++ standard  
set(CMAKE_CXX_STANDARD 11)  
set(CMAKE_CXX_STANDARD_REQUIRED True)  

# Create a model library.
add_library(wiredtiger_mockfs SHARED
    src/ext/mock_file.cpp
    src/ext/mock_fs.cpp
)

set_target_properties(wiredtiger_mockfs PROPERTIES ENABLE_EXPORTS ON)

target_include_directories(
    wiredtiger_mockfs
    PUBLIC src
    PRIVATE
        ${CMAKE_BINARY_DIR}/include
)


# Add the executable target and source file  
add_executable(wiredtiger_mockfs_test  
    src/app/demo.cpp
    src/helper/wt.cpp
    src/helper/wt_cur.cpp
    src/helper/wt_cfg.cpp
)

target_link_libraries(wiredtiger_mockfs_test PRIVATE wiredtiger_mockfs)

target_include_directories(
    wiredtiger_mockfs_test
    PUBLIC src
    PRIVATE
        ${CMAKE_BINARY_DIR}/include
)

target_link_directories(wiredtiger_mockfs_test PRIVATE ${CMAKE_BINARY_DIR})
target_link_libraries(wiredtiger_mockfs_test PRIVATE wiredtiger)

# (Optional) also export compile_commands for verification
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
