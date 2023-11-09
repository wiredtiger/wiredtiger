project(model_tests)

include(${CMAKE_SOURCE_DIR}/cmake/helpers.cmake)

# Create the common test utility library for the model.
add_library(wiredtiger_model_test_common SHARED
   common/wiredtiger_util.cpp
)
target_include_directories(wiredtiger_model_test_common PUBLIC common/include)
target_include_directories(wiredtiger_model_test_common
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_SOURCE_DIR}/test/utility
        ${CMAKE_BINARY_DIR}/config
)
target_link_libraries(wiredtiger_model_test_common wiredtiger_model wt::wiredtiger test_util)

# Build tests.
create_test_executable(test_model_basic
    SOURCES model_basic/main.cpp
    LIBS wiredtiger_model wiredtiger_model_test_common
    CXX
)
create_test_executable(test_model_checkpoint
    SOURCES model_checkpoint/main.cpp
    LIBS wiredtiger_model wiredtiger_model_test_common
    CXX
)
create_test_executable(test_model_transaction
    SOURCES model_transaction/main.cpp
    LIBS wiredtiger_model wiredtiger_model_test_common
    CXX
)

# The test script.
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/test_model.sh
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/test_model.sh
        ${CMAKE_CURRENT_BINARY_DIR}/test_model.sh
    DEPENDS test_model.sh
)
add_custom_target(copy_file_test_model DEPENDS test_model.sh)
add_dependencies(wiredtiger_model_test_common copy_file_test_model)
