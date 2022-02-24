set(CMAKE_CXX_STANDARD 17)
project(s3_store CXX)

# Create an intermediate library to build the helper library.
add_library(aws_bucket_util STATIC s3_connection.cpp)
set_property(TARGET aws_bucket_util PROPERTY POSITION_INDEPENDENT_CODE ON)

target_include_directories(
    aws_bucket_util
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
    aws_bucket_util
    PUBLIC
        aws-sdk::s3-crt
        aws-sdk::core
)

# Build the S3 extension as a library.
add_library(wiredtiger_s3_store MODULE s3_storage_source.cpp s3_log_system.cpp)
target_include_directories(
    wiredtiger_s3_store
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(
    wiredtiger_s3_store
    PRIVATE
        aws_bucket_util
        aws-sdk::s3-crt
        aws-sdk::core
)

# Add the unit tests.
add_subdirectory(test)
