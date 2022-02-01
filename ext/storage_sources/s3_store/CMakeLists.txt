project(s3_store CXX)    

if(IMPORT_S3_SDK_PACKAGE AND ENABLE_S3)
    set(LINK_LIBS ${AWSSDK_LINK_LIBRARIES})
elseif(IMPORT_S3_SDK_EXTERNAL AND ENABLE_S3)
    set(LINK_LIBS aws-sdk::s3-crt aws-sdk::core)
endif()

# Create an intermediate library to build the helper library.
add_library(aws_bucket_util STATIC aws_bucket_conn.cxx)
set_property(TARGET aws_bucket_util PROPERTY POSITION_INDEPENDENT_CODE ON)

target_include_directories(
    aws_bucket_util
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}   
)

target_link_libraries(
    aws_bucket_util
    PUBLIC
        ${LINK_LIBS}
)

# Build the S3 extension as a library.
set(sources "s3_store.cxx")
add_library(wiredtiger_s3_store MODULE ${sources})
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
        ${LINK_LIBS}  
)

# Add the unit tests.
add_subdirectory(test)
