project(s3_store CXX)

include(ExternalProject)
 
ExternalProject_Add(libawscpp-download                                                    
    PREFIX aws-sdk-cpp                                                                    
    GIT_REPOSITORY    https://github.com/aws/aws-sdk-cpp.git                              
    GIT_TAG           1.9.175                                                              
    LIST_SEPARATOR    "|"                                                                 
    CMAKE_ARGS       -DBUILD_SHARED_LIBS=OFF                                              
                     -DBUILD_ONLY=s3|s3-crt                                               
                     -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install
                     -DCMAKE_POSITION_INDEPENDENT_CODE=ON                                 
    BUILD_ALWAYS      TRUE                                  
    TEST_COMMAND      ""                                                           
)

set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH}
    ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install/lib64 
    ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install/lib64/cmake/AWSSDK
    ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install/lib64/cmake/aws-cpp-sdk-core
    ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install/lib64/cmake/aws-cpp-sdk-s3
    ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install/lib64/cmake/aws-cpp-sdk-s3-crt)

find_package(AWSSDK REQUIRED COMPONENTS s3)

set(sources "s3_store.cpp")
add_library(wiredtiger_s3_store MODULE ${sources})
add_dependencies(wiredtiger_s3_store libawscpp-download)

target_include_directories(
    wiredtiger_s3_store
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)

target_link_libraries(
    wiredtiger_s3_store
    ${AWSSDK_LINK_LIBRARIES}
)

if(ENABLE_STRICT)
target_compile_options(
    wiredtiger_s3_store
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)
endif()
