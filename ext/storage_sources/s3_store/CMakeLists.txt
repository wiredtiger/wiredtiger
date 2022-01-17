project(s3_store CXX)    
    
include(ExternalProject)    
include(GNUInstallDirs)    
    
ExternalProject_Add(aws-sdk    
    PREFIX aws-sdk-cpp    
    GIT_REPOSITORY    https://github.com/aws/aws-sdk-cpp.git    
    GIT_TAG           1.9.175    
    LIST_SEPARATOR    "|"    
    CMAKE_ARGS       -DBUILD_SHARED_LIBS=ON
                     -DBUILD_ONLY=s3|s3-crt
                     -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install
                     -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    BUILD_ALWAYS    FALSE
    INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install
    BUILD_BYPRODUCTS
    ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install/${CMAKE_INSTALL_LIBDIR}/libaws-cpp-sdk-s3${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install/${CMAKE_INSTALL_LIBDIR}/libaws-cpp-sdk-s3-crt${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk-cpp/install/${CMAKE_INSTALL_LIBDIR}/libaws-cpp-sdk-core${CMAKE_SHARED_LIBRARY_SUFFIX}
    TEST_COMMAND      ""
)
ExternalProject_Get_Property(aws-sdk INSTALL_DIR)

add_library(aws-sdk::core SHARED IMPORTED)
add_library(aws-sdk::s3 SHARED IMPORTED)
add_library(aws-sdk::s3-crt SHARED IMPORTED)
add_library(aws-sdk::crt SHARED IMPORTED)
set_target_properties(aws-sdk::core PROPERTIES
    IMPORTED_LOCATION ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libaws-cpp-sdk-core${CMAKE_SHARED_LIBRARY_SUFFIX}                                             
)
set_target_properties(aws-sdk::s3 PROPERTIES
    IMPORTED_LOCATION ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libaws-cpp-sdk-s3${CMAKE_SHARED_LIBRARY_SUFFIX}
)
set_target_properties(aws-sdk::s3-crt PROPERTIES
    IMPORTED_LOCATION ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libaws-cpp-sdk-s3-crt${CMAKE_SHARED_LIBRARY_SUFFIX})
add_dependencies(aws-sdk::core aws-sdk)
add_dependencies(aws-sdk::s3 aws-sdk)  
add_dependencies(aws-sdk::s3-crt aws-sdk)

set(AWSSDK_INCLUDE_DIRS ${INSTALL_DIR}/include)
set(AWSSDK_LINK_LIBRARIES aws-sdk::s3 aws-sdk::core aws-sdk::s3-crt)

set(sources "s3_store.cpp" "AwsBucketConn.cpp")
add_library(wiredtiger_s3_store MODULE ${sources})
target_include_directories(
    wiredtiger_s3_store    
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include    
        ${CMAKE_BINARY_DIR}/config 
        ${CMAKE_CURRENT_SOURCE_DIR}/AwsBucketConn.h 
        ${AWSSDK_INCLUDE_DIRS}    
)
target_link_libraries(
    wiredtiger_s3_store
    aws-sdk::s3
    aws-sdk::s3-crt
)
target_link_libraries(
    wiredtiger_s3_store
    aws-sdk::core
)