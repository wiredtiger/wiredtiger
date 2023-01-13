project(azure_store CXX)

find_package(CURL REQUIRED) 
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
cmake_minimum_required(VERSION 3.12)

# Create an intermediate library to build the helper library.
add_library(azure_container_util STATIC azure_connection.cpp)
set_property(TARGET azure_container_util PROPERTY POSITION_INDEPENDENT_CODE ON)

target_include_directories(
    azure_container_util
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_options(
    azure_container_util
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(
    azure_container_util
    PUBLIC
        Azure::azure-storage-blobs
)

# Build the Azure extension as a library 
add_library(wiredtiger_azure_store MODULE azure_storage_source.cpp)
target_include_directories( 
    wiredtiger_azure_store
    PRIVATE    
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_options(
    wiredtiger_azure_store
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(
    wiredtiger_azure_store
    PRIVATE
        azure_container_util
)
<<<<<<< HEAD

add_subdirectory(test)
=======
>>>>>>> develop
