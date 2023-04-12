project(azure_store CXX)

find_package(CURL REQUIRED)
cmake_minimum_required(VERSION 3.13)

# Using the Azure SDK produces a reference problem where an Azure Library
# indirectly depends on an internal Azure library. Fixing this problem requires setting
# the linker flags and runtime path.
set(CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS} -Wl,--disable-new-dtags)
set(CMAKE_INSTALL_RPATH "${CMAKE_BINARY_DIR}/azure-sdk-cpp/install/lib")

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
        Azure::azure-core
        Azure::azure-storage-blobs
        Azure::azure-storage-common
)

# Build the Azure extension as a library
add_library(wiredtiger_azure_store MODULE azure_storage_source.cpp azure_log_system.cpp)
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

if(HAVE_UNITTEST)
    add_subdirectory(test)
endif()
