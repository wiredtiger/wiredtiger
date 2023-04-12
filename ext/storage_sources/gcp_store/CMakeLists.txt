project(gcp_store CXX)

# Using the Google Cloud Storage SDK produces a reference problem where a google library
# indirectly depends on an internal google library. Fixing this problem requires setting
# the linker flags and runtime path.
set(CMAKE_EXE_LINKER_FLAGS ${CMAKE_EXE_LINKER_FLAGS} -Wl,--disable-new-dtags)
set(CMAKE_INSTALL_RPATH "${CMAKE_BINARY_DIR}/gcp-sdk-cpp/install/lib")

# Create an intermediate library to build the helper library.
add_library(gcp_bucket_util STATIC gcp_connection.cpp)
set_property(TARGET gcp_bucket_util PROPERTY POSITION_INDEPENDENT_CODE ON)

find_package(absl)
target_include_directories(
    gcp_bucket_util
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_options(
    gcp_bucket_util
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(
    gcp_bucket_util
    PUBLIC
        google-cloud-cpp::storage
        google-cloud-cpp::common
        absl::variant
)

# Build the gcp extension as a library.
add_library(wiredtiger_gcp_store MODULE gcp_storage_source.cpp gcp_log_system.cpp)
target_include_directories(
    wiredtiger_gcp_store
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
        ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_options(
    wiredtiger_gcp_store
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

target_link_libraries(
    wiredtiger_gcp_store
    PRIVATE
        gcp_bucket_util
)

if(HAVE_UNITTEST)
    add_subdirectory(test)
endif()
