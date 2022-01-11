project(s3_store CXX)
find_package(AWSSDK REQUIRED COMPONENTS s3-crt)

set(sources "s3_store.cpp")
add_library(wiredtiger_s3_store MODULE ${sources})

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
