project(rotn_encrypt C)

set(sources "rotn_encrypt.c")
add_library(wiredtiger_rotn MODULE ${sources})

target_include_directories(
    wiredtiger_rotn
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
target_compile_options(
    wiredtiger_rotn
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
