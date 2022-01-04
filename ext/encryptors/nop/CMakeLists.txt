project(nop_encrypt C)

set(sources "nop_encrypt.c")
add_library(wiredtiger_nop_encrypt MODULE ${sources})

target_include_directories(
    wiredtiger_nop_encrypt
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
target_compile_options(
    wiredtiger_nop_encrypt
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
