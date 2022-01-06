project(nop_compression C)

set(sources "nop_compress.c")
add_library(wiredtiger_nop_compress SHARED ${sources})

target_include_directories(
    wiredtiger_nop_compress
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
target_compile_options(
    wiredtiger_nop_compress
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
