project(reverse_collator C)

set(sources "reverse_collator.c")
add_library(wiredtiger_reverse_collator MODULE ${sources})

target_include_directories(
    wiredtiger_reverse_collator
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
target_compile_options(
    wiredtiger_reverse_collator
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
