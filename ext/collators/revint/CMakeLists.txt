project(revint_collator C)

set(sources "revint_collator.c")
add_library(wiredtiger_revint_collator MODULE ${sources})

target_include_directories(
    wiredtiger_revint_collator
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
target_compile_options(
    wiredtiger_revint_collator
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
