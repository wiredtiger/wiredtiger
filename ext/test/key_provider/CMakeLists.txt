set(sources
    "key_provider.h"
    "key_provider.c"
)
add_library(wiredtiger_key_provider MODULE ${sources})

target_include_directories(
    wiredtiger_key_provider
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)

target_compile_options(
    wiredtiger_key_provider
    PRIVATE
        ${COMPILER_DIAGNOSTIC_C_FLAGS}
        # Allow zero variadic macro arguments for GNU and Clang compilers.
        $<$<OR:$<BOOL:${GNU_C_COMPILER}>,$<BOOL:${CLANG_C_COMPILER}>>:
            -Wno-gnu-zero-variadic-macro-arguments
        >
)
