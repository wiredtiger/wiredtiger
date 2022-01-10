project(libwinshim C)

set(sources
    windows_shim.c
)

set(link_type)
add_library(windows_shim STATIC ${sources})

target_include_directories(
    windows_shim
    PUBLIC ${CMAKE_SOURCE_DIR}/test/windows
    PRIVATE ${CMAKE_BINARY_DIR}/include ${CMAKE_BINARY_DIR}/config ${CMAKE_SOURCE_DIR}/src/include
)

target_compile_options(
    windows_shim
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
