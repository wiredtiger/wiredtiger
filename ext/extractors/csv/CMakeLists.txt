project(csv C)

set(sources "csv_extractor.c")
add_library(wiredtiger_csv_extractor MODULE ${sources})

target_include_directories(
    wiredtiger_csv_extractor
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
target_compile_options(
    wiredtiger_csv_extractor
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)
