set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(wiredtiger_palite MODULE
    "palite.cpp"
)

target_link_libraries(
    wiredtiger_palite
    PRIVATE
        SQLite::SQLite3
)

target_include_directories(
    wiredtiger_palite
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)

target_compile_options(
    wiredtiger_palite
    PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
)

# For MacOS builds we need to generate a dSYM bundle that contains the debug
# symbols for each executable. The name of the binary will either be the name
# of the target or some other name passed to this function. We need to use the
# correct one for the dsymutil.
if (WT_DARWIN)
    add_custom_command(
        TARGET wiredtiger_palite POST_BUILD
        COMMAND dsymutil $<TARGET_FILE:wiredtiger_palite>
        WORKING_DIRECTORY $<TARGET_FILE_DIR:wiredtiger_palite>
        COMMENT "Running dsymutil on wiredtiger_palite"
        VERBATIM
    )
endif()
