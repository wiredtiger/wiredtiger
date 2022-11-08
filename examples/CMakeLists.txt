project(examples)

add_subdirectory(c)

# Some of the examples expect to find the `wt` binary at a specific location relative to
# their own binary directory (../../wt). This being the directory structure produced by the autoconf
# build. Copy the wt binary in an expected path for backwards compatibility.
add_custom_command(OUTPUT wt
    COMMAND ${CMAKE_COMMAND} -E copy
    $<TARGET_FILE:wt>
    ${CMAKE_CURRENT_BINARY_DIR}/wt
    DEPENDS wt
)

add_custom_target(sym_wt_examples
    ALL
    DEPENDS wt ${CMAKE_CURRENT_BINARY_DIR}/wt
)
