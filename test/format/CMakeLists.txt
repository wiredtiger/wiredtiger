project(test_format C)

set(format_sources
    alter.c
    backup.c
    bulk.c
    checkpoint.c
    compact.c
    format_config.c
    config_compat.c
    format_config_def.c
    hs.c
    import.c
    kv.c
    ops.c
    random.c
    format_salvage.c
    snap.c
    t.c
    trace.c
    format_timestamp.c
    format_util.c
    verify.c
    wts.c
)

create_test_executable(test_format
    SOURCES ${format_sources}
    EXECUTABLE_NAME "t"
    ADDITIONAL_FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/CONFIG.stress
        ${CMAKE_CURRENT_SOURCE_DIR}/format.sh
        ${CMAKE_CURRENT_SOURCE_DIR}/smoke.sh
)

target_compile_options(test_format PRIVATE -DEXT_LIBPATH="")
add_test(NAME test_format COMMAND ${CMAKE_CURRENT_BINARY_DIR}/smoke.sh)
set_tests_properties(test_format PROPERTIES LABELS "check")
