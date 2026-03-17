set(format_sources
    alter.c
    backup.c
    bulk.c
    checkpoint.c
    checksum.c
    compact.c
    config_compat.c
    follower.c
    format_config.c
    format_config_def.c
    format_disagg.c
    format_prepare_discover.c
    format_salvage.c
    format_timestamp.c
    format_util.c
    hs.c
    import.c
    kv.c
    ops.c
    random.c
    replay.c
    snap.c
    t.c
    trace.c
    verify.c
    wts.c
)

create_test_executable(test_format
    SOURCES ${format_sources}
    EXECUTABLE_NAME "t"
    ADDITIONAL_FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/CONFIG.stress
        ${CMAKE_CURRENT_SOURCE_DIR}/format.sh
        ${CMAKE_CURRENT_SOURCE_DIR}/format_disagg_multi.sh
        ${CMAKE_CURRENT_SOURCE_DIR}/smoke.sh
)

# Use C++ linker for building this target.
# When building sanitized builds (ASan, UBSan, etc) the C++ linker is required
# to link against the C++ sanitizer runtimes.
# Some extensions (PALite, tiered storage) require C++ runtime to be linked
# for sanitizers to work correctly.
set_target_properties(test_format PROPERTIES LINKER_LANGUAGE CXX)
target_compile_options(test_format PRIVATE -DEXT_LIBPATH="")
add_test(NAME test_format COMMAND ${CMAKE_CURRENT_BINARY_DIR}/smoke.sh)
set_tests_properties(test_format PROPERTIES LABELS "check")
