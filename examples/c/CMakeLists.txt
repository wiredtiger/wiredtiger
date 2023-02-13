include(${CMAKE_SOURCE_DIR}/test/ctest_helpers.cmake)

set(c_tests)

define_c_test(
    TARGET ex_access
    SOURCES ex_access.c
    DIR_NAME ex_access
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_access>/WT_HOME>
)

define_c_test(
    TARGET ex_all
    SOURCES ex_all.c
    DIR_NAME ex_all
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_all>/WT_HOME>
)

define_c_test(
    TARGET ex_backup
    SOURCES ex_backup.c
    DIR_NAME ex_backup
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_backup>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET ex_backup_block
    SOURCES ex_backup_block.c
    DIR_NAME ex_backup_block
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_backup_block>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET ex_call_center
    SOURCES ex_call_center.c
    DIR_NAME ex_call_center
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_call_center>/WT_HOME>
)

define_c_test(
    TARGET ex_col_store
    SOURCES ex_col_store.c
    DIR_NAME ex_col_store
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_col_store>/WT_HOME>
)

define_c_test(
    TARGET ex_config_parse
    SOURCES ex_config_parse.c
    DIR_NAME ex_config_parse
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_config_parse>/WT_HOME>
)

define_c_test(
    TARGET ex_cursor
    SOURCES ex_cursor.c
    DIR_NAME ex_cursor
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_cursor>/WT_HOME>
)

define_c_test(
    TARGET ex_data_source
    SOURCES ex_data_source.c
    DIR_NAME ex_data_source
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_data_source>/WT_HOME>
)

define_c_test(
    TARGET ex_encrypt
    SOURCES ex_encrypt.c
    DIR_NAME ex_encrypt
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_encrypt>/WT_HOME>
)

define_c_test(
    TARGET ex_event_handler
    SOURCES ex_event_handler.c
    DIR_NAME ex_event_handler
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_event_handler>/WT_HOME>
)
define_c_test(
    TARGET ex_extending
    SOURCES ex_extending.c
    DIR_NAME ex_extending
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_extending>/WT_HOME>
)

define_c_test(
    TARGET ex_extractor
    SOURCES ex_extractor.c
    DIR_NAME ex_extractor
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_extractor>/WT_HOME>
)

define_c_test(
    TARGET ex_file_system
    SOURCES ex_file_system.c
    DIR_NAME ex_file_system
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_file_system>/WT_HOME>
)

define_c_test(
    TARGET ex_hello
    SOURCES ex_hello.c
    DIR_NAME ex_hello
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_hello>/WT_HOME>
)

define_c_test(
    TARGET ex_log
    SOURCES ex_log.c
    DIR_NAME ex_log
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_log>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET ex_pack
    SOURCES ex_pack.c
    DIR_NAME ex_pack
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_pack>/WT_HOME>
)

define_c_test(
    TARGET ex_process
    SOURCES ex_process.c
    DIR_NAME ex_process
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_process>/WT_HOME>
)

define_c_test(
    TARGET ex_schema
    SOURCES ex_schema.c
    DIR_NAME ex_schema
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_schema>/WT_HOME>
)

define_c_test(
    TARGET ex_smoke
    SOURCES ex_smoke.c
    DIR_NAME ex_smoke
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_smoke>/WT_HOME>
    DEPENDS "WT_POSIX"
)

define_c_test(
    TARGET ex_stat
    SOURCES ex_stat.c
    DIR_NAME ex_stat
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_stat>/WT_HOME>
)

define_c_test(
    TARGET ex_thread
    SOURCES ex_thread.c
    DIR_NAME ex_thread
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_thread>/WT_HOME>
)

define_c_test(
    TARGET ex_tiered
    SOURCES ex_tiered.c
    DIR_NAME ex_tiered
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_tiered>/WT_HOME>
)

define_c_test(
    TARGET ex_verbose
    SOURCES ex_verbose.c
    DIR_NAME ex_verbose
    ARGUMENTS -h $<SHELL_PATH:$<TARGET_FILE_DIR:ex_verbose>/WT_HOME>
)

if(NOT "${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC")
    # On GCC and Clang built targets we want to pass the '-rdynamic' linker option.
    target_link_libraries(ex_encrypt "-rdynamic")
    target_link_libraries(ex_file_system "-rdynamic")
endif()
