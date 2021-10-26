#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

project(wt C)

set(sources
    util_main.c
    util_alter.c
    util_backup.c
    util_compact.c
    util_cpyright.c
    util_create.c
    util_downgrade.c
    util_drop.c
    util_dump.c
    util_list.c
    util_load.c
    util_load_json.c
    util_loadtext.c
    util_misc.c
    util_printlog.c
    util_read.c
    util_rename.c
    util_salvage.c
    util_stat.c
    util_truncate.c
    util_upgrade.c
    util_verbose.c
    util_verify.c
    util_write.c
)

add_executable(wt ${sources})
target_compile_options(wt PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS})
target_include_directories(wt
    PRIVATE ${CMAKE_SOURCE_DIR}/src/include  ${CMAKE_BINARY_DIR}/config
)
target_link_libraries(wt wiredtiger)

if(ENABLE_TCMALLOC AND HAVE_LIBTCMALLOC)
    target_link_libraries(wt ${HAVE_LIBTCMALLOC})
endif()

# Our test suite expects `wt` to sit at the top level build directory.
# Set the output binary location to the top level directory for backwards
# compatibility.
set_target_properties(wt
    PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
)

# Install destination for the wiredtiger utility binary.
install(TARGETS wt
    RUNTIME DESTINATION bin
    DESTINATION ${CMAKE_INSTALL_BINDIR}
)
