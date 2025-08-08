project(page_log C)

set(sources
  "palite.c"
  "palite_kv.c"
  "palite_trace.c"
  "palite_verbose.c"
  "../../../third_party/sqlite3/sqlite3.c"
)

add_library(wiredtiger_palite MODULE ${sources})

target_include_directories(
    wiredtiger_palite
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
target_compile_options(
    wiredtiger_palite
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)

set_property(
  SOURCE palite_kv.c
  APPEND
  PROPERTY COMPILE_OPTIONS "-Wno-format-nonliteral"
)

if(WT_DARWIN)
  set_property(
    SOURCE ../../../third_party/sqlite3/sqlite3.c
    APPEND
    PROPERTY COMPILE_OPTIONS "-DSQLITE_THREADSAFE;-Wno-error;-Wno-sign-conversion;-Wno-missing-noreturn;-Wno-shorten-64-to-32;-Wno-unreachable-code-break;-Wno-unreachable-code;-Wno-conditional-uninitialized;-Wno-implicit-int-conversion;-Wno-documentation;-Wno-shadow;-Wno-covered-switch-default;-Wno-unused-parameter;-Wno-unused-macros"
  )
  set_property(
    SOURCE ../../../third_party/sqlite3/shell.c
    APPEND
    PROPERTY COMPILE_OPTIONS "-DSQLITE_THREADSAFE;-Wno-error;-Wno-sign-conversion;-Wno-missing-noreturn;-Wno-shorten-64-to-32;-Wno-unreachable-code-break;-Wno-unreachable-code;-Wno-conditional-uninitialized;-Wno-implicit-int-conversion;-Wno-documentation;-Wno-shadow;-Wno-covered-switch-default;-Wno-unused-parameter;-Wno-unused-macros"
  )
else()
  set_property(
    SOURCE ../../../third_party/sqlite3/sqlite3.c
    APPEND
    PROPERTY COMPILE_OPTIONS "-DSQLITE_THREADSAFE;-Wno-error;-Wno-sign-conversion;-Wno-discarded-qualifiers;-Wno-undef;-Wno-unused-macros;-Wno-shadow;-Wno-unused-parameter;-Wno-jump-misses-init;-Wno-declaration-after-statement"
  )
  set_property(
    SOURCE ../../../third_party/sqlite3/shell.c
    APPEND
    PROPERTY COMPILE_OPTIONS "-DSQLITE_THREADSAFE;-Wno-error;-Wno-sign-conversion;-Wno-discarded-qualifiers;-Wno-undef;-Wno-unused-macros;-Wno-shadow;-Wno-unused-parameter;-Wno-jump-misses-init;-Wno-declaration-after-statement"
  )
endif()
