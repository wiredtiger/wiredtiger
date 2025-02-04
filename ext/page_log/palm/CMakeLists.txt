project(page_log C)

set(sources
  "palm.c"
  "palm_kv.c"
  "palm_verbose.c"
  "../../../third_party/openldap_liblmdb/mdb.c"
  "../../../third_party/openldap_liblmdb/midl.c"
)

add_library(wiredtiger_page_log MODULE ${sources})

target_include_directories(
    wiredtiger_page_log
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
target_compile_options(
    wiredtiger_page_log
    PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
)

if(WT_DARWIN)
  set_property(
    SOURCE ../../../third_party/openldap_liblmdb/mdb.c
    APPEND
    PROPERTY COMPILE_OPTIONS "-Wno-error;-Wno-sign-conversion;-Wno-missing-noreturn;-Wno-shorten-64-to-32;-Wno-unreachable-code-break;-Wno-unreachable-code;-Wno-conditional-uninitialized;-Wno-implicit-int-conversion;-Wno-documentation;-Wno-shadow;-Wno-covered-switch-default;-Wno-unused-parameter;-Wno-unused-macros"
  )
  set_property(
    SOURCE ../../../third_party/openldap_liblmdb/midl.c
    APPEND
    PROPERTY COMPILE_OPTIONS "-Wno-error;-Wno-sign-conversion;-Wno-missing-noreturn;-Wno-shorten-64-to-32;-Wno-unreachable-code-break;-Wno-unreachable-code;-Wno-conditional-uninitialized;-Wno-implicit-int-conversion;-Wno-documentation;-Wno-shadow;-Wno-covered-switch-default;-Wno-unused-parameter;-Wno-unused-macros"
  )
else()
  set_property(
    SOURCE ../../../third_party/openldap_liblmdb/mdb.c
    APPEND
    PROPERTY COMPILE_OPTIONS "-Wno-error;-Wno-sign-conversion;-Wno-discarded-qualifiers;-Wno-undef;-Wno-unused-macros;-Wno-shadow;-Wno-unused-parameter;-Wno-jump-misses-init;-Wno-declaration-after-statement"
  )
  set_property(
    SOURCE ../../../third_party/openldap_liblmdb/midl.c
    APPEND
    PROPERTY COMPILE_OPTIONS "-Wno-error;-Wno-sign-conversion;-Wno-discarded-qualifiers;-Wno-undef;-Wno-unused-macros;-Wno-shadow;-Wno-unused-parameter;-Wno-jump-misses-init;-Wno-declaration-after-statement"
  )
endif()
