project(page_log C)

set(sources
  "palm.c"
  "palm_kv.c"
  "../../../third_party/openldap_liblmdb/mdb.c"
  "../../../third_party/openldap_liblmdb/midl.c"
)

if("${CMAKE_BUILD_TYPE}" MATCHES "^MSan$")
    list(APPEND sources ${CMAKE_SOURCE_DIR}/src/support/msan_fstat_suppression_wrappers.c)
endif()

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

set_property(
  SOURCE ../../../third_party/openldap_liblmdb/mdb.c
  APPEND
  PROPERTY COMPILE_OPTIONS "-Wno-sign-conversion;-Wno-discarded-qualifiers;-Wno-undef;-Wno-unused-macros;-Wno-shadow;-Wno-unused-parameter;-Wno-jump-misses-init;-Wno-declaration-after-statement"
)
set_property(
  SOURCE ../../../third_party/openldap_liblmdb/midl.c
  APPEND
  PROPERTY COMPILE_OPTIONS "-Wno-sign-conversion;-Wno-discarded-qualifiers;-Wno-undef;-Wno-unused-macros;-Wno-shadow;-Wno-unused-parameter;-Wno-jump-misses-init;-Wno-declaration-after-statement"
  )
