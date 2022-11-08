project(snappy C)

include(GNUInstallDirs)

config_bool(
    HAVE_BUILTIN_EXTENSION_SNAPPY
    "Builtin snappy compression library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBSNAPPY"
    DEPENDS_ERROR ON "Failed to find snappy library."
)

if (HAVE_BUILTIN_EXTENSION_SNAPPY AND ENABLE_SNAPPY)
    message(FATAL_ERROR "Only one of 'ENABLE_SNAPPY' or 'HAVE_BUILTIN_EXTENSION_SNAPPY' can be enabled.")
endif()

set(sources "snappy_compress.c")
set(link_type)
if(HAVE_BUILTIN_EXTENSION_SNAPPY)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_SNAPPY OR ENABLE_SNAPPY)
    add_library(wiredtiger_snappy ${link_type} ${sources})
    target_include_directories(
        wiredtiger_snappy
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
            ${HAVE_LIBSNAPPY_INCLUDES}
        )
    target_compile_options(
        wiredtiger_snappy
        PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
    )
    set_property(TARGET wiredtiger_snappy PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_SNAPPY)
    target_link_libraries(wiredtiger_snappy wt::snappy)
    install(TARGETS wiredtiger_snappy
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
