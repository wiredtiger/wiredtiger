project(lz4 C)

include(GNUInstallDirs)

config_bool(
    HAVE_BUILTIN_EXTENSION_LZ4
    "Builtin lz4 compression library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBLZ4"
    DEPENDS_ERROR ON "Failed to find lz4 library."
)

if (HAVE_BUILTIN_EXTENSION_LZ4 AND ENABLE_LZ4)
    message(FATAL_ERROR "Only one of 'ENABLE_LZ4' or 'HAVE_BUILTIN_EXTENSION_LZ4' can be enabled.")
endif()

set(sources "lz4_compress.c")
set(link_type)

if(HAVE_BUILTIN_EXTENSION_LZ4)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_LZ4 OR ENABLE_LZ4)
    add_library(wiredtiger_lz4 ${link_type} ${sources})
    target_include_directories(
        wiredtiger_lz4
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
            ${HAVE_LIBLZ4_INCLUDES}
        )
    target_compile_options(
        wiredtiger_lz4
        PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
    )

    set_property(TARGET wiredtiger_lz4 PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_LZ4)
    target_link_libraries(wiredtiger_lz4 wt::lz4)
    install(TARGETS wiredtiger_lz4
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )

endif()
