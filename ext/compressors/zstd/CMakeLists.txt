project(zstd C)

include(GNUInstallDirs)

config_bool(
    HAVE_BUILTIN_EXTENSION_ZSTD
    "Builtin zstd compression library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBZSTD"
    DEPENDS_ERROR ON "Failed to find zstd library."
)

if (HAVE_BUILTIN_EXTENSION_ZSTD AND ENABLE_ZSTD)
    message(FATAL_ERROR "Only one of 'ENABLE_ZSTD' or 'HAVE_BUILTIN_EXTENSION_ZSTD' can be enabled.")
endif()

set(sources "zstd_compress.c")
set(link_type)
if(HAVE_BUILTIN_EXTENSION_ZSTD)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_ZSTD OR ENABLE_ZSTD)
    add_library(wiredtiger_zstd ${link_type} ${sources})
    target_include_directories(
        wiredtiger_zstd
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
            ${HAVE_LIBZSTD_INCLUDES}
        )
    target_compile_options(
        wiredtiger_zstd
        PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
    )

    set_property(TARGET wiredtiger_zstd PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_ZSTD)
    target_link_libraries(wiredtiger_zstd wt::zstd)
    install(TARGETS wiredtiger_zstd
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
