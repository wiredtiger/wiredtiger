project(zlib C)

include(GNUInstallDirs)

config_bool(
    HAVE_BUILTIN_EXTENSION_ZLIB
    "Builtin zlib compression library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBZ"
    DEPENDS_ERROR ON "Failed to find zlib library."
)

if (HAVE_BUILTIN_EXTENSION_ZLIB AND ENABLE_ZLIB)
    message(FATAL_ERROR "Only one of 'ENABLE_ZLIB' or 'HAVE_BUILTIN_EXTENSION_ZLIB' can be enabled.")
endif()

set(sources "zlib_compress.c")
set(link_type)
if(HAVE_BUILTIN_EXTENSION_ZLIB)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_ZLIB OR ENABLE_ZLIB)
    add_library(wiredtiger_zlib ${link_type} ${sources})
    target_include_directories(
        wiredtiger_zlib
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
            ${HAVE_LIBZ_INCLUDES}
        )
    target_compile_options(
        wiredtiger_zlib
        PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
    )

    set_property(TARGET wiredtiger_zlib PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_ZLIB)
    target_link_libraries(wiredtiger_zlib wt::zlib)
    install(TARGETS wiredtiger_zlib
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
