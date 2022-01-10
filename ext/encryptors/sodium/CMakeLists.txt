project(sodium_encrypt C)

include(GNUInstallDirs)

config_bool(
    HAVE_BUILTIN_EXTENSION_SODIUM
    "Builtin sodium encryption library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBSODIUM"
    DEPENDS_ERROR ON "Failed to find sodium library."
)

if (HAVE_BUILTIN_EXTENSION_SODIUM AND ENABLE_SODIUM)
    message(FATAL_ERROR "Only one of 'ENABLE_SODIUM or 'HAVE_BUILTIN_EXTENSION_SODIUM can be enabled.")
endif()

set(sources "sodium_encrypt.c")
set(link_type)
if(HAVE_BUILTIN_EXTENSION_SODIUM)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_SODIUM OR ENABLE_SODIUM)
    add_library(wiredtiger_sodium ${link_type} ${sources})
    target_include_directories(
        wiredtiger_sodium
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
            ${HAVE_LIBSODIUM_INCLUDES}
        )
    target_compile_options(
        wiredtiger_sodium
        PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
    )

    set_property(TARGET wiredtiger_sodium PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_SODIUM)
    target_link_libraries(wiredtiger_sodium wt::sodium)
    install(TARGETS wiredtiger_sodium
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
