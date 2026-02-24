config_bool(
    HAVE_BUILTIN_EXTENSION_KEY_PROVIDER
    "Builtin key provider extension."
    DEFAULT OFF
)

set(sources
    key_provider.h
    key_provider.c
)

if(HAVE_BUILTIN_EXTENSION_KEY_PROVIDER)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

add_library(wiredtiger_key_provider ${link_type} ${sources})

target_include_directories(
    wiredtiger_key_provider
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
)
