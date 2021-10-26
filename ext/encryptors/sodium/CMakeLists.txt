#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

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
        )
    target_compile_options(
        wiredtiger_sodium
        PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
    )

    set_property(TARGET wiredtiger_sodium PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_SODIUM)
    # HAVE_LIBSODIUM contains the full pathname to the lib, which is
    # what the cmake docs recommend using.
    target_link_libraries(wiredtiger_sodium ${HAVE_LIBSODIUM})
    install(TARGETS wiredtiger_sodium
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
