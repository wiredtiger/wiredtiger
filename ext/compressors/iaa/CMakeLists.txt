config_bool(
    HAVE_BUILTIN_EXTENSION_IAA
    "Builtin iaa compression library."
    DEFAULT OFF
    DEPENDS "HAVE_LIBQPL"
    DEPENDS_ERROR ON "Failed to find qpl library."
)

if (HAVE_BUILTIN_EXTENSION_IAA AND ENABLE_IAA)
    message(FATAL_ERROR "Only one of 'ENABLE_IAA' or 'HAVE_BUILTIN_EXTENSION_IAA' can be enabled.")
endif()

if(HAVE_BUILTIN_EXTENSION_IAA OR ENABLE_IAA)
    add_library(iaacodec STATIC iaacodec/src/IAACompressionCodecDeflate.cpp iaacodec/src/iaaInterface-c.cpp)
    target_include_directories(
        iaacodec
        PRIVATE
            iaacodec/include
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
    )
    target_compile_options(
        iaacodec
        PRIVATE ${COMPILER_DIAGNOSTIC_CXX_FLAGS}
    )
    target_link_libraries(iaacodec PUBLIC wt::qpl)
    set_property(TARGET iaacodec PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

set(sources "iaa_compress.c")
set(link_type)
if(HAVE_BUILTIN_EXTENSION_IAA)
    set(link_type "OBJECT")
else()
    set(link_type "MODULE")
endif()

if(HAVE_BUILTIN_EXTENSION_IAA OR ENABLE_IAA)
    add_library(wiredtiger_iaa ${link_type} ${sources})
    target_include_directories(
        wiredtiger_iaa
        PRIVATE
            ${CMAKE_SOURCE_DIR}/src/include
            ${CMAKE_BINARY_DIR}/include
            ${CMAKE_BINARY_DIR}/config
            iaacodec/include
        )
    target_link_libraries(wiredtiger_iaa PRIVATE wt::qpl)
    target_compile_options(
        wiredtiger_iaa
        PRIVATE ${COMPILER_DIAGNOSTIC_C_FLAGS}
    )
    set_property(TARGET wiredtiger_iaa PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

if(ENABLE_IAA)
    if(TARGET iaacodec)
        target_link_libraries(wiredtiger_iaa PRIVATE iaacodec)
    endif()
    target_link_libraries(wiredtiger_iaa PUBLIC ${CMAKE_DL_LIBS})
    if(HAVE_LIBACCEL_CONFIG)
        target_link_libraries(wiredtiger_iaa PRIVATE wt::accel_config)
    endif()

    install(TARGETS wiredtiger_iaa
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
