if(NOT HAVE_LIBVCDENC OR NOT HAVE_LIBVCDDEC OR NOT HAVE_LIBVCDCOM)
    return()
endif()

if(NOT TARGET wt::vcdenc)
    # Define the imported vcdenc library target that can be subsequently linked across the build system.
    # We use the double colons (::) as a convention to tell CMake that the target name is associated
    # with an IMPORTED target (which allows CMake to issue a diagnostic message if the library wasn't found).
    add_library(wt::vcdenc SHARED IMPORTED GLOBAL)
    set_target_properties(wt::vcdenc PROPERTIES
        IMPORTED_LOCATION ${HAVE_LIBVCDENC}
    )
    if (HAVE_LIBVCDENC_INCLUDES)
        set_target_properties(wt::vcdenc PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES ${HAVE_LIBVCDENC_INCLUDES}
        )
    endif()
endif()

if(NOT TARGET wt::vcddec)
    # Define the imported vcdenc library target that can be subsequently linked across the build system.
    # We use the double colons (::) as a convention to tell CMake that the target name is associated
    # with an IMPORTED target (which allows CMake to issue a diagnostic message if the library wasn't found).
    add_library(wt::vcddec SHARED IMPORTED GLOBAL)
    set_target_properties(wt::vcddec PROPERTIES
        IMPORTED_LOCATION ${HAVE_LIBVCDDEC}
    )
    if (HAVE_LIBVCDDEC_INCLUDES)
        set_target_properties(wt::vcddec PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES ${HAVE_LIBVCDDEC_INCLUDES}
        )
    endif()
endif()

if(NOT TARGET wt::vcdcom)
    # Define the imported vcdcom library target that can be subsequently linked across the build system.
    # We use the double colons (::) as a convention to tell CMake that the target name is associated
    # with an IMPORTED target (which allows CMake to issue a diagnostic message if the library wasn't found).
    add_library(wt::vcdcom SHARED IMPORTED GLOBAL)
    set_target_properties(wt::vcdcom PROPERTIES
        IMPORTED_LOCATION ${HAVE_LIBVCDCOM}
    )
endif()
