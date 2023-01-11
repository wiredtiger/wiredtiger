azure_include(ExternalProject)
include(GNUInstallDirs)
include(${CMAKE_SOURCE_DIR}/cmake/helpers.cmake)

# Skip the AZURE SDK build step if the extension is not enabled.
if(NOT ENABLE_AZURE)
    return()
endif()

config_choice(
    IMPORT_AZURE_SDK
    "Specify how to import the AZURE SDK"
    OPTIONS
        "none;IMPORT_AZURE_SDK_NONE;NOT ENABLE_AZURE"
        "package;IMPORT_AZURE_SDK_PACKAGE;ENABLE_AZURE"
        "external;IMPORT_AZURE_SDK_EXTERNAL;ENABLE_AZURE"
)
 
if(IMPORT_AZURE_SDK_NONE)
    message(FATAL_ERROR "Cannot enable Azure extension without specifying an IMPORT_AZURE_SDK method (package, external).")
endif()

set(azure_storage_lib_location)
set(azure_core_lib_location)
set(azure_sdk_include_location)

if(IMPORT_AZURE_SDK_PACKAGE)
    find_package()
    # use the Azure provided variables to set the paths for the Azure targets.
    set(azure_storage_lib_location ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libazure-storage-blobs${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(azure_core_lib_location ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libazure-core${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(azure_sdk_include_location ${INSTALL_DIR}/${CMAKE_INSTALL_INCLUDEDIR})

else if (IMPORT_AZURE_SDK_EXTERNAL)
    ExternalProject_Add(
        azure-sdk
        PREFIX azure-sdk-cpp
        GIT_REPOSITORY      https://github.com/Azure/azure-sdk-for-cpp.git
        GIT_TAG             azure-storage-blobs_12.2.0
        CMAKE_ARGS
            -DBUILD_DEPS=ON
            -DBUILD_SHARED_LIBS=ON
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/azure-sdk-cpp/install
        BUILD_ALWAYS FALSE
        INSTALL_DIR ${CMAKE_CURRENT_BINARY_DIR}/azure-sdk-cpp/install
        BUILD_BYPRODUCTS
            ${CMAKE_CURRENT_BINARY_DIR}/azure-sdk-cpp/install/${CMAKE_INSTALL_LIBDIR}/libazure-storage-blobs${CMAKE_SHARED_LIBRARY_SUFFIX}
            ${CMAKE_CURRENT_BINARY_DIR}/azure-sdk-cpp/install/${CMAKE_INSTALL_LIBDIR}/libazure-core${CMAKE_SHARED_LIBRARY_SUFFIX}
        TEST_COMMAND ""
        UPDATE_COMMAND ""
    )
    ExternalProject_Get_Property(azure-sdk INSTALL_DIR)
    file(MAKE_DIRECTORY ${INSTALL_DIR}/${CMAKE_INSTALL_INCLUDEDIR})

    # Set the path variables to be used for the AZURE targets.
    set(azure_storage_lib_location ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libazure-storage-blobs${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(azure_core_lib_location ${INSTALL_DIR}/${CMAKE_INSTALL_LIBDIR}/libazure-core${CMAKE_SHARED_LIBRARY_SUFFIX})
    set(azure_sdk_include_location ${INSTALL_DIR}/${CMAKE_INSTALL_INCLUDEDIR})
endif()

add_library(azure_storage_lib SHARED IMPORTED)
add_library(azure_core_lib SHARED IMPORTED)

# Small workaround to declare the include directory under INTERFACE_INCLUDE_DIRECTORIES during the configuration phase.
set_target_properties(azure_storage_lib PROPERTIES
    IMPORTED_LOCATION ${azure_storage_lib_location}
    INTERFACE_INCLUDE_DIRECTORIES ${azure_sdk_include_location}
)

# Small workaround to declare the include directory under INTERFACE_INCLUDE_DIRECTORIES during the configuration phase.
set_target_properties(azure_core_lib PROPERTIES
    IMPORTED_LOCATION ${azure_core_lib_location}
    INTERFACE_INCLUDE_DIRECTORIES ${azure_sdk_include_location}
)
