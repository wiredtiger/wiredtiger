# Determine which wiredtiger target we should use to link against the
# Python API. For linking purposes, we need to ensure the wiredtiger
# library was compiled as position independent code (fPIC). This being achieved
# either using the 'WITH_PIC' configuration option or building a shared library
# version of WiredTiger ('ENABLE_SHARED'). Default to whichever option is available
# (with a preference for a PIC static build), otherwise throw an error if neither
# build is available.
set(wiredtiger_target)
if(ENABLE_STATIC AND WITH_PIC)
    set(wiredtiger_target wiredtiger_static)
elseif(ENABLE_SHARED)
    set(wiredtiger_target wiredtiger_shared)
else()
    message(FATAL_ERROR "Unable to build the Python API. Requires either a shared \
        library (ENABLE_SHARED) or a PIC-enabled (WITH_PIC) build of wiredtiger.")
endif()

# Manually specify the interface name '_wiredtiger' over a generated name.
# Our wiredtiger python module expects the swig interface name '_wiredtiger'
# when importing the generated swig library.
list(APPEND swig_flags "-interface;_wiredtiger")

list(APPEND swig_flags "-DPY_MAJOR_VERSION=3")
list(APPEND swig_flags "-threads")
list(APPEND swig_flags "-O")
list(APPEND swig_flags "-nodefaultctor")
list(APPEND swig_flags "-nodefaultdtor")

# Pass in our generated include directories.
list(APPEND swig_flags "-I${CMAKE_BINARY_DIR}/include")
list(APPEND swig_flags "-I${CMAKE_BINARY_DIR}/config")

set(CMAKE_SWIG_FLAGS ${swig_flags})

# Add dependencies to the swig module.
set(SWIG_MODULE_wiredtiger_python_EXTRA_DEPS ${CMAKE_SOURCE_DIR}/src/include/wiredtiger.h.in)

swig_add_library(wiredtiger_python
  TYPE SHARED
  LANGUAGE python
  SOURCES ${CMAKE_CURRENT_LIST_DIR}/wiredtiger.i
)

# Depending on the chosen compiler (cl, clang or gcc), a slightly different set of compiler flags
# will be used.
set(swig_c_flags)
if("${CMAKE_C_COMPILER_ID}" MATCHES "^(Apple)?(C|c?)lang")
    set(swig_c_flags
        -Wno-pointer-sign
        -Wno-ignored-qualifiers
        -Wno-incompatible-pointer-types
        -Wno-int-conversion
        -Wno-sign-conversion
        -Wno-unused-parameter
        -Wno-missing-prototypes
        -Wno-conditional-uninitialized
        -Wno-shorten-64-to-32
        -Wno-used-but-marked-unused
        -Wno-unused-macros
        -Wno-unused-variable
        -Wno-strict-prototypes
    )
elseif("${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC")
    set(swig_c_flags
        # Ignore warning about mismatched const qualifiers.
        /wd4090
        # Ignore int conversion warnings.
        /wd4267
        # Ignore incompatible pointer type warnings.
        /wd4133
        # Ignore sign conversion warning.
        /wd4244
        # Ignore warnings about levels of pointer indirection.
        /wd4047
        # Ignore warnings about differing formal and actual qualifiers.
        /wd4024
    )
else()
    set(swig_c_flags
        -Wno-discarded-qualifiers
        -Wno-incompatible-pointer-types
        -Wno-int-conversion
        -Wno-sign-conversion
        -Wno-missing-prototypes
        -Wno-missing-declarations
        -Wno-shadow
        -Wno-pointer-sign
        -Wno-declaration-after-statement
        -Wno-unused-variable
        -Wno-missing-field-initializers
    )
    if(${CMAKE_C_COMPILER_VERSION} VERSION_GREATER_EQUAL 8)
        list(APPEND swig_c_flags -Wno-cast-function-type)
    endif()
endif()
list(APPEND swig_c_flags -I${CMAKE_SOURCE_DIR})
list(APPEND swig_c_flags -I${CMAKE_BINARY_DIR}/config)

# SWIG will generate deprecated python3 protocols (being marked as deprecated in Python 3.7+). Generally supress these warnings
# as the SWIG compiler lags behind newer python3 versions.
if(${Python3_VERSION} VERSION_GREATER_EQUAL 3.7)
    if(NOT "${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC")
        list(APPEND swig_c_flags -Wno-deprecated-declarations)
    else()
        list(APPEND swig_c_flags /wd4996)
    endif()
endif()

target_compile_options(wiredtiger_python
    PRIVATE ${swig_c_flags}
)

target_link_libraries(wiredtiger_python ${wiredtiger_target} Python3::Python)

# Setup our output 'wiredtiger' directory to hold the python module sources.
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/wiredtiger
    DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# Setup the module constructor file (python requirement).
add_custom_command(TARGET wiredtiger_python
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/wiredtiger/init.py
        ${CMAKE_CURRENT_BINARY_DIR}/wiredtiger/__init__.py
)

# The generated swig file cannot be named 'wiredtiger.py' in the target directory
# as we won't be able to import it.
add_custom_command(TARGET wiredtiger_python
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_BINARY_DIR}/wiredtiger.py
        ${CMAKE_CURRENT_BINARY_DIR}/wiredtiger/swig_wiredtiger.py
)

# Force the output name of the shared library to '_wiredtiger'. Various tooling in
# WiredTiger expects to find a binary named '_wiredtiger.so'.
# SWIG adds the underscore prefix '_' automatically.
set_target_properties(wiredtiger_python PROPERTIES OUTPUT_NAME "wiredtiger")

# Manually changing the suffix on macOS/Darwin builds since DYLD_LIBRARY_PATH
# doesn't work well for dynamic modules loaded by python.
if(WT_DARWIN)
    set_target_properties(wiredtiger_python PROPERTIES SUFFIX ".so")
endif()

# Define a simple smoke test to check the Python API can be used.
if(WT_POSIX)
    add_test(NAME test_ex_access
        COMMAND ${CMAKE_COMMAND} -E env PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR}
            ${Python3_EXECUTABLE} -S ${CMAKE_SOURCE_DIR}/examples/python/ex_access.py
    )
    set_tests_properties(test_ex_access PROPERTIES LABELS "check")
endif()
