# Skip compiling workgen on non-Posix systems. Currently workgen isn't supported
# for our Windows builds.
if (NOT WT_POSIX)
    return()
endif()

# Determine which wiredtiger target we should use to link against the
# Python Workgen API. For linking purposes, we need to ensure the wiredtiger
# library was compiled as position independent code (fPIC). This being achieved
# either using the 'WITH_PIC' configuration option or building a shared library
# version of WiredTiger ('ENABLE_SHARED'). Default to whichever option is available
# (with a preference for a PIC static build), otherwise throw an error if neither
# build is available.
set(wiredtiger_target)
if(ENABLE_STATIC AND WITH_PIC)
    set(share_state STATIC)
    set(wiredtiger_target wiredtiger_static)
elseif(ENABLE_SHARED)
    set(share_state SHARED)
    set(wiredtiger_target wiredtiger_shared)
else()
    message(STATIC "Skipping workgen compiliation: Requires either a shared library (ENABLE_SHARED)"
                    "or a PIC-enabled (WITH_PIC) build of wiredtiger")
endif()

# Compile the intermediate C++ workgen sources into static library.
add_library(
    workgen_cpp
    STATIC
        ${CMAKE_CURRENT_SOURCE_DIR}/workgen.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/workgen_func.c
)
target_include_directories(
    workgen_cpp
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_BINARY_DIR}/include
        ${CMAKE_BINARY_DIR}/config
        ${CMAKE_SOURCE_DIR}/src/include
        ${CMAKE_SOURCE_DIR}/test/utility
)
target_link_libraries(workgen_cpp test_util Python3::Python)

# For some reason, libstdc++ symbols collide when running workgen under the V5 toolchain 
# on armv9 machines. Therefore prevent symbols from being exported inside the workgen cpp
# library as a workaround. 
target_link_options(workgen_cpp PRIVATE "-Wl,--exclude-libs,ALL")  

set_property(TARGET workgen_cpp PROPERTY POSITION_INDEPENDENT_CODE ON)

# Manually specify the interface name '_workgen' over a generated name.
# Our workgen tooling expects the swig interface name '_workgen'
# when importing the generated swig library.
list(APPEND swig_flags "-c++")
list(APPEND swig_flags "-interface;_workgen")
list(APPEND swig_flags "-threads")
list(APPEND swig_flags "-O")
list(APPEND swig_flags "-Wall")
# Pass in our generated include directories.
list(APPEND swig_flags "-I${CMAKE_BINARY_DIR}/include")
list(APPEND swig_flags "-I${CMAKE_BINARY_DIR}/config")

set(CMAKE_SWIG_FLAGS ${swig_flags})
# Indicate the swig file should be compiled in C++ mode.
set_property(SOURCE ${CMAKE_CURRENT_LIST_DIR}/workgen.i PROPERTY CPLUSPLUS ON)
swig_add_library(workgen
  TYPE ${share_state}
  LANGUAGE python
  SOURCES ${CMAKE_CURRENT_LIST_DIR}/workgen.i
)

target_link_libraries(workgen workgen_cpp ${wiredtiger_target})

# Depending on the chosen compiler (cl, clang or gcc), a slightly different set of compiler flags
# will be used.
set(swig_cpp_flags)
if("${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC")
    set(swig_cpp_flags
        # Ignore sign conversion warning.
        /wd4244
        /w
    )
else()
    set(swig_cpp_flags
        -Wno-sign-conversion
        -w
    )
endif()
list(APPEND swig_cpp_flags -I${CMAKE_SOURCE_DIR})
list(APPEND swig_cpp_flags -I${CMAKE_BINARY_DIR}/config)
list(APPEND swig_cpp_flags -I${CMAKE_CURRENT_SOURCE_DIR})

target_compile_options(workgen
    PRIVATE ${swig_cpp_flags}
)

# Manually changing the suffix on macOS/Darwin builds since DYLD_LIBRARY_PATH
# doesn't work well for dynamic modules loaded by python.
if(WT_DARWIN)
    set_target_properties(workgen PROPERTIES SUFFIX ".so")
endif()
