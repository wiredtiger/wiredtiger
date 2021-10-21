#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

# List of common C/CPP flags.
list(APPEND gcc_base_flags "-Wcast-align")
list(APPEND gcc_base_flags "-Wdouble-promotion")
list(APPEND gcc_base_flags "-Werror")
list(APPEND gcc_base_flags "-Wfloat-equal")
list(APPEND gcc_base_flags "-Wformat-nonliteral")
list(APPEND gcc_base_flags "-Wformat-security")
list(APPEND gcc_base_flags "-Wformat=2")
list(APPEND gcc_base_flags "-Winit-self")
list(APPEND gcc_base_flags "-Wmissing-declarations")
list(APPEND gcc_base_flags "-Wmissing-field-initializers")
list(APPEND gcc_base_flags "-Wpacked")
list(APPEND gcc_base_flags "-Wpointer-arith")
list(APPEND gcc_base_flags "-Wredundant-decls")
list(APPEND gcc_base_flags "-Wswitch-enum")
list(APPEND gcc_base_flags "-Wundef")
list(APPEND gcc_base_flags "-Wuninitialized")
list(APPEND gcc_base_flags "-Wunreachable-code")
list(APPEND gcc_base_flags "-Wunused")
list(APPEND gcc_base_flags "-Wwrite-strings")

# Non-fatal informational warnings.
# We don't turn on the unsafe-loop-optimizations warning after gcc7,
# it's too noisy to tolerate. Regardless, don't fail even when it's
# configured.
list(APPEND gcc_base_flags "-Wno-error=unsafe-loop-optimizations")
if(${CMAKE_C_COMPILER_VERSION} VERSION_EQUAL 4.7)
    list(APPEND gcc_base_flags "-Wno-c11-extensions")
    list(APPEND gcc_base_flags "-Wunsafe-loop-optimizations")
elseif(${CMAKE_C_COMPILER_VERSION} VERSION_EQUAL 5)
    list(APPEND gcc_base_flags "-Wunsafe-loop-optimizations")
elseif(${CMAKE_C_COMPILER_VERSION} VERSION_EQUAL 6)
    list(APPEND gcc_base_flags "-Wunsafe-loop-optimizations")
endif()

if(${CMAKE_C_COMPILER_VERSION} VERSION_GREATER_EQUAL 5)
    list(APPEND gcc_base_flags "-Wformat-signedness")
    list(APPEND gcc_base_flags "-Wredundant-decls")
    list(APPEND gcc_base_flags "-Wunused-macros")
    list(APPEND gcc_base_flags "-Wvariadic-macros")
endif()
if(${CMAKE_C_COMPILER_VERSION} VERSION_GREATER_EQUAL 6)
    list(APPEND gcc_base_flags "-Wduplicated-cond")
    list(APPEND gcc_base_flags "-Wlogical-op")
    list(APPEND gcc_base_flags "-Wunused-const-variable=2")
endif()
if(${CMAKE_C_COMPILER_VERSION} VERSION_GREATER_EQUAL 7)
    list(APPEND gcc_base_flags "-Walloca")
    list(APPEND gcc_base_flags "-Walloc-zero")
    list(APPEND gcc_base_flags "-Wduplicated-branches")
    list(APPEND gcc_base_flags "-Wformat-overflow=2")
    list(APPEND gcc_base_flags "-Wformat-truncation=2")
    list(APPEND gcc_base_flags "-Wrestrict")
endif()
if(${CMAKE_C_COMPILER_VERSION} VERSION_GREATER_EQUAL 8)
    list(APPEND gcc_base_flags "-Wmultistatement-macros")
endif()

set(gcc_c_flags ${gcc_base_flags})
set(gcc_cpp_flags ${gcc_base_flags})

# FIX-ME-WT-8247: Add those flags to gcc_base_flags if we want them for the CPP compilation too.
list(APPEND gcc_c_flags "-Waggregate-return")
list(APPEND gcc_c_flags "-Wall")
list(APPEND gcc_c_flags "-Wextra")
list(APPEND gcc_c_flags "-Wshadow")
list(APPEND gcc_c_flags "-Wsign-conversion")

# Specific C flags.
list(APPEND gcc_c_flags "-Wbad-function-cast")
list(APPEND gcc_c_flags "-Wdeclaration-after-statement")
list(APPEND gcc_c_flags "-Wjump-misses-init")
list(APPEND gcc_c_flags "-Wmissing-prototypes")
list(APPEND gcc_c_flags "-Wnested-externs")
list(APPEND gcc_c_flags "-Wold-style-definition")
list(APPEND gcc_c_flags "-Wpointer-sign")
list(APPEND gcc_c_flags "-Wstrict-prototypes")

# Set our gcc flags that can be used by the rest of our build.
set(COMPILER_DIAGNOSTIC_FLAGS ${gcc_c_flags})
set(COMPILER_DIAGNOSTIC_CPP_FLAGS ${gcc_cpp_flags})
