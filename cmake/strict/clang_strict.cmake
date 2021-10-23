#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

# List of common C/CXX flags.
list(APPEND clang_base_flags "-Werror")
list(APPEND clang_base_flags "-Wno-cast-align")
list(APPEND clang_base_flags "-Wno-documentation-unknown-command")
list(APPEND clang_base_flags "-Wno-format-nonliteral")
list(APPEND clang_base_flags "-Wno-packed")
list(APPEND clang_base_flags "-Wno-padded")
list(APPEND clang_base_flags "-Wno-reserved-id-macro")
list(APPEND clang_base_flags "-Wno-zero-length-array")

# We should turn on cast-qual, but not as a fatal error: see WT-2690.
# For now, turn it off.
list(APPEND clang_base_flags "-Wno-cast-qual")

# Turn off clang thread-safety-analysis, it doesn't like some of the
# code patterns in WiredTiger.
list(APPEND clang_base_flags "-Wno-thread-safety-analysis")

# On Centos 7.3.1611, system header files aren't compatible with
# -Wdisabled-macro-expansion.
list(APPEND clang_base_flags "-Wno-disabled-macro-expansion")

# We occasionally use an extra semicolon to indicate an empty loop or
# conditional body.
list(APPEND clang_base_flags "-Wno-extra-semi-stmt")

# Ignore unrecognized options.
list(APPEND clang_base_flags "-Wno-unknown-warning-option")

if(WT_DARWIN AND (CMAKE_C_COMPILER_VERSION VERSION_EQUAL 4.1))
    # Apple clang has its own numbering system, and older OS X
    # releases need some special love. Turn off some flags for
    # Apple's clang 4.1:
    #   Apple clang version 4.1
    #   ((tags/Apple/clang-421.11.66) (based on LLVM 3.1svn)
    list(APPEND clang_base_flags "-Wno-attributes")
    list(APPEND clang_base_flags "-Wno-pedantic")
    list(APPEND clang_base_flags "-Wno-unused-command-line-argument")
endif()

if(CMAKE_C_COMPILER_VERSION VERSION_GREATER_EQUAL 10)
    # Clang 10+ has added additional on-by-default diagnostics that isn't
    # compatible with some of the code patterns in WiredTiger.
    list(APPEND clang_base_flags "-Wno-implicit-fallthrough")
    list(APPEND clang_base_flags "-Wno-implicit-int-float-conversion")
endif()

if(WT_DARWIN AND NOT CMAKE_CROSSCOMPILING)
    # If we are not cross-compiling, we can safely disable this diagnostic.
    # Its incompatible with strict diagnostics when including external
    # libraries that are not in the default linker path
    # e.g. linking zlib/snappy/... from /usr/local/.
    list(APPEND clang_base_flags "-Wno-poison-system-directories")
endif()

set(clang_c_flags ${clang_base_flags})
set(clang_cxx_flags ${clang_base_flags})

# Specific C flags.
# The -Weverything flag is not recommended, we only keep it for C files as it was already enabled.
list(APPEND clang_c_flags "-Weverything")

# Set our compiler flags that can be used by the rest of our build.
set(COMPILER_DIAGNOSTIC_C_FLAGS ${clang_c_flags})
set(COMPILER_DIAGNOSTIC_CXX_FLAGS ${clang_cxx_flags})
