#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

# Warning level 3.
list(APPEND win_c_flags "/WX")
# Ignore warning about mismatched const qualifiers.
list(APPEND win_c_flags "/wd4090")
# Ignore deprecated functions.
list(APPEND win_c_flags "/wd4996")
# Complain about unreferenced format parameter.
list(APPEND win_c_flags "/we4100")
# Compile as C code .
list(APPEND win_c_flags "/TC")
# Inline expansion.
list(APPEND win_c_flags "/Ob1")
# Enable string pooling.
list(APPEND win_c_flags "/GF")
# Extern "C" does not throw.
list(APPEND win_c_flags "/EHsc")
# Enable security check.
list(APPEND win_c_flags "/GS")
# Separate functions for linker.
list(APPEND win_c_flags "/Gy")
# Conformance: wchar_t is a native type, not a typedef.
list(APPEND win_c_flags "/Zc:wchar_t")
# Use the __cdecl calling convention for all functions.
list(APPEND win_c_flags "/Gd")

# Set our base compiler flags that can be used by the rest of our build.
set(COMPILER_DIAGNOSTIC_FLAGS "${COMPILER_DIAGNOSTIC_FLAGS};${win_c_flags}" CACHE INTERNAL "" FORCE)
