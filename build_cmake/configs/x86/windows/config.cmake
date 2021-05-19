#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
#  See the file LICENSE for redistribution information
#

set(WT_ARCH "x86" CACHE STRING "")
set(WT_OS "windows" CACHE STRING "")
set(WT_POSIX OFF CACHE BOOL "")
set(SPINLOCK_TYPE "msvc" CACHE STRING "" FORCE)
# We force a static compilation to generate a ".lib" file. We can then
# additionally generate a dll file using a *DEF file.
set(ENABLE_STATIC ON CACHE BOOL "" FORCE)

# Disable incremental linking.
string(APPEND win_link_flags " /INCREMENTAL:NO")
# Remove dead code.
string(APPEND win_link_flags " /OPT:REF")
# Allow executables to be randomly rebased at load time (enables virtual address allocation randomization).
string(APPEND win_link_flags " /DYNAMICBASE")
# Executable is compatible with the Windows Data Execution Prevention.
string(APPEND win_link_flags " /NXCOMPAT")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${win_link_flags}")
