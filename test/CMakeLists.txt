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

project(test)

include(ctest_helpers.cmake)

if(WT_WIN)
    # Compile the windows shim library.
    add_subdirectory(windows)
endif()

# Compile the test library utility
add_subdirectory(utility)

# Compile our tests.
add_subdirectory(bloom)
add_subdirectory(checkpoint)
add_subdirectory(cursor_order)
add_subdirectory(fops)
add_subdirectory(huge)
add_subdirectory(manydbs)
add_subdirectory(csuite)
add_subdirectory(packing)

# We currently don't support these tests on non-POSIX systems since they are designed around the
# existence of POSIX utilities (e.g. certain bash/sh commands, pthreads) or certain system headers
# e.g Linux signals.
if(WT_POSIX)
    add_subdirectory(format)
    add_subdirectory(readonly)
    add_subdirectory(salvage)
    add_subdirectory(syscall)
    add_subdirectory(thread)
    add_subdirectory(cppsuite)

    if("${CMAKE_C_COMPILER_ID}" STREQUAL "Clang")
        # The fuzz test only works with llvm compilers. Restrict compilation of this test to only
        # when using Clang.
        add_subdirectory(fuzz)
    endif()
endif()
