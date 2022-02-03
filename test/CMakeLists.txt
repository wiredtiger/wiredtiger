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
    add_subdirectory(unittest)

    if(ENABLE_LLVM)
        # The fuzz test only works with llvm compilers. Restrict compilation of this test to only
        # when using Clang.
        add_subdirectory(fuzz)
    endif()
endif()
