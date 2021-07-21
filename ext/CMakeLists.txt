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

project(ext C)

# Build the compressor extensions.
add_subdirectory(compressors/lz4)
add_subdirectory(compressors/snappy)
add_subdirectory(compressors/zlib)
add_subdirectory(compressors/zstd)
add_subdirectory(compressors/nop)

# Build the collator extensions.
add_subdirectory(collators/reverse)
add_subdirectory(collators/revint)

# Build the encryptor extensions.
add_subdirectory(encryptors/nop)

# Build the extractor extensions.
add_subdirectory(extractors/csv)

# We currently don't support these tests extensions on non-POSIX systems since they are designed around the
# existence of POSIX utilities or certain system headers e.g Linux signals.
if(WT_POSIX)
    # Build the encryptor extensions.
    add_subdirectory(encryptors/rotn)
    add_subdirectory(encryptors/sodium)

    # Build the storage_sources extensions.
    add_subdirectory(storage_sources/local_store)

    # Build the test extensions.
    add_subdirectory(test/fail_fs)
endif()
