#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#  All rights reserved.
#
# See the file LICENSE for redistribution information.
#

cmake_minimum_required(VERSION 3.11.0)

project(ext C)

# Build the compressor extensions
add_subdirectory(compressors/lz4)
add_subdirectory(compressors/snappy)
add_subdirectory(compressors/zlib)
add_subdirectory(compressors/zstd)
add_subdirectory(compressors/nop)

# Build the collator extensions
add_subdirectory(collators/reverse)
add_subdirectory(collators/revint)

# Build the encryptor extensions
add_subdirectory(encryptors/nop)
add_subdirectory(encryptors/rotn)

# Build the extractor extensions
add_subdirectory(extractors/csv)

# Build the storage_sources extensions
if(WT_POSIX)
    add_subdirectory(storage_sources/local_store)
endif()

# Build the test extensions
add_subdirectory(test/fail_fs)
