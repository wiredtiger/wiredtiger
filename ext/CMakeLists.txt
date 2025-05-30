project(ext C)

# Build the compressor extensions.
add_subdirectory(compressors/lz4)
add_subdirectory(compressors/snappy)
add_subdirectory(compressors/zlib)
add_subdirectory(compressors/zstd)
add_subdirectory(compressors/iaa)
add_subdirectory(compressors/nop)

# Build the collator extensions.
add_subdirectory(collators/reverse)
add_subdirectory(collators/revint)

# Build the encryptor extensions.
add_subdirectory(encryptors/nop)

# We currently don't support these tests extensions on non-POSIX systems since they are designed around the
# existence of POSIX utilities or certain system headers e.g Linux signals.
if(WT_POSIX)
    # Build the encryptor extensions.
    add_subdirectory(encryptors/rotn)
    add_subdirectory(encryptors/sodium)

    # Build the page log extensions.
    add_subdirectory(page_log/palm)

    # Build the storage_sources extensions.
    add_subdirectory(storage_sources/dir_store)
    if(ENABLE_S3)
        add_subdirectory(storage_sources/s3_store)
    endif()

    if(ENABLE_GCP)
        add_subdirectory(storage_sources/gcp_store)
    endif()

    if(ENABLE_AZURE)
        add_subdirectory(storage_sources/azure_store)
    endif()

    # Build the test extensions.
    add_subdirectory(test/fail_fs)
endif()
