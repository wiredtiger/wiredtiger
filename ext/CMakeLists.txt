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
    if(ENABLE_PALITE)
        add_subdirectory(page_log/palite)
    endif()

    # Build the test extensions.
    add_subdirectory(test/fail_fs)
endif()

add_subdirectory(test/key_provider)

# Umbrella target: depends on every MODULE extension library built in this tree.
# Consumers that load extensions via dlopen() at runtime (e.g. test_format, wiredtiger_python)
# can add_dependencies on this target instead of maintaining their own per-consumer lists.
add_custom_target(wiredtiger_ext)
foreach(_ext
        wiredtiger_lz4 wiredtiger_snappy wiredtiger_zlib wiredtiger_zstd
        wiredtiger_reverse_collator wiredtiger_reverse_int_collator
        wiredtiger_rotn wiredtiger_sodium
        wiredtiger_palite
        wiredtiger_fail_fs
        wiredtiger_key_provider)
    if(TARGET ${_ext})
        get_target_property(_type ${_ext} TYPE)
        if(_type STREQUAL "MODULE_LIBRARY")
            add_dependencies(wiredtiger_ext ${_ext})
        endif()
    endif()
endforeach()
