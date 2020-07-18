from SCons.Script import *

def wt_options():
    AddOption("--enable-attach", dest="enable_attach", action="store_true",
        help='Configure WiredTiger library to spin until debugger attach on assert failure. DO NOT configure this option in production environments')

    AddOption("--enable-diagnostic", dest="enable_diagnostic", action="store_true",
        help='Configure WiredTiger library for debugging as well as to perform various run-time diagnostic tests. DO NOT configure this option in production environments')

    AddOption("--enable-lz4", dest="enable_lz4", action="store_true",
        help='Build the LZ4 compression extension (requires LZ4 shared library install)')

    AddOption("--enable-snappy", dest="enable_snappy", action="store_true",
        help='Build the snappy compression extension (requires snappy shared library install)')

    AddOption("--enable-static", dest="enable_static", action="store_true",
        help='Build a static load of the library as well as a shared library')

    AddOption("--enable-zlib", dest="enable_zlib", action="store_true",
        help='Build the zlib compression extension (requires zlib shared library install)')

    AddOption("--enable-zstd", dest="enable_zstd", action="store_true",
        help='Build the zstd compression extension (requires Zstd shared library install)')

    AddOption("--with-spinlock", dest="with_spinlock", action="store", nargs=1, type='string',
        help='Configure WiredTiger library to use "gcc", "pthread", or "pthread_adaptive" spinlocks')
