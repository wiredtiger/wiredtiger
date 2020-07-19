from SCons.Script import *
from wt_platform import *

def wt_config(conf):
    if GetOption("enable_diagnostic"):
        conf.Define('HAVE_DIAGNOSTIC', 1)
    if GetOption("enable_attach"):
        conf.Define('HAVE_ATTACH', 1)

    if GetOption("with_spinlock"):
        if GetOption("with_spinlock") == "gcc":
            conf.Define('SPINLOCK_TYPE', 'SPINLOCK_GCC')
        if GetOption("with_spinlock") == "msvc":
            conf.Define('SPINLOCK_TYPE', 'SPINLOCK_MSVC')
        if GetOption("with_spinlock") == "pthread":
            conf.Define('SPINLOCK_TYPE', 'SPINLOCK_PTHREAD_MUTEX')
        if GetOption("with_spinlock") == "pthread_adaptive":
            conf.Define('SPINLOCK_TYPE', 'SPINLOCK_PTHREAD_MUTEX_ADAPTIVE')

    # Linux requires buffers aligned to 4KB boundaries for O_DIRECT to work.
    if os_linux:
        conf.Define('WT_BUFFER_ALIGNMENT_DEFAULT', 4096)
    else:
        conf.Define('WT_BUFFER_ALIGNMENT_DEFAULT', 0)

    if conf.CheckCHeader('x86intrin.h'):
        conf.Define('HAVE_X86INTRIN_H', 1)

    if conf.CheckFunc('clock_gettime'):
        conf.Define('HAVE_CLOCK_GETTIME', 1)
    if conf.CheckFunc('fallocate'):
        conf.Define('HAVE_FALLOCATE', 1)
    # OS X wrongly reports that it has fdatasync.
    if not os_darwin and conf.CheckFunc('fdatasync'):
        conf.Define('HAVE_FDATASYNC', 1)
    if conf.CheckFunc('ftruncate'):
        conf.Define('HAVE_FTRUNCATE', 1)
    if conf.CheckFunc('gettimeofday'):
        conf.Define('HAVE_GETTIMEOFDAY', 1)
    if conf.CheckFunc('posix_fadvise'):
        conf.Define('HAVE_FADVISE', 1)
    if conf.CheckFunc('posix_fallocate'):
        conf.Define('HAVE_FALLOCATE', 1)
    if conf.CheckFunc('posix_madvise'):
        conf.Define('HAVE_MADVISE', 1)
    if conf.CheckFunc('posix_memalign'):
        conf.Define('HAVE_MEMALIGN', 1)
    if conf.CheckFunc('setrlimit'):
        conf.Define('HAVE_SETRLIMIT', 1)
    if conf.CheckFunc('strtouq'):
        conf.Define('HAVE_STRTOUQ', 1)
    if conf.CheckFunc('sync_file_range'):
        conf.Define('HAVE_SYNC_FILE_RANGE', 1)
    if conf.CheckFunc('timer_create'):
        conf.Define('HAVE_TIMER_CREATE', 1)
