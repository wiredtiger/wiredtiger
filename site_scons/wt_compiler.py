import re
import subprocess
from SCons.Script import *
from wt_platform import *

def compiler_version(cc):
    process = subprocess.Popen([cc, '--version'], stdout=subprocess.PIPE)
    (stdout, _) = process.communicate()
    return (re.search(' [0-9][0-9.]* ', stdout.decode()).group().split('.'))

std_includes = """
#include <sys/types.h>
#ifdef _WIN32
#include <inttypes.h>
#endif
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>"
#include <unistd.h>"
"""

def type_check(conf, type, size):
    if not conf.CheckType(type, std_includes):
        print('%s type not found' %(type))
        Exit (1)
    if size != 0 and conf.CheckTypeSize(type) != size:
        print('%s type found, but not %d bytes in size' %(type, size))
        Exit (1)

def wt_compiler(conf):
    # Compiler defaults to gcc.
    cc = conf.env['CC']
    cc_gcc = 'clang' not in cc
    cc_version = compiler_version(cc)
    cc_major = cc_version[0]

    # Check for some basic types and sizes.
    # Windows doesn't have off_t, we'll fix that up later.
    # WiredTiger expects off_t and size_t to be the same size.
    # WiredTiger expects a time_t to fit into a uint64_t.
    type_check(conf, 'pid_t', 0)
    if not os_windows:
        type_check(conf, 'off_t', 8)
    type_check(conf, 'size_t', 8)
    type_check(conf, 'ssize_t', 8)
    type_check(conf, 'time_t', 8)
    type_check(conf, 'uintmax_t', 0)
    type_check(conf, 'uintptr_t', 0)

    conf.env.Append(CPPPATH = ['#', '#src/include'])

    if os_linux:
        conf.env.Append(CPPDEFINES = '_GNU_SOURCE')
    cflags = []
    if GetOption("enable_diagnostic"):
        cflags.append('-g')
    if ARGUMENTS.get('CFLAGS', '').find('-O') == -1:
        cflags.append('-O3')
    if cc_gcc:
        cflags.append('-Wall')
        cflags.append('-Wextra')
        cflags.append('-Werror')
        cflags.append('-Waggregate-return')
        cflags.append('-Wbad-function-cast')
        cflags.append('-Wcast-align')
        cflags.append('-Wdeclaration-after-statement')
        cflags.append('-Wdouble-promotion')
        cflags.append('-Wfloat-equal')
        cflags.append('-Wformat-nonliteral')
        cflags.append('-Wformat-security')
        cflags.append('-Wformat=2')
        cflags.append('-Winit-self')
        cflags.append('-Wjump-misses-init')
        cflags.append('-Wmissing-declarations')
        cflags.append('-Wmissing-field-initializers')
        cflags.append('-Wmissing-prototypes')
        cflags.append('-Wnested-externs')
        cflags.append('-Wold-style-definition')
        cflags.append('-Wpacked')
        cflags.append('-Wpointer-arith')
        cflags.append('-Wpointer-sign')
        cflags.append('-Wredundant-decls')
        cflags.append('-Wshadow')
        cflags.append('-Wsign-conversion')
        cflags.append('-Wstrict-prototypes')
        cflags.append('-Wswitch-enum')
        cflags.append('-Wundef')
        cflags.append('-Wuninitialized')
        cflags.append('-Wunreachable-code')
        cflags.append('-Wunused')
        cflags.append('-Wwrite-strings')

        if cc_major == '4':
            cflags.append('-Wno-c11-extensions')
            cflags.append('-Wunsafe-loop-optimizations')

        if cc_major == '5':
            cflags.append('-Wunsafe-loop-optimizations')

        if cc_major == '6':
            cflags.append('-Wunsafe-loop-optimizations')

        if cc_major >= '5':
            cflags.append('-Wformat-signedness')
            cflags.append('-Wjump-misses-init')
            cflags.append('-Wredundant-decls')
            cflags.append('-Wunused-macros')
            cflags.append('-Wvariadic-macros')

        if cc_major >= '6':
            cflags.append('-Wduplicated-cond')
            cflags.append('-Wlogical-op')
            cflags.append('-Wunused-const-variable=2')

        if cc_major >= '7':
            cflags.append('-Walloca')
            cflags.append('-Walloc-zero')
            cflags.append('-Wduplicated-branches')
            cflags.append('-Wformat-overflow=2')
            cflags.append('-Wformat-truncation=2')
            cflags.append('-Wrestrict')

        if cc_major >= '8':
            cflags.append('-Wmultistatement-macros')

    else:
        cflags.append('-Weverything')
        cflags.append('-Werror')
        cflags.append('-Wno-cast-align')
        cflags.append('-Wno-documentation-unknown-command')
        cflags.append('-Wno-format-nonliteral')
        cflags.append('-Wno-packed')
        cflags.append('-Wno-padded')
        cflags.append('-Wno-reserved-id-macro')
        cflags.append('-Wno-zero-length-array')


        # We should turn on cast-qual, but not as a fatal error: see WT-2690. For now, leave it off.
        cflags.append('-Wno-cast-qual')

        # Turn off clang thread-safety-analysis, it doesn't like some of WiredTiger's code patterns.
        cflags.append('-Wno-thread-safety-analysis')

        # On Centos 7.3.1611, system header files aren't compatible with -Wdisabled-macro-expansion.
        cflags.append('-Wno-disabled-macro-expansion')

        # We occasionally use an extra semicolon to indicate an empty loop or conditional body.
        cflags.append('-Wno-extra-semi-stmt')

        # Ignore unrecognized options.
        cflags.append('-Wno-unknown-warning-option')

    conf.env.Append(CFLAGS = cflags)
