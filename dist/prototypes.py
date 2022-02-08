#!/usr/bin/env python

# Generate WiredTiger function prototypes.
import fnmatch, re
from dist import compare_srcfile, format_srcfile, source_files

def clean_function_name(filename, fn):
    ret = fn.strip()

    # Ignore statics in XXX.c files.
    if fnmatch.fnmatch(filename, "*.c") and 'static' in ret:
        return None

    # Join the first two lines, type and function name.
    ret = ret.replace("\n", " ", 1)

    # If there's no CPP syntax, join everything.
    if not '#endif' in ret:
        ret = " ".join(ret.split())

    # If it's not an inline function, prefix with "extern".
    if not 'inline' in ret:
        ret = 'extern ' + ret

    # Switch to the include file version of any gcc attributes.
    ret = ret.replace("WT_GCC_FUNC_ATTRIBUTE", "WT_GCC_FUNC_DECL_ATTRIBUTE")

    # Everything but void requires using any return value.
    if not re.match(r'(static inline|extern) void', ret):
        ret = ret + " WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result))"

    # If a line ends in #endif, appending a semicolon results in an illegal
    # expression, force an appended newline.
    if re.match(r'#endif$', ret):
        ret = ret + '\n'

    return ret + ';\n'

# Build function prototypes from a list of files.
def fn_prototypes(fns, tests, name):
    s = open(name, 'r').read()
    for p in re.findall(r'\n[A-Za-z_].*\n__wt_[^{]*', s):
        clean = clean_function_name(name, p)
        if clean is not None:
            fns.append(clean)

    s = open(name, 'r').read()
    for p in re.findall(r'\n[A-Za-z_].*\n__ut_[^{]*', s):
        clean = clean_function_name(name, p)
        if clean is not None:
            tests.append(clean)

# Write results and compare to the current file.
def output(fns, tests, f):
    tmp_file = '__tmp'
    tfile = open(tmp_file, 'w')
    for e in sorted(list(set(fns))):
        tfile.write(e)

    tfile.write('\n#ifdef HAVE_UNITTEST\n')
    for e in sorted(list(set(tests))):
        tfile.write(e)
    tfile.write('\n#endif\n')

    tfile.close()
    format_srcfile(tmp_file)
    compare_srcfile(tmp_file, f)

# Update generic function prototypes.
def prototypes_extern():
    fns = []
    tests = []
    for name in source_files():
        if not fnmatch.fnmatch(name, '*.c') + fnmatch.fnmatch(name, '*_inline.h'):
                continue;
        if fnmatch.fnmatch(name, '*/checksum/arm64/*'):
            continue
        if fnmatch.fnmatch(name, '*/checksum/power8/*'):
            continue
        if fnmatch.fnmatch(name, '*/checksum/riscv64/*'):
            continue
        if fnmatch.fnmatch(name, '*/checksum/zseries/*'):
            continue
        if fnmatch.fnmatch(name, '*/os_posix/*'):
            continue
        if fnmatch.fnmatch(name, '*/os_win/*'):
            continue
        if fnmatch.fnmatch(name, '*/ext/*'):
            continue
        fn_prototypes(fns, tests, name)

    output(fns, tests, "../src/include/extern.h")

# Update POSIX-specific function prototypes.
def prototypes_posix():
    fns = []
    tests = []
    for name in source_files():
        if not fnmatch.fnmatch(name, '*.c') + fnmatch.fnmatch(name, '*_inline.h'):
                continue;
        if not fnmatch.fnmatch(name, '*/os_posix/*'):
            continue
        fn_prototypes(fns, tests, name)

    output(fns, tests, "../src/include/extern_posix.h")

# Update Windows-specific function prototypes.
def prototypes_win():
    fns = []
    tests = []
    for name in source_files():
        if not fnmatch.fnmatch(name, '*.c') + fnmatch.fnmatch(name, '*_inline.h'):
                continue;
        if not fnmatch.fnmatch(name, '*/os_win/*'):
            continue
        fn_prototypes(fns, tests, name)

    output(fns, tests, "../src/include/extern_win.h")

prototypes_extern()
prototypes_posix()
prototypes_win()
