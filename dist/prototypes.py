#!/usr/bin/env python3

# Generate WiredTiger function prototypes.
import fnmatch, re, os, sys
from dist import compare_srcfile, format_srcfile, source_files
from common_functions import filter_if_fast

if not [f for f in filter_if_fast(source_files(), prefix="../")]:
    sys.exit(0)

def clean_function_name(filename, fn):
    ret = fn.strip()

    # Ignore statics in XXX.c files.
    if fnmatch.fnmatch(filename, "*.c") and 'static' in ret:
        return None

    # Join the first two lines, type and function name.
    # Drop the whitespace change. They break comments appended to external functions
    # TODO - Clean this up. We can be smarter about not processing the comment.
    # ret = ret.replace("\n", " ", 1)
    # # If there's no CPP syntax, join everything.
    # if not '#endif' in ret:
    #     ret = " ".join(ret.split())

    # If it's not an inline function, prefix with "extern".
    if 'inline' not in ret and 'WT_INLINE' not in ret:
        if ret.startswith("/*"):
            # We need to append extern *after* the comment
            end_of_comment = ret.find("*/")
            ret = ret[:end_of_comment + 2] + "extern " + ret[end_of_comment + 2:]
        else:
            ret = 'extern ' + ret

    # Switch to the include file version of any gcc attributes.
    ret = ret.replace("WT_GCC_FUNC_ATTRIBUTE", "WT_GCC_FUNC_DECL_ATTRIBUTE")

    # Everything but void requires using any return value.
    if not re.match(r'(static inline|static WT_INLINE|extern) void', ret):
        ret = ret + " WT_GCC_FUNC_DECL_ATTRIBUTE((warn_unused_result))"

    # If a line ends in #endif, appending a semicolon results in an illegal
    # expression, force an appended newline.
    if re.match(r'#endif$', ret):
        ret = ret + '\n'

    if "/*" in ret:
        # Add an extra line between functions when printing their comment. It looks cleaner
        return ret + ';\n\n'
    else:
        return ret + ';\n'

# Find function prototypes in a file matching a given regex. Cleans the
# function names to the point being immediately usable.
def extract_prototypes(filename, regexp):
    ret = []
    s = open(filename, 'r').read()
    for p in re.findall(regexp, s):
        clean = clean_function_name(filename, p)
        if clean is not None:
            ret.append(clean)

    return ret

# Build function prototypes from a list of files.
def fn_prototypes(ext_fns, int_fns, tests, name):
    preceding_comment = r'/\*\n(?: \*.*\n)+ \*/'
    for sig in extract_prototypes(name, preceding_comment + r'\n[A-Za-z_].*\n__wt_[^{]*'):
        ext_fns.append(sig)

    for sig in extract_prototypes(name, r'\n[A-Za-z_].*\n__wti_[^{]*'):
        int_fns.append(sig)

    for sig in extract_prototypes(name, r'\n[A-Za-z_].*\n__ut_[^{]*'):
        tests.append(sig)

# Write results and compare to the current file.
# Unit-testing functions are exposed separately in their own section to
# allow them to be ifdef'd out.
def output(ext_fns, tests, f):
    tmp_file = '__tmp_prototypes' + str(os.getpid())
    tfile = open(tmp_file, 'w')
    tfile.write("#pragma once\n\n")
    for e in sorted(list(set(ext_fns))):
        tfile.write(e)

    tfile.write('\n#ifdef HAVE_UNITTEST\n')
    for e in sorted(list(set(tests))):
        tfile.write(e)
    tfile.write('\n#endif\n')

    tfile.close()
    format_srcfile(tmp_file)
    compare_srcfile(tmp_file, f)

from collections import defaultdict

# Update generic function prototypes.
def prototypes_extern():
    ext_func_dict = defaultdict(list)
    int_func_dict = defaultdict(list)
    test_dict = defaultdict(list)

    for name in source_files():
        if not fnmatch.fnmatch(name, '*.c') + fnmatch.fnmatch(name, '*_inline.h'):
            continue
        if fnmatch.fnmatch(name, '*/checksum/arm64/*'):
            continue
        if fnmatch.fnmatch(name, '*/checksum/loongarch64/*'):
            continue
        if fnmatch.fnmatch(name, '*/checksum/power8/*'):
            continue
        if fnmatch.fnmatch(name, '*/checksum/riscv64/*'):
            continue
        if fnmatch.fnmatch(name, '*/checksum/zseries/*'):
            continue
        if fnmatch.fnmatch(name, '*/checksum/*'):
            # TODO - checksum is a multi-level directory and this script assumes a flat hierarchy.
            # For now throw these functions into extern.h where they were already
            fn_prototypes(ext_func_dict["include"], int_func_dict["include"], test_dict["include"], name)
            continue
        if re.match(r'^.*/os_(?:posix|win|linux|darwin)/.*', name):
            # Handled separately in prototypes_os().
            continue
        if fnmatch.fnmatch(name, '*/ext/*'):
            continue

        if fnmatch.fnmatch(name, '../src/*'):
            # NOTE: This assumes a flat directory with no subdirectories
            comp = os.path.basename(os.path.dirname(name))
            fn_prototypes(ext_func_dict[comp], int_func_dict[comp], test_dict[comp], name)
        else:
            print(f"Unexpected filepath {name}")
            exit(1)


    for comp in ext_func_dict.keys():
        if comp == "include":
            assert(len(int_func_dict["include"]) == 0)
            output(ext_func_dict[comp], test_dict[comp], f"../src/include/extern.h")
        else:
            # print(f"#include \"../{comp}/{comp}.h\"")
            output(ext_func_dict[comp], test_dict[comp], f"../src/{comp}/{comp}.h")
            if len(int_func_dict[comp]) > 0:
                # empty dict for tests. These functions only exist to expose code to unit tests
                output(int_func_dict[comp], {}, f"../src/{comp}/{comp}_internal.h")
        
def prototypes_os():
    """
    The operating system abstraction layer duplicates function names. So each 
    os gets its own extern header file.
    """
    ports = 'posix win linux darwin'.split()
    fns = {k:[] for k in ports}
    tests = {k:[] for k in ports}
    for name in source_files():
        if m := re.match(r'^.*/os_(posix|win|linux|darwin)/.*', name):
            port = m.group(1)
            assert port in ports
            # TODO - just using the same fns dict for internal and external
            fn_prototypes(fns[port], fns[port], tests[port], name)

    for p in ports:
        output(fns[p], tests[p], f"../src/include/extern_{p}.h")

prototypes_extern()
prototypes_os()
