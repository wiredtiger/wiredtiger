import re
from SCons.Script import *
from wt_platform import *

def wt_config(env):
    # Build a zero-length wiredtiger_config.h file so SCons and autoconf can co-exist.
    # XXX: Autoconf compatibility.
    env.Textfile(
        target='wiredtiger_config.h',
        source=[])

    # Build the wiredtiger.h file.
    version_file = 'build_posix/aclocal/version-set.m4'
    VERSION_MAJOR = None
    VERSION_MINOR = None
    VERSION_PATCH = None
    VERSION_STRING = None

    # Read the version information from the version-set.m4 file
    for l in open(File(version_file).srcnode().abspath):
        if re.match(r'^VERSION_[A-Z]+', l):
            exec(l)

    if (VERSION_MAJOR == None or
        VERSION_MINOR == None or
        VERSION_PATCH == None or
        VERSION_STRING == None):
        print("Failed to find version variables in " + version_file)
        Exit(1)

    replacements = {
        '@VERSION_MAJOR@' : VERSION_MAJOR,
        '@VERSION_MINOR@' : VERSION_MINOR,
        '@VERSION_PATCH@' : VERSION_PATCH,
        '@VERSION_STRING@' : VERSION_STRING,
        '@uintmax_t_decl@': "",             # XXX: Autoconf compatibility.
        '@uintptr_t_decl@': "",             # XXX: Autoconf compatibility.
    }
    if os_windows:                          # XXX: Autoconf compatibility.
        replacements.update({'@off_t_decl@' : 'typedef int64_t wt_off_t;'})
    else:
        replacements.update({'@off_t_decl@' : 'typedef off_t wt_off_t;'})

    env.Substfile(
        target='wiredtiger.h',
        source=[
            'src/include/wiredtiger.in',
        ],
        SUBST_DICT=replacements)
