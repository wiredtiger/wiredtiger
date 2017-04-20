#!/usr/bin/env python
#
# Public Domain 2014-2016 MongoDB, Inc.
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
# __init__.py
#      initialization for workgen module
#
import glob, os, sys

# After importing the SWIG-generated file, copy all symbols from from it
# to this module so they will appear in the workgen namespace.
me = sys.modules[__name__]
sys.path.append(os.path.dirname(__file__))  # needed for Python3
import workgen
for name in dir(workgen):
    value = getattr(workgen, name)
    setattr(me, name, value)

def txn(op, config=None):
    t = Transaction(config)
    op._transaction = t
    return op

def wiredtiger_builddir():
    thisdir = os.path.dirname(__file__)
    wt_disttop = os.path.dirname(os.path.dirname(os.path.dirname(thisdir)))

    # Check for a local build that contains the wt utility. First check in
    # current working directory, then in build_posix and finally in the disttop
    # directory. This isn't ideal - if a user has multiple builds in a tree we
    # could pick the wrong one.
    if os.path.isfile(os.path.join(os.getcwd(), 'wt')):
        builddir = os.getcwd()
    elif os.path.isfile(os.path.join(wt_disttop, 'wt')):
        builddir = wt_disttop
    elif os.path.isfile(os.path.join(wt_disttop, 'build_posix', 'wt')):
        builddir = os.path.join(wt_disttop, 'build_posix')
    elif os.path.isfile(os.path.join(wt_disttop, 'wt.exe')):
        builddir = wt_disttop
    else:
        raise Exception('Unable to find useable WiredTiger build')
    return builddir

# Return the wiredtiger_open extension argument for any needed shared library.
# Called with a list of extensions, e.g.
#    [ 'compressors/snappy', 'encryptors/rotn=config_string' ]
def extensions_config(exts):
    result = ''
    extfiles = {}
    errpfx = 'extensions_config'
    builddir = wiredtiger_builddir()
    for ext in exts:
        extconf = ''
        if '=' in ext:
            splits = ext.split('=', 1)
            ext = splits[0]
            extconf = '=' + splits[1]
        splits = ext.split('/')
        if len(splits) != 2:
            raise Exception(errpfx + ": " + ext +
                ": extension is not named <dir>/<name>")
        libname = splits[1]
        dirname = splits[0]
        pat = os.path.join(builddir, 'ext',
            dirname, libname, '.libs', 'libwiredtiger_*.so')
        filenames = glob.glob(pat)
        if len(filenames) == 0:
            raise Exception(errpfx +
                ": " + ext +
                ": no extensions library found matching: " + pat)
        elif len(filenames) > 1:
            raise Exception(errpfx + ": " + ext +
                ": multiple extensions libraries found matching: " + pat)
        complete = '"' + filenames[0] + '"' + extconf
        if ext in extfiles:
            if extfiles[ext] != complete:
                raise Exception(errpfx +
                    ": non-matching extension arguments in " +
                    str(exts))
        else:
            extfiles[ext] = complete
    if len(extfiles) != 0:
        result = ',extensions=[' + ','.join(extfiles.values()) + ']'
    return result
