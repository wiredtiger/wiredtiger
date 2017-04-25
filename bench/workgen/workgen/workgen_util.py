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
# workgen_util.py
#      Utility functions that are mixed into the workgen namespace

import glob, os, sys
from wiredtiger import *
from workgen import *

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

def show_buckets(title, mult, buckets, n):
    shown = False
    s = title + ': '
    for count in range(0, n):
        val = buckets[count]
        if val != 0:
            if shown:
                s += ','
            s += str(count*mult) + '=' + str(val)
            shown = True
    print(s)

def latency_preprocess(arr, merge):
    mx = 0
    cur = 0
    # SWIG arrays have a clunky interface
    for i in range(0, arr.__len__()):
        if i % merge == 0:
            cur = 0
        cur += arr[i]
        if cur > mx:
            mx = cur
    arr.height = mx

def latency_plot(box, ch, left, width, arr, merge, scale):
    pos = 0
    for x in range(0, width):
        t = 0
        for i in range(0, merge):
            t += arr[pos]
            pos += 1
        nch = scale * t
        y = 0
        thisch = ch
        while nch > 0.0:
            if nch < 1.0:
                thisch = '.'
            box[y][left + x] = thisch
            nch -= 1.0
            y += 1

def latency_optype(name, t):
    if t.ops == 0:
        return
    if t.latency_ops == 0:
        print('**** ' + name + ' operations: ' + str(t.ops))
        return
    print('**** ' + name + ' operations: ' + str(t.ops) + \
          ', latency operations: ' + str(t.latency_ops))
    print('  avg: ' + str(t.latency/t.latency_ops) + \
          ', min: ' + str(t.min_latency) + ', max: ' + str(t.max_latency))
    us = t.us()
    ms = t.ms()
    sec = t.sec()
    latency_preprocess(us, 40)
    latency_preprocess(ms, 40)
    latency_preprocess(sec, 4)
    max_height = max(us.height, ms.height, sec.height)
    if max_height == 0:
        return
    height = 20    # 20 chars high
    # a list of a list of characters
    box = [list(' ' * 80) for x in range(height)]
    scale = (1.0 / (max_height + 1)) * height
    latency_plot(box, 'u', 0,  25, us, 40, scale)
    latency_plot(box, 'm', 27, 25, ms, 40, scale)
    latency_plot(box, 's', 54, 25, sec, 4, scale)
    box.reverse()
    for line in box:
        print(''.join(line))
    dash25 = '-' * 25
    print('  '.join([dash25] * 3))
    print(' 0 - 999 us (40/bucket)     1 - 999 ms (40/bucket)     ' + \
          '1 - 99 sec (4/bucket)')
    print('')
    show_buckets(name + ' us', 1, us, 1000)
    show_buckets(name + ' ms', 1000, ms, 1000)
    show_buckets(name + ' sec', 1000000, sec, 100)
    print('')

def workload_latency(workload):
    latency_optype('insert', workload.stats.insert)
    latency_optype('read', workload.stats.read)
    latency_optype('remove', workload.stats.remove)
    latency_optype('update', workload.stats.update)
    latency_optype('truncate', workload.stats.truncate)
    latency_optype('not found', workload.stats.not_found)
