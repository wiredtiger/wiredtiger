#!/usr/bin/env python
#
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

from packing import pack, unpack


def check(fmt, *v):
    v = list(v)
    packed = pack(fmt, *v)
    unpacked = unpack(fmt, packed)
    if unpacked == v:
        result = 'PASS!'
    else:
        result = 'FAIL!'
    print '* %s: %s' % (fmt, result)


def check_verbose(fmt, *v):
    v = list(v)
    print '* %s as %s' % (repr(v), fmt)
    packed = pack(fmt, *v)
    print '** packed: ', ''.join('%02x' % ord(c) for c in packed), packed
    unpacked = unpack(fmt, packed)
    print '** unpacked: ', unpacked
    if unpacked == v:
        print '** PASS!'
    else:
        print '** FAIL!'


if __name__ == '__main__':
    import sys
    if 'verbose' in sys.argv:
        check = check_verbose
    check('iii', 0, 101, -99)
    check('3i', 0, 101, -99)
    check('iS', 42, "forty two")
    check('9SS', "forty two", "spam egg")
    check('u', r"\x42" * 20)
    check('uu', r"\x42" * 10, r"\x42" * 10)
    check('3u', r"\x4")
    check('3uu', r"\x4", r"\x42" * 10)
    check('u3u', r"\x42" * 10, r"\x4")
    check('s', "4")
    check("2s", "42")
