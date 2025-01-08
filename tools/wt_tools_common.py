#!/usr/bin/env python3
#
# Public Domain 2014-present MongoDB, Inc.
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

# Common functions used by python scripts in this directory.

import sys

################################################################
# Borrowed from intpacking.py, with small adjustments.
# Variable-length integer packing
# need: up to 64 bits, both signed and unsigned
#
# Try hard for small values (up to ~2 bytes), after that, just encode the
# length in the first byte.
#
#  First byte | Next |                        |
#  byte       | bytes| Min Value              | Max Value
# ------------+------+------------------------+--------------------------------
# [00 00xxxx] | free | N/A                    | N/A
# [00 01llll] | 8-l  | -2^64                  | -2^13 - 2^6
# [00 1xxxxx] | 1    | -2^13 - 2^6            | -2^6 - 1
# [01 xxxxxx] | 0    | -2^6                   | -1
# [10 xxxxxx] | 0    | 0                      | 2^6 - 1
# [11 0xxxxx] | 1    | 2^6                    | 2^13 + 2^6 - 1
# [11 10llll] | l    | 2^13 + 2^6             | 2^64 - 1
# [11 11xxxx] | free | N/A                    | N/A

NEG_MULTI_MARKER = 0x10
NEG_2BYTE_MARKER = 0x20
NEG_1BYTE_MARKER = 0x40
POS_1BYTE_MARKER = 0x80
POS_2BYTE_MARKER = 0xc0
POS_MULTI_MARKER = 0xe0

NEG_1BYTE_MIN = -2**6
NEG_2BYTE_MIN = -2**13 + NEG_1BYTE_MIN
POS_1BYTE_MAX = 2**6 - 1
POS_2BYTE_MAX = 2**13 + POS_1BYTE_MAX

_python3 = (sys.version_info >= (3, 0, 0))

if not _python3:
    raise Exception('This script requires Python 3')

def _ord(b):
    return b

def _getbits(x, start, end=0):
    '''return the least significant bits of x, from start to end'''
    return (x & ((1 << start) - 1)) >> (end)

def _get_int(b, size):
    r = 0
    for i in range(size):
        r = (r << 8) | _ord(b[i])
    return r

def unpack_int(b):
    marker = _ord(b[0])
    if marker < NEG_MULTI_MARKER or marker >= 0xf0:
        raise Exception('Not a packed integer')
    elif marker < NEG_2BYTE_MARKER:
        sz = 8 - _getbits(marker, 4)
        if sz < 0:
            raise Exception('Not a valid packed integer')
        part1 = (-1 << (sz << 3))
        part2 = _get_int(b[1:], sz)
        part3 = b[sz+1:]
        return (part1 | part2, part3)
    elif marker < NEG_1BYTE_MARKER:
        return (NEG_2BYTE_MIN + ((_getbits(marker, 5) << 8) | _ord(b[1])), b[2:])
    elif marker < POS_1BYTE_MARKER:
        return (NEG_1BYTE_MIN + _getbits(marker, 6), b[1:])
    elif marker < POS_2BYTE_MARKER:
        return (_getbits(marker, 6), b[1:])
    elif marker < POS_MULTI_MARKER:
        return (POS_1BYTE_MAX + 1 +
               ((_getbits(marker, 5) << 8) | _ord(b[1])), b[2:])
    else:
        sz = _getbits(marker, 4)
        return (POS_2BYTE_MAX + 1 + _get_int(b[1:], sz), b[sz+1:])

################################################################
