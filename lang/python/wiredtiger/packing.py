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
# WiredTiger variable-length packing and unpacking functions
"""Packing and unpacking functions

The format string use the following conversion table:

Format  Python  Notes

  x 	 N/A   	pad byte, no associated value
  b 	 int 	signed byte
  B 	 int 	unsigned byte
  h 	 int 	signed 16-bit
  H 	 int 	unsigned 16-bit
  i 	 int 	signed 32-bit
  I 	 int 	unsigned 32-bit
  l 	 int 	signed 32-bit
  L 	 int 	unsigned 32-bit
  q 	 int 	signed 64-bit
  Q 	 int 	unsigned 64-bit
  r  	 int 	record number
  s  	 str 	fixed-length string
  S  	 str 	NUL-terminated string
  t  	 int 	fixed-length bit field
  u  	 str 	raw byte array
"""

from intpacking import pack_int, unpack_int


def __get_type(fmt):
    if not fmt:
        return None, fmt
    # Variable-sized encoding is the default (and only supported format in v1)
    if fmt[0] in '.@<>':
        tfmt = fmt[0]
        fmt = fmt[1:]
    else:
        tfmt = '.'
    return tfmt, fmt


def unpack(fmt, string):
    tfmt, fmt = __get_type(fmt)
    if not fmt:
        return ()
    if tfmt != '.':
        raise ValueError('Only variable-length encoding is currently supported')
    result = []
    havesize = size = 0
    for offset, char in enumerate(fmt):
        if char.isdigit():
            size = (size * 10) + int(char)
            havesize = 1
            continue
        elif char == 'x':
            size = size if havesize else 1
            string = string[size:]
            havesize = size = 0
        elif char == 'S':
            size = size if havesize else string.find('\0')
            result.append(string[:size])
            string = string[size+1:]
            havesize = size = 0
        elif char == 's':
            size = size if havesize else 1
            result.append(string[:size])
            string = string[size:]
            havesize = size = 0
        elif char == 'u' and (not havesize):
            if offset == len(fmt) - 1:
                size = len(string)
            else:
                size, string = unpack_int(string)
            result.append(string[:size])
            string = string[size:]
            havesize = size = 0
        elif char == 'u':   # and havesize
            result.append(string[:size])
            string = string[size:]
            havesize = size = 0
        elif char == 't':
            # bit type always stored as byte
            result.append(ord(string[0]))
            string = string[1:]
            havesize = size = 0
        else:
            # integral type
            size = size if havesize else 1
            for j in xrange(size):
                v, string = unpack_int(string)
                result.append(v)
            havesize = size = 0
    return result


def pack(fmt, *values):
    tfmt, fmt = __get_type(fmt)
    if not fmt:
            return ()
    if tfmt != '.':
            raise ValueError('Only variable-length encoding is currently supported')
    result = ''
    havesize = i = size = 0
    for offset, char in enumerate(fmt):
        if char.isdigit():
            size = (size * 10) + int(char)
            havesize = 1
        elif char == 'x':
            size = size if havesize else 1
            result += '\0' * size
            # Note: no value, don't increment i
            havesize = size = 0
        elif char == 'S':
            value = values[i]
            if '\0' in value:
                l = value.find('\0')
            else:
                l = len(value)
            if havesize and l > size:
                l = size
            result += value[:l]
            if not havesize:
                result += '\0'
            elif size > l:
                result += '\0' * (size - l)
            havesize = size = 0
            i += 1
        elif char == 's':
            value = values[i]
            l = len(value)
            if havesize and l > size:
                l = size
            elif not havesize:
                havesize = size = 1
            result += value[:l]
            if size > l:
                result += '\0' * (size - l)
            havesize = size = 0
            i += 1
        elif char == 'u':
            value = values[i]
            l = len(value)
            if havesize and l > size:
                l = size
            elif not havesize and offset != (len(fmt) - 1):
                result += pack_int(l)
            result += value[:l]
            if size > l:
                result += '\0' * (size - l)
            havesize = size = 0
            i += 1
        elif char == 't':
            # bit type, size is number of bits
            size = size if havesize else 1
            if size > 8:
                raise ValueError("bit count cannot be greater than 8 for 't' encoding")
            mask = (1 << size) - 1
            value = values[i]
            if (mask & value) != value:
                raise ValueError("value out of range for '%st' encoding" % size)
            result += chr(value)
            havesize = size = 0
            i += 1
        else:
            # integral type
            size = size if havesize else 1
            for j in xrange(size):
                result += pack_int(values[i])
                i += 1
            havesize = size = 0
    return result
