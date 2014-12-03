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
            size = size if havesize else 1
            result.append(string[:size])
            string = string[size+1:]
            havesize = size = 0
        elif char == 's':
            size = size if havesize else s.find('\0')
            result.append(string[:size])
            string = string[size:]
            havesize = size = 0
        elif char == 'u':
            if not havesize:
                if offset == len(fmt) - 1:
                    size = len(string)
                else:
                    size, string = unpack_int(string)
            result.append(string[:size])
            string = string[size:]
            havesize = size = 0
        elif char == 't':
            # bit type, size is number of bits
            if not havesize:
                    size = 1
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
            if '\0' in values[i]:
                l = values[i].find('\0')
            else:
                l = len(values[i])
            if havesize and l > size:
                l = size
            result += values[i][:l]
            if not havesize:
                result += '\0'
            elif size > l:
                result += '\0' * (size - l)
            i += 1
            havesize = size = 0
        elif char == 's':
            l = len(values[i])
            if havesize:
                if l > size:
                    l = size
            else:
                havesize = size = 1
            result += values[i][:l]
            if size > l:
                result += '\0' * (size - l)
            i += 1
            havesize = size = 0
        elif char == 'u':
            l = len(values[i])
            if havesize:
                if l > size:
                    l = size
            elif offset != len(fmt) - 1:
                result += pack_int(l)
            result += values[i][:l]
            if size > l:
                result += '\0' * (size - l)
            i += 1
            havesize = size = 0
        elif char == 't':
            # bit type, size is number of bits
            size = size if havesize else 1
            if size > 8:
                raise ValueError("bit count cannot be greater than 8 for 't' encoding")
            mask = (1 << size) - 1
            val = values[i]
            if (mask & val) != val:
                raise ValueError("value out of range for 't' encoding")
            result += chr(val)
            i += 1
            havesize = size = 0
        else:
            # integral type
            size = size if havesize else 1
            for j in xrange(size):
                result += pack_int(values[i])
                i += 1
            havesize = size = 0
    return result
