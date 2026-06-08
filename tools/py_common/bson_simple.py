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
#
# bson_simple.py
#       A minimal, read-only BSON element reader -- just enough to walk a
#       document. This is deliberately NOT a BSON library: it does not encode,
#       does not build documents, and hands ObjectId / Decimal128 back as raw
#       bytes. It exists so tools that only need to *traverse* a BSON document
#       (e.g. mongod FTDC metrics chunks) can do so with no third-party
#       dependency, keeping the BSON type/size table in exactly one place. If you
#       need real BSON (encoding, full type fidelity, codec options), use pymongo.

import struct

# BSON element type bytes (the subset that appears in mongod FTDC).
DOUBLE = 0x01
STRING = 0x02
DOC = 0x03
ARRAY = 0x04
BINARY = 0x05
OID = 0x07
BOOL = 0x08
DATETIME = 0x09
NULL = 0x0A
INT32 = 0x10
TIMESTAMP = 0x11
INT64 = 0x12
DECIMAL128 = 0x13


def _read_value(buf, p, t):
    """Return (value, next_offset) for the value of type t at buf[p].

    Embedded documents and arrays return their *start offset* (recurse with
    iter_document); binary returns its payload bytes; timestamp returns
    (increment, seconds); ObjectId / Decimal128 return raw bytes; other scalars
    return their natural Python value."""
    if t == DOUBLE:
        return struct.unpack_from("<d", buf, p)[0], p + 8
    if t == STRING:
        n = struct.unpack_from("<i", buf, p)[0]  # length includes the trailing NUL
        return buf[p + 4:p + 4 + n - 1].decode("utf-8", "replace"), p + 4 + n
    if t == DOC or t == ARRAY:
        return p, p + struct.unpack_from("<i", buf, p)[0]
    if t == BINARY:
        n = struct.unpack_from("<i", buf, p)[0]
        return buf[p + 5:p + 5 + n], p + 5 + n  # skip the 4-byte length and 1 subtype byte
    if t == OID:
        return buf[p:p + 12], p + 12
    if t == BOOL:
        return buf[p] != 0, p + 1
    if t == DATETIME:
        return struct.unpack_from("<q", buf, p)[0], p + 8  # ms since epoch
    if t == NULL:
        return None, p
    if t == INT32:
        return struct.unpack_from("<i", buf, p)[0], p + 4
    if t == TIMESTAMP:
        inc, sec = struct.unpack_from("<II", buf, p)
        return (inc, sec), p + 8
    if t == INT64:
        return struct.unpack_from("<q", buf, p)[0], p + 8
    if t == DECIMAL128:
        return buf[p:p + 16], p + 16
    raise ValueError("unhandled BSON type 0x%02x" % t)


def iter_document(buf, start):
    """Yield (name_bytes, type_byte, value) for each element of the BSON document
    whose length prefix is at buf[start]. The names are raw bytes (callers decode
    if needed); see _read_value for how each value type is represented. For DOC /
    ARRAY the value is the embedded document's start offset -- recurse with
    iter_document(buf, value)."""
    end = start + struct.unpack_from("<i", buf, start)[0]
    p = start + 4
    while p < end - 1:
        t = buf[p]
        p += 1
        ke = buf.index(0, p)
        name = buf[p:ke]
        p = ke + 1
        value, p = _read_value(buf, p, t)
        yield name, t, value
