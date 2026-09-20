#!/usr/bin/env python
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

import wiredtiger, wttest
from wiredtiger import WiredTigerError

# Exercise the split point computation: descent + internal-page sampling. The
# count getter must agree with what get_split_point iterates, max_points is
# honored (0 means no keys), and the returned keys are real, ascending,
# in-range, and begin with the exact first key at or after the lower bound.
#
# Integer keys make the assertions exact: WT's record-store separator keys are
# the full child first keys, so a boundary decoded back to an int is the actual
# first record of a page, and the leading key is the record at the lower bound.
class test_split_points02(wttest.WiredTigerTestCase):
    uri = 'table:split_points02'
    conn_config = 'cache_size=50MB'

    def create_populated(self, n=10000):
        self.session.create(self.uri, 'key_format=i,value_format=S,leaf_page_max=4KB')
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, n + 1):
            cursor[i] = 'v' * 10
        # Reconcile: split the single in-memory leaf, so the tree has internal
        # pages with real boundaries to sample.
        self.session.checkpoint()
        return cursor

    def collect(self, cursor):
        keys = []
        while True:
            k = cursor.get_split_point()
            if k is None:
                break
            keys.append(wiredtiger.unpack('i', k)[0])
        return keys

    def test_count_getter(self):
        cursor = self.create_populated()
        # Iterate the keys, then check the count getter reports the same total.
        self.assertEqual(cursor.split_points(8, 0), 0)
        self.assertEqual(cursor.get_split_point_count(), len(self.collect(cursor)))
        # A second call recomputes from scratch and still agrees.
        self.assertEqual(cursor.split_points(8, 0), 0)
        self.assertEqual(cursor.get_split_point_count(), len(self.collect(cursor)))

    def test_max_points_zero_returns_no_keys(self):
        cursor = self.create_populated()
        # max_points <= 0 means no keys: the range is not partitioned.
        self.assertEqual(cursor.split_points(0, 0), 0)
        self.assertEqual(self.collect(cursor), [])
        self.assertEqual(cursor.get_split_point_count(), 0)
        self.assertEqual(cursor.split_points(-1, 0), 0)
        self.assertEqual(self.collect(cursor), [])

    def test_bounded_range_keys(self):
        cursor = self.create_populated()
        lo, hi = 10, 9900
        cursor.set_key(lo)
        cursor.bound('action=set,bound=lower')
        cursor.set_key(hi)
        cursor.bound('action=set,bound=upper')
        self.assertEqual(cursor.split_points(8, 0), 0)
        keys = self.collect(cursor)
        # A multi-page range yields more than one partition.
        self.assertGreater(len(keys), 1)
        # Up to max_points keys are returned.
        self.assertLessEqual(len(keys), 8)
        # The leading key is exactly the first key at or after the lower bound.
        self.assertEqual(keys[0], lo)
        # Keys are strictly ascending and strictly inside the requested range.
        for i in range(len(keys) - 1):
            self.assertLess(keys[i], keys[i + 1])
        for k in keys:
            self.assertGreaterEqual(k, lo)
            self.assertLessEqual(k, hi)

    def test_range_inside_one_leaf_returns_one_key(self):
        cursor = self.create_populated()
        # A range that falls entirely inside a single leaf page cannot be split.
        cursor.set_key(1)
        cursor.bound('action=set,bound=lower')
        cursor.set_key(2)
        cursor.bound('action=set,bound=upper')
        self.assertEqual(cursor.split_points(8, 0), 0)
        keys = self.collect(cursor)
        self.assertEqual(len(keys), 1)
        self.assertEqual(keys[0], 1)

if __name__ == '__main__':
    wttest.run()
