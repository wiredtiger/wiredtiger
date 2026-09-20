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

# Exercise the split point API surface: it exists on a cursor, reports one
# partition for a range that cannot be split (with the leading key included),
# zero keys for a range with no keys, and rejects a cursor type that cannot
# support it. split_points returns 0 on success or an error; the number of
# keys is obtained by iterating get_split_point.
class test_split_points01(wttest.WiredTigerTestCase):
    uri = 'table:split_points01'
    conn_config = 'cache_size=50MB'

    def count_split_points(self, cursor):
        n = 0
        while cursor.get_split_point() is not None:
            n += 1
        return n

    def test_empty_table_has_no_split_points(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        self.assertEqual(cursor.split_points(4, 0), 0)
        self.assertEqual(self.count_split_points(cursor), 0)

    def test_no_bounds_is_the_whole_table(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, 1001):
            cursor[i] = 'v' * 20
        # A single small table lives in one page, so there is no boundary to
        # sample: one key, one partition, and the caller scans it serially.
        self.assertEqual(cursor.split_points(4, 0), 0)
        self.assertEqual(self.count_split_points(cursor), 1)

    def test_column_store_is_not_supported(self):
        self.session.create('table:split_points01c', 'key_format=r,value_format=S')
        cursor = self.session.open_cursor('table:split_points01c')
        cursor.set_key(1)
        cursor.set_value('v')
        cursor.insert()
        # The ENOTSUP is intentional: mark the logged stderr line as expected.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: cursor.split_points(4, 0), '/split points not supported/')

if __name__ == '__main__':
    wttest.run()
