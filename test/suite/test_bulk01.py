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
#
# test_bulk.py
#       bulk-cursor test.
#

import wiredtiger, wttest
from wtdataset import simple_key, simple_value
from wtscenario import make_scenarios
from wiredtiger import stat

# Smoke test bulk-load.
class test_bulk_load(wttest.WiredTigerTestCase):
    name = 'test_bulk'

    types = [
        ('file', dict(type='file:')),
        ('table', dict(type='table:'))
    ]
    keyfmt = [
        ('integer', dict(keyfmt='i')),
        ('string', dict(keyfmt='S')),
    ]
    valfmt = [
        ('integer', dict(valfmt='i')),
        ('string', dict(valfmt='S')),
    ]
    scenarios = make_scenarios(types, keyfmt, valfmt)

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    # Test a simple bulk-load
    def test_bulk_load(self):
        uri = self.type + self.name
        self.session.create(uri,
            'key_format=' + self.keyfmt + ',value_format=' + self.valfmt)

        self.assertEqual(self.get_stat(stat.conn.cursor_bulk_count), 0)
        cursor = self.session.open_cursor(uri, None, "bulk")
        self.assertEqual(self.get_stat(stat.conn.cursor_bulk_count), 1)

        for i in range(1, 1000):
            cursor[simple_key(cursor, i)] = simple_value(cursor, i)
        cursor.close()

        self.assertEqual(self.get_stat(stat.conn.cursor_bulk_count), 0)

    # Test that bulk-load out-of-order fails.
    def test_bulk_load_order_check(self):
        uri = self.type + self.name
        self.session.create(uri,
            'key_format=' + self.keyfmt + ',value_format=' + self.valfmt)
        cursor = self.session.open_cursor(uri, None, "bulk")
        cursor[simple_key(cursor, 10)] = simple_value(cursor, 10)

        for i in [1, 9, 10]:
            cursor.set_key(simple_key(cursor, 1))
            cursor.set_value(simple_value(cursor, 1))
            msg = '/less than or equal to the previously inserted key/'
            self.assertRaisesWithMessage(
                wiredtiger.WiredTigerError, lambda: cursor.insert(), msg)

        cursor[simple_key(cursor, 11)] = simple_value(cursor, 11)

    # Test that row-store bulk-load out-of-order can succeed.
    def test_bulk_load_row_order_nocheck(self):
        # Row-store offers an optional fast-past that skips the relatively
        # expensive key-order checks, used when the input is known to be
        # correct. Column-store comparisons are cheap, so it doesn't have
        # that fast-path support.
        if self.keyfmt != 'S':
                return

        uri = self.type + self.name
        self.session.create(uri,
            'key_format=' + self.keyfmt + ',value_format=' + self.valfmt)
        cursor = self.session.open_cursor(uri, None, "bulk,skip_sort_check")
        cursor[simple_key(cursor, 10)] = simple_value(cursor, 10)
        cursor[simple_key(cursor, 1)] = simple_value(cursor, 1)

        if not wiredtiger.diagnostic_build():
            self.skipTest('requires a diagnostic build')

        # Close explicitly, there's going to be a failure.
        msg = '/are incorrectly sorted/'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.conn.close(), msg)

    # Test bulk-load only permitted on newly created objects.
    def test_bulk_load_not_empty(self):
        uri = self.type + self.name
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri, None)
        cursor[simple_key(cursor, 1)] = simple_value(cursor, 1)
        # Close the insert cursor, else we'll get EBUSY.
        cursor.close()
        self.session.checkpoint()
        msg = '/bulk-load is only supported on newly created objects/'
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(uri, None, "bulk"), msg)

    # Test that bulk-load objects cannot be opened by other cursors.
    def test_bulk_load_busy(self):
        uri = self.type + self.name
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri, None)
        cursor[simple_key(cursor, 1)] = simple_value(cursor, 1)
        # Don't close the insert cursor, we want EBUSY.
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(uri, None, "bulk"))
