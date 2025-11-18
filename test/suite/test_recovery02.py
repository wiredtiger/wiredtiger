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

import wttest, wiredtiger
from suite_subprocess import suite_subprocess

# test_recovery02.py
#    Simulate crashes during a table's creation and check that the table state is still valid.
class test_recovery02(wttest.WiredTigerTestCase, suite_subprocess):
    conn_config = 'log=(enabled=true)'

    tablename = 'table:test_recovery02'

    def create_table(self):
        self.session.create(self.tablename, 'key_format=5s,value_format=HQ,exclusive=true')

    # Run in a subprocess, expected to call abort
    def subprocess_func(self):
        self.conn.reconfigure("debug_mode=(crash_point_colgroup=true)")
        self.create_table() # Expected to fail

    def test_populate_subprocess(self):
        self.close_conn()

        subdir = 'SUBPROCESS'
        [ignore_result, new_home_dir] = self.run_subprocess_function(subdir,
            'test_recovery02.test_recovery02.subprocess_func', silent=True)

        self.conn = self.setUpConnectionOpen(new_home_dir)
        self.session = self.setUpSessionOpen(self.conn)

        self.conn.reconfigure("debug_mode=(crash_point_colgroup=false)")

        # Try to open a cursor, expected to return WT_NOTFOUND
        self.assertRaises(wiredtiger.WiredTigerError, lambda: self.session.open_cursor(self.tablename))

        # Try to drop the table, expected to return WT_NOTFOUND
        self.assertRaises(wiredtiger.WiredTigerError, lambda: self.session.drop(self.tablename))

        # Since the table creation hasn't been completed properly, we expect table creation to create
        # it the second time properly.
        self.create_table()
