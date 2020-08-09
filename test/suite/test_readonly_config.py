#!/usr/bin/env python
#
# Public Domain 2014-2020 MongoDB, Inc.
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
from wtdataset import SimpleDataSet
import os, shutil


# test_readonly_config.py
# test to create a test database, close all the connections and re-open it with readonly mode.
class test_readonly_config(wttest.WiredTigerTestCase):
    conn_config = ('cache_size=50MB')

    # Create a table.
    uri = "table:test_readonly"

    def test_readonly_config(self):
        nrows = 100
        ds = SimpleDataSet(self, self.uri, nrows, key_format="S", value_format='u')
        ds.populate()
        value = b"aaaaa" * 10

        # Initially load huge data
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, 10000):
            cursor.set_key(ds.key(nrows + i))
            cursor.set_value(value)
            self.assertEquals(cursor.insert(), 0)
        cursor.close()
        self.session.checkpoint()

        # Search the inserted values.
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, 10000):
            cursor.set_key(ds.key(nrows + i))
            self.assertEquals(cursor.search(), 0)
        cursor.close()
        self.conn.close()

        shutil.copy('WiredTiger.turtle', 'WiredTiger.turtle.set')

        # Open wiredtiger in readonly mode
        conn = self.wiredtiger_open(self.home, "readonly")
        conn.close()

if __name__ == '__main__':
    wttest.run()
