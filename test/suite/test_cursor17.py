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
# test_cursor17.py
#   Cursors: return correct error in case of write conflict on insert.
#

import wttest
from wiredtiger import stat, wiredtiger_strerror, WiredTigerError, WT_ROLLBACK
from wtscenario import make_scenarios

class test_cursor17(wttest.WiredTigerTestCase):
    tablename = 'test_write_conflict'
    uri = 'table:' + tablename

    conn_config = 'cache_size=32M'

    scenarios = make_scenarios([
        ('lsm', dict(extra_config=',type=lsm')),
        ('row', dict(extra_config='')),
    ])

    def test_cursor17(self):

        value = "ABC" * 500

        self.session.create(self.uri, 'key_format=i,value_format=S' + self.extra_config)

        # Open a cursor, insert a range of keys and keep the transaction running.
        self.session.begin_transaction('isolation=snapshot')
        cursor = self.session.open_cursor(self.uri, None, "overwrite=false")
        for i in range(100):
            cursor.set_key(i)
            cursor.set_value(value)
            self.assertEqual(cursor.insert(), 0)
        cursor.close()

        # Open another session and with it a new cursor.
        session2 = self.conn.open_session('isolation=snapshot')
        cursor2 = session2.open_cursor(self.uri, None, "overwrite=false")
        session2.begin_transaction()

        # Insert the same keys as before in this new transaction. We should get a rollback error.
        for i in range(100):
            cursor2.set_key(i)
            cursor2.set_value(value)
            try:
                cursor2.insert()
            except WiredTigerError as e:
                rollback_str = wiredtiger_strerror(WT_ROLLBACK)
                if rollback_str not in str(e):
                    raise(e)

        # Inserting keys that don't conflict shouldn't raise an error.
        for i in range(100,200):
            cursor2.set_key(i)
            cursor2.set_value(value)
            cursor2.insert()

        # Cleanup
        self.session.rollback_transaction()
        session2.rollback_transaction()

if __name__ == '__main__':
    wttest.run()