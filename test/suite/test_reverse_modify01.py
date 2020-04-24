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

import wiredtiger, wttest, time

def timestamp_str(t):
    return '%x' % t

# test_reverse_modify01.py
# Verify reverse modify traversal after eviction
class test_reverse_modify01(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=2MB,statistics=(all),eviction=(threads_max=1)'
    session_config = 'isolation=snapshot'

    def test_reverse_modifies_fails_visibility_check_without_timestamps(self):
        uri = "table:test_reverse_modify01_notimestamp"
        create_params = 'value_format=S,key_format=i'
        value1 = 'abcedfghijklmnopqrstuvwxyz' * 5
        value2 = 'b' * 100
        value3 = 'c' * 100
        value4 = 'd' * 100
        valuebig = 'e' * 1000
        self.session.create(uri, create_params)
        cursor = self.session.open_cursor(uri)

        session2 = self.setUpSessionOpen(self.conn)
        session2.create(uri, create_params)
        cursor2 = session2.open_cursor(uri)

        # Insert a full value.
        self.session.begin_transaction()
        cursor[1] = value1
        self.session.commit_transaction()

        # Insert a modify
        self.session.begin_transaction()
        cursor.set_key(1)
        cursor.modify([wiredtiger.Modify('A', 130, 1)])
        self.session.commit_transaction()

        # Validate that we do see the correct value.
        session2.begin_transaction()
        cursor2.set_key(1)
        cursor2.search()
        self.assertEquals(cursor2.get_value(),  value1 + 'A')
        session2.commit_transaction()

        # Begin transaction on session 2 so it sees and current snap_min snap_max
        session2.begin_transaction()

        # reset the cursor
        cursor2.reset()

        # Insert two more values
        self.session.begin_transaction()
        cursor.set_key(1)
        cursor[1] = value3
        self.session.commit_transaction()

        self.session.begin_transaction()
        cursor.set_key(1)
        cursor[1] = value4
        self.session.commit_transaction()

        self.session.checkpoint()

        # Insert a whole bunch of data into the table to force wiredtiger to evict data
        # from the previous table.
        self.session.begin_transaction()
        for i in range(2, 10000):
            cursor[i] = valuebig
        self.session.commit_transaction()

        # Try to find the value we saw earlier
        cursor2.set_key(1)
        cursor2.search()
        self.session.breakpoint()
        self.assertEquals(cursor2.get_value(), value1 + 'A')

if __name__ == '__main__':
    wttest.run()