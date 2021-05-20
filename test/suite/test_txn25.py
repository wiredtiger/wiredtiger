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
# test_txn24.py
#   Test the write generation mechanism to ensure that transaction ids get wiped between runs.
#

import wiredtiger, wttest

class test_txn25(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB,log=(enabled),statistics=(all)'
    session_config = 'isolation=snapshot'

    def test_txn25(self):
        uri = 'file:test_txn25'
        create_config = 'allocation_size=512,key_format=S,value_format=S'
        self.session.create(uri, create_config)

        # Populate the file and ensure that we start seeing some high transaction IDs in the system.
        value1 = 'aaaaa' * 100
        value2 = 'bbbbb' * 100
        value3 = 'ccccc' * 100

        # Keep transaction ids around.
        session2 = self.conn.open_session()
        session2.begin_transaction()

        cursor = self.session.open_cursor(uri)
        for i in range(1, 1000):
            self.session.begin_transaction()
            cursor[str(i)] = value1
            self.session.commit_transaction()

        for i in range(1, 1000):
            self.session.begin_transaction()
            cursor[str(i)] = value2
            self.session.commit_transaction()

        for i in range(1, 1000):
            self.session.begin_transaction()
            cursor[str(i)] = value3
            self.session.commit_transaction()

        session2.rollback_transaction()
        session2.close()

        # Close and re-open the connection.
        cursor.close()
        self.conn.close()
        self.conn = wiredtiger.wiredtiger_open(self.home, self.conn_config)
        self.session = self.conn.open_session(self.session_config)

        # Now that we've reopened, check that we can view the latest data from the previous run.
        #
        # Since we've restarted the system, our transaction IDs are going to begin from 1 again
        # so we have to wipe the cell's transaction IDs in order to see them.
        cursor = self.session.open_cursor(uri)
        self.session.begin_transaction()
        for i in range(1, 1000):
            self.assertEqual(cursor[str(i)], value3)
        self.session.rollback_transaction()
