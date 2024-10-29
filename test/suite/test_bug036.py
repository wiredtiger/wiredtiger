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

import wiredtiger, wttest, threading, time
from wiredtiger import stat
# test_bug36.py
# This python test uses control points to reproduce WT-10905.
# 1. Created a record in the table
# 2. Remove the record, creating a tombstone.
# 3. Enable control points.
# 4. Create another thread T2 which will perform a read under isolation read-uncommitted.
# 5. The main thread will generate an update followed by a modify for the key.
# 6. The main thread then waits till T2 reaches a control point in modify.c indicating it has
# reconstructed a modify.
# 7. The main thread wakes and T2 waits while the main thread rolls back it's transaction.
# 7. T2 wakes and asserts.

class test_bug36(wttest.WiredTigerTestCase):
    uri = 'table:test_bug036'
    conn_config = 'cache_size=500MB,statistics=(all)'
    wt_conn_control_point_id_thread_wait_for_upd_abort = 6

    def construct_modify_upd_list(self):
        session = self.setUpSessionOpen(self.conn)
        cursor = session.open_cursor(self.uri)
        session.begin_transaction("isolation=read-uncommitted")
        cursor.set_key(str(0))

        # Search for the record, this will trigger two control points in succession.
        self.assertRaisesException(wiredtiger.WiredTigerError,
            lambda: cursor.search(), '/conflict between concurrent operations/')

        # If the WT_RET_MSG on modify.c:442 is commented out the test will abort, and the below code
        # will be unreachable.
        session.commit_transaction()
        self.ignoreStderrPatternIfExists("Read-uncommitted readers")

    def test_bug36(self):
        create_params = 'key_format=S,value_format=S'
        self.session.create(self.uri, create_params)
        value1 = 'a' * 500
        value2 = 'b' * 500

        # Add a record.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[str(0)] = value1
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))

        # Create a tombstone.
        self.session.begin_transaction()
        cursor.set_key(str(0))
        self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(6))

        # Enable control point to control the execution of the concurrency between two threads.
        self.conn.enable_control_point(6, "")

        # Create another thread which will perform reads under isolation read-uncommitted.
        read_uncommitted_thread = threading.Thread(target=self.construct_modify_upd_list)

        # Add an update and a modify.
        self.session.begin_transaction()
        cursor[str(0)] = value2
        cursor.set_key(str(0))
        mods = [wiredtiger.Modify("b", 0, 1)]
        self.assertEquals(cursor.modify(mods), 0)

        # Start thread T2.
        read_uncommitted_thread.start()
        self.conn.control_point_thread_barrier(self.wt_conn_control_point_id_thread_wait_for_upd_abort)

        # Rollback the transaction and then wake T2.
        self.session.rollback_transaction()
        self.conn.control_point_thread_barrier(self.wt_conn_control_point_id_thread_wait_for_upd_abort)

        read_uncommitted_thread.join()
