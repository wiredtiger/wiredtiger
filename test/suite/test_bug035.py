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
# test_bug35.py
# This python test uses control points to reproduce WT-12349.
# 1. Populate data in the table
# 2. Create tombstones on 50% of the table
# 3. Force evict the tombstone data onto disk.
# 4. Enable control point to control the execution of the concurrency between two threads
# 5. Create two threads. One thread T1 will perform reads under isolation read-uncommitted and
# another thread T2 will generate an update and a modify.
# 6. Pause T1 when the thread starts to reconstruct the modify in the update list.
# 7. Once T2 generates an update, modify and finishes rollback, signal T1 to continue.
# 8. T1 is now in an invalid state and should assert.
class test_bug35(wttest.WiredTigerTestCase):
    uri = 'table:test_bug035'
    conn_config = 'cache_size=500MB,statistics=(all)'
    nrows = 100

    def evict_cursor(self, uri, nrows):
        s = self.conn.open_session()
        s.begin_transaction()
        # Configure debug behavior on a cursor to evict the page positioned on when the reset API is used.
        evict_cursor = s.open_cursor(uri, None, "debug=(release_evict)")
        for i in range(1, nrows):
            evict_cursor.set_key(i)
            evict_cursor.search()
            evict_cursor.reset()
        s.rollback_transaction()
        evict_cursor.close()

    def construct_modify_upd_list(self):
        session = self.setUpSessionOpen(self.conn)
        cursor = session.open_cursor(self.uri)
        session.begin_transaction("isolation=read-uncommitted")
        # 5. Pause thread due to control point when it starts to reconstruct the modify in the update list.
        cursor.set_key(1)
        self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: cursor.search(), '/conflict between concurrent operations/')

        session.rollback_transaction()
        cursor.close()
        session.close()
        self.ignoreStderrPatternIfExists("Read-uncommitted readers")

    def test_bug35(self):
        create_params = 'key_format=r,value_format=S'
        self.session.create(self.uri, create_params)
        value1 = 'a' * 500
        value2 = 'b' * 500

        # 1. Populate data in the data store table.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[i] = value1
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))

        # 2. Create tombstones on second half of the data in the table.
        self.session.begin_transaction()
        for i in range(1, self.nrows // 2):
            cursor.set_key(i)
            self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(6))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(7))

        # 3. Force evict everything from in-memory to on disk.
        self.evict_cursor(self.uri, self.nrows)
        self.session.checkpoint()

        # 4. Enable control point to control the execution of the concurrency between two threads
        self.conn.enable_control_point(6, "")

        # 5. Create another thread which will perform reads under isolation read-uncommitted.
        read_uncommitted_thread = threading.Thread(target=self.construct_modify_upd_list)
        
        # Add in a update and a modify.
        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[i] = value2

            cursor.set_key(i)
            mods = [wiredtiger.Modify("b", 0, 1)]
            self.assertEquals(cursor.modify(mods), 0)

        read_uncommitted_thread.start()
        # Wait for read-uncommited thread to reconstruct the modify before calling rollback.
        modify_reconstruct = False
        while not modify_reconstruct:
            stat_cursor = self.session.open_cursor('statistics:', None, None)
            modify_reconstruct = stat_cursor[stat.conn.txn_modify_reconstruct_uncommited][2] != 0
            stat_cursor.close()
            time.sleep(0.1)

        # 6. Once T2 generates an update, modify and finishes rollback, signal T1 to continue.
        self.session.rollback_transaction()
        # 7. T1 is now in an invalid state and should assert at this point.
        read_uncommitted_thread.join()
        self.captureerr.check(self)
