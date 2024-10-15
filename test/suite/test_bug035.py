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

import wiredtiger, wttest, threading
# test_bug35.py
# This tests for the scenario discovered in WT-12602.
# Before WT-12602, it was possible that evicting a page when checkpoint is happening parallel would
# lead to an incorrect EBUSY error. The page that was getting evicted required a modify to be
# written to history store and the oldest update in the history store as a globally visible tombstone.
# For this condition to occur, we must have the following:
# - The history page must be reconciled and written to disk
# - One transaction needs to have an modify and update on the record.
# - There needs to be a globally visible tombstone that is older than the modify
# and update on the record.
class test_bug35(wttest.WiredTigerTestCase):
    # Configure debug behavior on evict, where eviction threads would evict as if checkpoint was in
    # parallel.
    uri = 'table:test_bug033'
    conn_config = 'cache_size=500MB,statistics=(all)'
    nrows = 100

    def construct_modify_upd_list(self):
        session = self.setUpSessionOpen(self.conn)
        cursor = session.open_cursor(self.uri)
        session.begin_transaction("isolation=read-uncommitted")
        for i in range(1, self.nrows):
            cursor.set_key(str(i))
            cursor.search()
        session.commit_transaction()
        cursor.close()

        session.close()

    def test_ts(self):
        create_params = 'key_format=S,value_format=S'
        self.session.create(self.uri, create_params)
        value1 = 'a' * 500
        value2 = 'b' * 500

        # Populate data in the data store table.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[str(i)] = value1
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(5))

        # Add in a tombstone.
        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor.set_key(str(i))
            self.assertEqual(cursor.remove(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(6))

        self.conn.enable_control_point(7, "")
        thread = threading.Thread(target=self.construct_modify_upd_list)

        thread.start()
        # Add in a update and a modify.
        self.session.begin_transaction()
        for i in range(1, self.nrows):
            cursor[str(i)] = value2

            cursor.set_key(str(i))
            mods = [wiredtiger.Modify("b", 0, 1)]
            self.assertEquals(cursor.modify(mods), 0)
        self.session.rollback_transaction()
        thread.join()
