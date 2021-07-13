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
from wtscenario import make_scenarios
from wiredtiger import stat, WT_NOTFOUND

def timestamp_str(t):
    return '%x' % t

# test_cursor17.py
# Test optimization that skips page during cursor traversal if it determines that all records on the
# page have been deleted with a tombstone visible to the current transaction.

class test_cursor17(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB,statistics=(all)'
    session_config = 'isolation=snapshot'

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def test_cursor_skip_pages(self):
        uri = 'table:test_cursor17'
        create_params = 'key_format=i,value_format=S'
        self.session.create(uri, create_params)

        value1 = 'a' * 500
        total_keys = 40000

        # Keep oldest timestamp pinned.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1))
        self.conn.set_timestamp('stable_timestamp=' + timestamp_str(2))
        cursor = self.session.open_cursor(uri)

        commit_timestamp = 2

        # Insert few thousand key-value pairs.
        for key in range(total_keys):
            self.session.begin_transaction()
            cursor[key] = value1
            self.session.commit_transaction('commit_timestamp=' + timestamp_str(commit_timestamp))
            commit_timestamp += 1

        # Delete everything on the table except for the first and the last KV pair.
        for key in range(total_keys):
            if key not in [0, total_keys - 1]:
                self.session.begin_transaction()
                cursor.set_key(key)
                self.assertEqual(cursor.remove(),0)
                self.session.commit_transaction('commit_timestamp=' + timestamp_str(commit_timestamp))
                commit_timestamp += 1

        # Take a checkpoint to reconcile the pages.
        self.session.checkpoint()

        self.session.begin_transaction('read_timestamp=' + timestamp_str(commit_timestamp))
        # Position the cursor on the first record.
        cursor.set_key(0)
        self.assertEqual(cursor.search(), 0)
        # This should move the cursor to the last record.
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), total_keys-1)

        # Check if we skipped any pages while moving the cursor.
        #
        # I ran this test few times and the stat appeared to be in range of ~1400. I have
        # put 1000 to be on safe side but this number sould be recalculated if we change the number
        # of keys in the test table.
        self.assertGreater(self.get_stat(stat.conn.cursor_next_skip_pages), 1000)