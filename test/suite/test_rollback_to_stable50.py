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

from helper import simulate_crash_restart
from test_rollback_to_stable01 import test_rollback_to_stable_base

# test_rollback_to_stable50.py
# Simple reproducer for WT-9870.
class test_rollback_to_stable50(test_rollback_to_stable_base):
    session_config = 'isolation=snapshot'

    def conn_config(self):
        # TODO - Is it required?
        config = 'cache_size=1MB,statistics=(all),log=(enabled=true),statistics_log=(wait=1,on_close=1)'
        return config

    def insert_kv(self, uri, key, value, ts):
        self.session.begin_transaction()
        cursor = self.session.open_cursor(uri)
        cursor[key] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    def evict(self, uri, key):
        self.session.begin_transaction()
        cursor = self.session.open_cursor(uri, None, "debug=(release_evict)")
        cursor.set_key(key)
        self.assertEquals(cursor.search(), 0)
        cursor.reset()
        self.session.rollback_transaction()
        cursor.close()

    def test_rollback_to_stable(self):

        # Create a table.
        uri = 'table:aaa'
        self.session.create(uri, 'key_format=S,value_format=S,log=(enabled=false)')


        # Setup global oldest and stable
        stable_ts = 10
        oldest_ts = 10
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(oldest_ts) +
                                ',stable_timestamp=' + self.timestamp_str(stable_ts))

        # Insert value A1 @ 15.
        key = 'A' * 100
        value = 'A1' * 100
        commit_ts = 15
        self.insert_kv(uri, key, value, commit_ts)

        # Update value A1 to A2 @ 20.
        value = 'A2' * 100
        commit_ts = 20
        self.insert_kv(uri, key, value, commit_ts)

        # Create a checkpoint that will force A1 to be written to the history store
        stable_ts = 22
        oldest_ts = 17
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(oldest_ts) +
                                ',stable_timestamp=' + self.timestamp_str(stable_ts))

        # Perform eviction, there should be A2 on the DS and A1 in the HS (in cache).
        self.evict(uri, key)
        # Get the history store page reconciled, oldest needs to be held back here, or the
        # reconciliation would remove the content from the history store.
        self.session.checkpoint()

        # Eviction isn't enough to force a rewrite here, since the page is clean. Add another
        # record on the page so a reconciliation happens
        self.insert_kv(uri, 'B' * 10, 'B2' * 10, 23)

        # Move the oldest and stable timestamps @ 25 to make A1 and A2 globally visible.
        stable_ts = 25
        oldest_ts = 25
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(oldest_ts) +
                                ',stable_timestamp=' + self.timestamp_str(stable_ts))
        # Evict the page with the now globally visible key
        self.evict(uri, key)
        self.session.checkpoint()
        exit(1)

        # Update value A2 -> A3 @ 30.
        value = 'A3' * 100
        commit_ts = 30
        self.insert_kv(uri, key, value, commit_ts)

        # Persist A3 to disk - this should move A2 to the history store, with a 0 timestamp
        self.evict(uri, key)
        exit(1)

        # Perform a checkpoint so that rollback to stable will try to replace A3 with A2 on
        # recovery
        self.session.checkpoint()

        # Simulate a crash, this will execute RTS using the stable timestamp 25.
        # We expect RTS to discard A3 since its commit timestamp is more recent than stable
        # timestamp. A2 should be set to the DS and A1 should remain in the HS.
        simulate_crash_restart(self, ".", "RESTART")

        # Verify the theory.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(25))
        cursor = self.session.open_cursor(uri)
        cursor.set_key(key)
        # Without the fix WT-9870, we get A1? Or nothing because the checkpoint of RTS deletes it?
        # With the fix of WT-9870, we should get the latest stable update A2.
        self.assertEquals(cursor.search(), 0)
        expected_value = 'A2' * 100
        self.assertEquals(cursor.get_value(), expected_value)
        self.session.rollback_transaction()
        cursor.close()

if __name__ == '__main__':
    wttest.run()

