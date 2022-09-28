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
        config = 'cache_size=1MB,statistics=(all),log=(enabled=true)'
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
        self.session.create(uri, 'key_format=S,value_format=S')

        # Insert value A1 @ 15.
        key = 'A' * 100
        value = 'A1' * 100
        commit_ts = 15
        self.insert_kv(uri, key, value, commit_ts)

        # Update value A1 to A2 @ 20.
        value = 'A2' * 100
        commit_ts = 20
        self.insert_kv(uri, key, value, commit_ts)

        # Persist to disk, there should be A2 on the DS and A1 in the HS.
        self.session.checkpoint()

        # TODO - Is it necessary?
        # Perform eviction, there should be A2 on the DS and A1 in the HS.
        # self.evict(uri, key)

        # Move the oldest and stable timestamps @ 25 to make A1 and A2 globally visible.
        stable_ts = 25
        oldest_ts = 25
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(stable_ts) +
                                ',stable_timestamp=' + self.timestamp_str(oldest_ts))

        # Perform eviction, this should reset the time window now the update is globally visible.
        self.evict(uri, key)

        # Update value A2 -> A3 @ 30.
        value = 'A3' * 100
        commit_ts = 30
        self.insert_kv(uri, key, value, commit_ts)

        # Perform a checkpoint to move add A2 to the HS and replace it with A3 on the DS.
        self.session.checkpoint()

        # TODO - Is it necessary?
        # self.evict(uri, key)

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

