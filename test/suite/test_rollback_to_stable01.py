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

import time
from helper import copy_wiredtiger_home
import wiredtiger, wttest
from wtdataset import SimpleDataSet
from wiredtiger import stat
from wtscenario import make_scenarios

def timestamp_str(t):
    return '%x' % t

# test_rollback_to_stable01.py
# Shared base class used by rollback to stable tests.
class test_rollback_to_stable_base(wttest.WiredTigerTestCase):
    def large_updates(self, uri, value, ds, nrows, prepare, commit_ts):
        # Update a large number of records.
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(1, nrows + 1):
            session.begin_transaction()
            cursor[ds.key(i)] = value
            if commit_ts == 0:
                session.commit_transaction()
            elif prepare:
                session.prepare_transaction('prepare_timestamp=' + timestamp_str(commit_ts-1))
                session.timestamp_transaction('commit_timestamp=' + timestamp_str(commit_ts))
                session.timestamp_transaction('durable_timestamp=' + timestamp_str(commit_ts+1))
                session.commit_transaction()
            else:
                session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def large_modifies(self, uri, value, ds, location, nbytes, nrows, prepare, commit_ts):
        # Load a slight modification.
        session = self.session
        cursor = session.open_cursor(uri)
        session.begin_transaction()
        for i in range(1, nrows + 1):
            cursor.set_key(i)
            mods = [wiredtiger.Modify(value, location, nbytes)]
            self.assertEqual(cursor.modify(mods), 0)

        if commit_ts == 0:
            session.commit_transaction()
        elif prepare:
            session.prepare_transaction('prepare_timestamp=' + timestamp_str(commit_ts-1))
            session.timestamp_transaction('commit_timestamp=' + timestamp_str(commit_ts))
            session.timestamp_transaction('durable_timestamp=' + timestamp_str(commit_ts+1))
            session.commit_transaction()
        else:
            session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def large_removes(self, uri, ds, nrows, prepare, commit_ts):
        # Remove a large number of records.
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(1, nrows + 1):
            session.begin_transaction()
            cursor.set_key(i)
            cursor.remove()
            if commit_ts == 0:
                session.commit_transaction()
            elif prepare:
                session.prepare_transaction('prepare_timestamp=' + timestamp_str(commit_ts-1))
                session.timestamp_transaction('commit_timestamp=' + timestamp_str(commit_ts))
                session.timestamp_transaction('durable_timestamp=' + timestamp_str(commit_ts+1))
                session.commit_transaction()
            else:
                session.commit_transaction('commit_timestamp=' + timestamp_str(commit_ts))
        cursor.close()

    def check(self, check_value, uri, nrows, read_ts):
        session = self.session
        if read_ts == 0:
            session.begin_transaction()
        else:
            session.begin_transaction('read_timestamp=' + timestamp_str(read_ts))
        cursor = session.open_cursor(uri)
        count = 0
        for k, v in cursor:
            self.assertEqual(v, check_value)
            count += 1
        session.commit_transaction()
        self.assertEqual(count, nrows)
        cursor.close()

# Test that rollback to stable clears the remove operation.
class test_rollback_to_stable01(test_rollback_to_stable_base):
    session_config = 'isolation=snapshot'

    key_format_values = [
        ('column', dict(key_format='r')),
        ('integer_row', dict(key_format='i')),
    ]

    in_memory_values = [
        ('no_inmem', dict(in_memory=False)),
        ('inmem', dict(in_memory=True))
    ]

    prepare_values = [
        ('no_prepare', dict(prepare=False)),
        ('prepare', dict(prepare=True))
    ]

    scenarios = make_scenarios(key_format_values, in_memory_values, prepare_values)

    def conn_config(self):
        config = 'cache_size=50MB,statistics=(all)'
        if self.in_memory:
            config += ',in_memory=true'
        else:
            config += ',log=(enabled),in_memory=false'
        return config

    def test_rollback_to_stable(self):
        nrows = 10000

        # Prepare transactions for column store table is not yet supported.
        if self.prepare and self.key_format == 'r':
            self.skipTest('Prepare transactions for column store table is not yet supported')

        # Create a table without logging.
        uri = "table:rollback_to_stable01"
        ds = SimpleDataSet(
            self, uri, 0, key_format=self.key_format, value_format="S", config='log=(enabled=false)')
        ds.populate()

        # Pin oldest and stable to timestamp 1.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(1) +
            ',stable_timestamp=' + timestamp_str(1))

        valuea = "aaaaa" * 100
        self.large_updates(uri, valuea, ds, nrows, self.prepare, 10)
        # Check that all updates are seen.
        self.check(valuea, uri, nrows, 10)

        # Remove all keys with newer timestamp.
        self.large_removes(uri, ds, nrows, self.prepare, 20)
        # Check that the no keys should be visible.
        self.check(valuea, uri, 0, 20)

        # Pin stable to timestamp 20 if prepare otherwise 10.
        if self.prepare:
            self.conn.set_timestamp('stable_timestamp=' + timestamp_str(20))
        else:
            self.conn.set_timestamp('stable_timestamp=' + timestamp_str(10))
        # Checkpoint to ensure that all the updates are flushed to disk.
        if not self.in_memory:
            self.session.checkpoint()

        self.conn.rollback_to_stable()
        # Check that the new updates are only seen after the update timestamp.
        self.check(valuea, uri, nrows, 20)

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        calls = stat_cursor[stat.conn.txn_rts][2]
        hs_removed = stat_cursor[stat.conn.txn_rts_hs_removed][2]
        keys_removed = stat_cursor[stat.conn.txn_rts_keys_removed][2]
        keys_restored = stat_cursor[stat.conn.txn_rts_keys_restored][2]
        pages_visited = stat_cursor[stat.conn.txn_rts_pages_visited][2]
        upd_aborted = stat_cursor[stat.conn.txn_rts_upd_aborted][2]
        stat_cursor.close()

        self.assertEqual(calls, 1)
        self.assertEqual(hs_removed, 0)
        self.assertEqual(keys_removed, 0)
        if self.in_memory:
            self.assertEqual(upd_aborted, nrows)
        else:
            self.assertEqual(upd_aborted + keys_restored, nrows)
        self.assertGreaterEqual(keys_restored, 0)
        self.assertGreater(pages_visited, 0)

if __name__ == '__main__':
    wttest.run()
