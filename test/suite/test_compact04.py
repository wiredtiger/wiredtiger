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

import fnmatch, os, shutil, time
from helper import simulate_crash_restart
from test_rollback_to_stable01 import test_rollback_to_stable_base
from wiredtiger import stat
from wiredtiger import stat, wiredtiger_strerror, WiredTigerError, WT_ROLLBACK
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# test_compact04.py
# Test to run compact and rollback to stable operation that performs as expected following a server crash and
# recovery. Verify that
#   (a) the on-disk value is replaced by the correct value from the history store, and
#   (b) newer updates are removed.
class test_compact04(test_rollback_to_stable_base):
    session_config = 'isolation=snapshot'

    key_format_values = [
        ('integer_row', dict(key_format='i')),
    ]

    prepare_values = [
        ('no_prepare', dict(prepare=False)),
        ('prepare', dict(prepare=True))
    ]

    compact_action = [
        ('before_crash', dict(before_crash=True)),
        ('after_crash', dict(before_crash=False))
    ]
    scenarios = make_scenarios(key_format_values, prepare_values, compact_action)

    # Return the size of the file
    def getSize(self, uri):
        # To allow this to work on systems without ftruncate,
        # get the portion of the file allocated, via 'statistics=(all)',
        # not the physical file size, via 'statistics=(size)'.
        cstat = self.session.open_cursor(
            'statistics:' + uri, None, 'statistics=(all)')
        sz1 = cstat[stat.dsrc.block_size][2]
        cstat.close()
        return sz1

    def large_removes(self, uri, ds, start, nrows, prepare, commit_ts):
        # Remove a large number of records.
        session = self.session
        try:
            cursor = session.open_cursor(uri)
            for i in range(start, nrows + 1):
                session.begin_transaction()
                cursor.set_key(i)
                cursor.remove()
                if commit_ts == 0:
                    session.commit_transaction()
                elif prepare:
                    session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(commit_ts-1))
                    session.timestamp_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
                    session.timestamp_transaction('durable_timestamp=' + self.timestamp_str(commit_ts+1))
                    session.commit_transaction()
                else:
                    session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
            cursor.close()
        except WiredTigerError as e:
            rollback_str = wiredtiger_strerror(WT_ROLLBACK)
            if rollback_str in str(e):
                session.rollback_transaction()
            raise(e)

    # Return stats that track the progress of compaction.
    def getCompactProgressStats(self, uri):
        cstat = self.session.open_cursor(
            'statistics:' + uri, None, 'statistics=(all)')
        statDict = {}
        statDict["pages_reviewed"] = cstat[stat.dsrc.btree_compact_pages_reviewed][2]
        statDict["pages_skipped"] = cstat[stat.dsrc.btree_compact_pages_skipped][2]
        statDict["pages_selected"] = cstat[stat.dsrc.btree_compact_pages_write_selected][2]
        statDict["pages_rewritten"] = cstat[stat.dsrc.btree_compact_pages_rewritten][2]
        cstat.close()
        return statDict

    def conn_config(self):
        config = 'cache_size=5MB,statistics=(all),log=(enabled=true)'
        return config

    def call_compact(self, uri):
        mb = 1024 * 1024
        self.assertEqual(self.session.compact(uri), 0)
        sizeAfterCompact = self.getSize(uri)
        self.pr('After Compact ' + str(sizeAfterCompact // mb) + 'MB')

        statDict = self.getCompactProgressStats(uri)
        self.assertGreater(statDict["pages_reviewed"],0)
        self.assertGreater(statDict["pages_selected"],0)
        self.assertGreater(statDict["pages_rewritten"],0)

        self.pr('pages_reviewed ' + str(statDict["pages_reviewed"]))
        self.pr('pages_selected ' + str(statDict["pages_selected"]))
        self.pr('pages_rewritten ' + str(statDict["pages_rewritten"]))


    def test_compact04(self):
        nrows = 80000
        mb = 1024 * 1024

        # Create a table without logging.
        uri = "table:test_compact04"
        ds = SimpleDataSet(
            self, uri, 0, key_format=self.key_format, value_format="S", config='log=(enabled=false)')
        ds.populate()

        # Pin oldest and stable to timestamp 10.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10) +
            ',stable_timestamp=' + self.timestamp_str(10))

        value_a = "aaaaa" * 100
        value_b = "bbbbb" * 100
        value_c = "ccccc" * 100
        value_d = "ddddd" * 100

        # Perform several updates.
        self.large_updates(uri, value_d, ds, nrows, self.prepare, 20)
        self.large_updates(uri, value_c, ds, nrows, self.prepare, 30)
        self.large_updates(uri, value_b, ds, nrows, self.prepare, 40)
        self.large_updates(uri, value_a, ds, nrows, self.prepare, 50)

        # Verify data is visible and correct.
        self.check(value_d, uri, nrows, 20)
        self.check(value_c, uri, nrows, 30)
        self.check(value_b, uri, nrows, 40)
        self.check(value_a, uri, nrows, 50)

        # Pin stable to timestamp 50 if prepare otherwise 40.
        if self.prepare:
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        else:
            self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(40))

        # Perform additional updates.
        self.large_updates(uri, value_b, ds, nrows, self.prepare, 60)
        self.large_updates(uri, value_c, ds, nrows, self.prepare, 70)
        self.large_updates(uri, value_d, ds, nrows, self.prepare, 80)

        # Checkpoint to ensure the data is flushed to disk.
        self.session.checkpoint()

        # Verify additional update data is visible and correct.
        self.check(value_b, uri, nrows, 60)
        self.check(value_c, uri, nrows, 70)
        self.check(value_d, uri, nrows, 80)

        sizeBeforeRemove = self.getSize(uri)
        self.pr('Before Remove ' + str(sizeBeforeRemove // mb) + 'MB')

        # Perform several removes.
        self.large_removes(uri, ds, nrows//2, nrows, self.prepare, 90)

        # Checkpoint to ensure the data is flushed to disk.
        self.session.checkpoint()

        sizeAfterRemove = self.getSize(uri)
        self.pr('After Remove ' + str(sizeAfterRemove // mb) + 'MB')

        if self.before_crash:
            self.call_compact(uri)

        # Simulate a server crash and restart.
        simulate_crash_restart(self, ".", "RESTART")
        
        if not self.before_crash:
            # Check that the correct data is seen at and after the stable timestamp and before compact.
            self.check(value_b, uri, nrows, 40)
            self.check(value_b, uri, nrows, 80)
            self.check(value_c, uri, nrows, 30)
            self.check(value_d, uri, nrows, 20)
            self.call_compact(uri)

        # Check that the correct data is seen at and after the stable timestamp.
        self.check(value_b, uri, nrows, 40)
        self.check(value_b, uri, nrows, 80)
        self.check(value_c, uri, nrows, 30)
        self.check(value_d, uri, nrows, 20)

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        calls = stat_cursor[stat.conn.txn_rts][2]
        hs_removed = stat_cursor[stat.conn.txn_rts_hs_removed][2]
        keys_removed = stat_cursor[stat.conn.txn_rts_keys_removed][2]
        keys_restored = stat_cursor[stat.conn.txn_rts_keys_restored][2]
        pages_visited = stat_cursor[stat.conn.txn_rts_pages_visited][2]
        upd_aborted = stat_cursor[stat.conn.txn_rts_upd_aborted][2]
        stat_cursor.close()

        self.assertEqual(calls, 0)
        self.assertEqual(keys_removed, 0)
        self.assertEqual(keys_restored, 0)
        self.assertGreaterEqual(upd_aborted, 0)
        self.assertGreater(pages_visited, 0)
        self.assertGreaterEqual(hs_removed, nrows * 4)

if __name__ == '__main__':
    wttest.run()
