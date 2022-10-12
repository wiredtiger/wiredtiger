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

from test_verbose01 import test_verbose_base
from wiredtiger import stat
from wtdataset import SimpleDataSet
import wttest

# test_verbose04.py
# Verify extended debug verbose levels (WT_VERBOSE_DEBUG_2 through 5).
class test_verbose04(test_verbose_base):
    def updates(self, uri, value, ds, nrows, commit_ts):
        session = self.session
        cursor = session.open_cursor(uri)
        for i in range(1, nrows + 1):
            session.begin_transaction()
            cursor[ds.key(i)] = value
            session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def test_verbose_level_2(self):
        self.close_conn()

        self.cleanStdout()
        verbose_config = self.create_verbose_configuration(['rts:2'])
        conn = self.wiredtiger_open(self.home, verbose_config)
        session = conn.open_session()

        self.conn = conn
        self.session = session

        uri = "table:test_verbose04"
        create_params = 'key_format=i,value_format=S'
        session.create(uri, create_params)

        ds = SimpleDataSet(self, uri, 0, key_format='i', value_format="S")
        ds.populate()

        nrows = 1000
        value = 'x' * 1000

        # Insert values with varying timestamps.
        self.updates(uri, value, ds, nrows, 20)

        # Move the oldest and stable timestamps to 40.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(40) +
                                ', stable_timestamp=' + self.timestamp_str(40))

        # Update values.
        self.updates(uri, value, ds, nrows, 60)

        # Perform a checkpoint and close the connection.
        self.session.checkpoint('use_timestamp=true')
        conn.close()

        output = self.readStdout(self.nlines)
        self.assertTrue('DEBUG_2' in output)
        self.cleanStdout()

    def conn_config(self):
        config = 'cache_size=50MB,statistics=(all)'
        return config

    def large_updates(self, uri, value, ds, nrows, prepare, commit_ts):
        # Update a large number of records.
        session = self.session
        try:
            cursor = session.open_cursor(uri)
            for i in range(1, nrows + 1):
                if commit_ts == 0:
                    session.begin_transaction('no_timestamp=true')
                else:
                    session.begin_transaction()
                cursor[ds.key(i)] = value
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

    def check(self, check_value, uri, nrows, flcs_extrarows, read_ts):
        # In FLCS, deleted values read back as 0, and (at least for now) uncommitted appends
        # cause zeros to appear under them. If flcs_extrarows isn't None, expect that many
        # rows of zeros following the regular data.
        flcs_tolerance = False

        session = self.session
        if read_ts == 0:
            session.begin_transaction()
        else:
            session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        cursor = session.open_cursor(uri)
        count = 0
        for k, v in cursor:
            if flcs_tolerance and count >= nrows:
                self.assertEqual(v, 0)
            else:
                self.assertEqual(v, check_value)
            count += 1
        session.commit_transaction()
        self.assertEqual(count, nrows + flcs_extrarows if flcs_tolerance else nrows)
        cursor.close()

    # test_rollback_to_stable06 and 14
    def test_verbose_level_3(self):
        nrows = 1000

        # Create a table.
        uri = "table:rollback_to_stable06"
        ds_config = ''
        ds = SimpleDataSet(self, uri, 0,
            key_format='i', value_format='S', config=ds_config)
        ds.populate()

        # value_a = "aaaaa" * 100
        value_b = "bbbbb" * 100
        value_c = "ccccc" * 100
        value_d = "ddddd" * 100

        # Pin oldest and stable to timestamp 10.
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10) +
            ',stable_timestamp=' + self.timestamp_str(10))

        # Perform several updates.
        self.large_updates(uri, value_a, ds, nrows, True, 20)
        self.large_updates(uri, value_b, ds, nrows, True, 30)
        self.large_updates(uri, value_c, ds, nrows, True, 40)
        self.large_updates(uri, value_d, ds, nrows, True, 50)

        # Verify data is visible and correct.
        self.check(value_a, uri, nrows, None, 21 if True else 20)
        self.check(value_b, uri, nrows, None, 31 if True else 30)
        self.check(value_c, uri, nrows, None, 41 if True else 40)
        self.check(value_d, uri, nrows, None, 51 if True else 50)

        # Checkpoint to ensure the data is flushed, then rollback to the stable timestamp.
        self.session.checkpoint()
        self.conn.rollback_to_stable()

        # Check that all keys are removed.
        # (For FLCS, at least for now, they will read back as 0, meaning deleted, rather
        # than disappear.)
        # self.check(value_a, uri, 0, nrows, 20)
        # self.check(value_b, uri, 0, nrows, 30)
        # self.check(value_c, uri, 0, nrows, 40)
        # self.check(value_d, uri, 0, nrows, 50)

        # stat_cursor = self.session.open_cursor('statistics:', None, None)
        # calls = stat_cursor[stat.conn.txn_rts][2]
        # hs_removed = stat_cursor[stat.conn.txn_rts_hs_removed][2]
        # keys_removed = stat_cursor[stat.conn.txn_rts_keys_removed][2]
        # keys_restored = stat_cursor[stat.conn.txn_rts_keys_restored][2]
        # pages_visited = stat_cursor[stat.conn.txn_rts_pages_visited][2]
        # upd_aborted = stat_cursor[stat.conn.txn_rts_upd_aborted][2]
        # stat_cursor.close()

        # self.assertEqual(calls, 1)
        # self.assertEqual(keys_restored, 0)
        # self.assertGreater(pages_visited, 0)
        # self.assertGreaterEqual(keys_removed, 0)
        # self.assertGreaterEqual(upd_aborted + hs_removed + keys_removed, nrows * 4)

    def test_verbose_level_4_and_5(self):
        self.close_conn()

        self.cleanStdout()
        verbose_config = self.create_verbose_configuration(['recovery:5'])
        conn = self.wiredtiger_open(self.home, verbose_config)
        session = conn.open_session()

        ckpt_uri = 'table:ckpt_table'
        session.create(ckpt_uri, 'key_format=i,value_format=i,log=(enabled=false)')
        c_ckpt = session.open_cursor(ckpt_uri)

        # Add some data.
        session.begin_transaction()
        c_ckpt[1] = 1
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        # Set the stable timestamp before the data.
        conn.set_timestamp('stable_timestamp=' + self.timestamp_str(9))

        # Run recovery.
        conn.close()
        conn = self.wiredtiger_open(self.home, verbose_config)

        output = self.readStdout(self.nlines)
        self.assertTrue('DEBUG_4' in output)
        self.assertTrue('DEBUG_5' in output)
        conn.close()
        self.cleanStdout()

if __name__ == '__main__':
    wttest.run()
