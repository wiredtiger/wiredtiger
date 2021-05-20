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

import fnmatch, os, shutil, threading, time
from helper import copy_wiredtiger_home, simulate_crash_restart
from test_rollback_to_stable01 import test_rollback_to_stable_base
from wiredtiger import stat, wiredtiger_strerror, WiredTigerError, WT_ROLLBACK
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from wtthread import checkpoint_thread, op_thread
from time import sleep

def timestamp_str(t):
    return '%x' % t

def retry_rollback(self, name, txn_session, code):
    retry_limit = 100
    retries = 0
    completed = False
    saved_exception = None
    while not completed and retries < retry_limit:
        if retries != 0:
            self.pr("Retrying operation for " + name)
            if txn_session:
                txn_session.rollback_transaction()
            sleep(0.1)
            if txn_session:
                txn_session.begin_transaction('isolation=snapshot')
                self.pr("Began new transaction for " + name)
        try:
            code()
            completed = True
        except WiredTigerError as e:
            rollback_str = wiredtiger_strerror(WT_ROLLBACK)
            if rollback_str not in str(e):
                raise(e)
            retries += 1
            saved_exception = e
    if not completed and saved_exception:
        raise(saved_exception)

# test_rollback_to_stable10.py
# Test the rollback to stable operation performs sweeping history store.
class test_rollback_to_stable10(test_rollback_to_stable_base):
    session_config = 'isolation=snapshot'

    key_format_values = [
        ('column', dict(key_format='r')),
        ('integer_row', dict(key_format='i')),
    ]

    prepare_values = [
        ('no_prepare', dict(prepare=False)),
        ('prepare', dict(prepare=True))
    ]

    scenarios = make_scenarios(key_format_values, prepare_values)

    def conn_config(self):
        config = 'cache_size=6MB,statistics=(all),statistics_log=(json,on_close,wait=1),log=(enabled=true),timing_stress_for_test=[history_store_checkpoint_delay]'
        return config

    def test_rollback_to_stable(self):
        nrows = 1000

        # Prepare transactions for column store table is not yet supported.
        if self.prepare and self.key_format == 'r':
            self.skipTest('Prepare transactions for column store table is not yet supported')

        # Create a table without logging.
        self.pr("create/populate tables")
        uri_1 = "table:rollback_to_stable10_1"
        ds_1 = SimpleDataSet(
            self, uri_1, 0, key_format=self.key_format, value_format="S", config='log=(enabled=false)')
        ds_1.populate()

        # Create another table without logging.
        uri_2 = "table:rollback_to_stable10_2"
        ds_2 = SimpleDataSet(
            self, uri_2, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds_2.populate()

        # Pin oldest and stable to timestamp 10.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(10) +
            ',stable_timestamp=' + timestamp_str(10))

        value_a = "aaaaa" * 100
        value_b = "bbbbb" * 100
        value_c = "ccccc" * 100
        value_d = "ddddd" * 100
        value_e = "eeeee" * 100
        value_f = "fffff" * 100

        # Perform several updates.
        self.pr("large updates")
        self.large_updates(uri_1, value_d, ds_1, nrows, self.prepare, 20)
        self.large_updates(uri_1, value_c, ds_1, nrows, self.prepare, 30)
        self.large_updates(uri_1, value_b, ds_1, nrows, self.prepare, 40)
        self.large_updates(uri_1, value_a, ds_1, nrows, self.prepare, 50)

        self.large_updates(uri_2, value_d, ds_2, nrows, self.prepare, 20)
        self.large_updates(uri_2, value_c, ds_2, nrows, self.prepare, 30)
        self.large_updates(uri_2, value_b, ds_2, nrows, self.prepare, 40)
        self.large_updates(uri_2, value_a, ds_2, nrows, self.prepare, 50)

        # Verify data is visible and correct.
        self.check(value_d, uri_1, nrows, 20)
        self.check(value_c, uri_1, nrows, 30)
        self.check(value_b, uri_1, nrows, 40)
        self.check(value_a, uri_1, nrows, 50)

        self.check(value_d, uri_2, nrows, 20)
        self.check(value_c, uri_2, nrows, 30)
        self.check(value_b, uri_2, nrows, 40)
        self.check(value_a, uri_2, nrows, 50)

        # Pin stable to timestamp 60 if prepare otherwise 50.
        if self.prepare:
            self.conn.set_timestamp('stable_timestamp=' + timestamp_str(60))
        else:
            self.conn.set_timestamp('stable_timestamp=' + timestamp_str(50))

        # Create a checkpoint thread
        done = threading.Event()
        ckpt = checkpoint_thread(self.conn, done)
        try:
            self.pr("start checkpoint")
            ckpt.start()

            # Perform several updates in parallel with checkpoint.
            # Rollbacks may occur when checkpoint is running, so retry as needed.
            self.pr("updates")
            retry_rollback(self, 'update ds1, e', None,
                           lambda: self.large_updates(uri_1, value_e, ds_1, nrows, self.prepare, 70))
            retry_rollback(self, 'update ds2, e', None,
                           lambda: self.large_updates(uri_2, value_e, ds_2, nrows, self.prepare, 70))
            retry_rollback(self, 'update ds1, f', None,
                           lambda: self.large_updates(uri_1, value_f, ds_1, nrows, self.prepare, 80))
            retry_rollback(self, 'update ds2, f', None,
                           lambda: self.large_updates(uri_2, value_f, ds_2, nrows, self.prepare, 80))
        finally:
            done.set()
            ckpt.join()

        # Simulate a server crash and restart.
        self.pr("restart")
        simulate_crash_restart(self, ".", "RESTART")
        self.pr("restart complete")

        # Check that the correct data is seen at and after the stable timestamp.
        self.check(value_a, uri_1, nrows, 50)
        self.check(value_a, uri_1, nrows, 80)
        self.check(value_b, uri_1, nrows, 40)
        self.check(value_c, uri_1, nrows, 30)
        self.check(value_d, uri_1, nrows, 20)

        # Check that the correct data is seen at and after the stable timestamp.
        self.check(value_c, uri_2, nrows, 30)
        self.check(value_a, uri_2, nrows, 50)
        self.check(value_a, uri_2, nrows, 80)
        self.check(value_b, uri_2, nrows, 40)
        self.check(value_d, uri_2, nrows, 20)

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        calls = stat_cursor[stat.conn.txn_rts][2]
        hs_removed = stat_cursor[stat.conn.txn_rts_hs_removed][2]
        hs_sweep = stat_cursor[stat.conn.txn_rts_sweep_hs_keys][2]
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
        self.assertGreaterEqual(hs_removed, 0)
        self.assertGreater(hs_sweep, 0)

    def test_rollback_to_stable_prepare(self):
        nrows = 1000

        # Prepare transactions for column store table is not yet supported.
        if self.prepare and self.key_format == 'r':
            self.skipTest('Prepare transactions for column store table is not yet supported')

        # Create a table without logging.
        self.pr("create/populate tables")
        uri_1 = "table:rollback_to_stable10_1"
        ds_1 = SimpleDataSet(
            self, uri_1, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds_1.populate()

        # Create another table without logging.
        uri_2 = "table:rollback_to_stable10_2"
        ds_2 = SimpleDataSet(
            self, uri_2, 0, key_format="i", value_format="S", config='log=(enabled=false)')
        ds_2.populate()

        # Pin oldest and stable to timestamp 10.
        self.conn.set_timestamp('oldest_timestamp=' + timestamp_str(10) +
            ',stable_timestamp=' + timestamp_str(10))

        value_a = "aaaaa" * 100
        value_b = "bbbbb" * 100
        value_c = "ccccc" * 100
        value_d = "ddddd" * 100
        value_e = "eeeee" * 100
        value_f = "fffff" * 100

        # Perform several updates.
        self.pr("large updates")
        self.large_updates(uri_1, value_d, ds_1, nrows, self.prepare, 20)
        self.large_updates(uri_1, value_c, ds_1, nrows, self.prepare, 30)
        self.large_updates(uri_1, value_b, ds_1, nrows, self.prepare, 40)
        self.large_updates(uri_1, value_a, ds_1, nrows, self.prepare, 50)

        self.large_updates(uri_2, value_d, ds_2, nrows, self.prepare, 20)
        self.large_updates(uri_2, value_c, ds_2, nrows, self.prepare, 30)
        self.large_updates(uri_2, value_b, ds_2, nrows, self.prepare, 40)
        self.large_updates(uri_2, value_a, ds_2, nrows, self.prepare, 50)

        # Verify data is visible and correct.
        self.check(value_d, uri_1, nrows, 20)
        self.check(value_c, uri_1, nrows, 30)
        self.session.breakpoint()
        self.check(value_b, uri_1, nrows, 40)
        self.check(value_a, uri_1, nrows, 50)

        self.check(value_d, uri_2, nrows, 20)
        self.check(value_c, uri_2, nrows, 30)
        self.session.breakpoint()
        self.check(value_b, uri_2, nrows, 40)
        self.check(value_a, uri_2, nrows, 50)

        # Pin stable to timestamp 60 if prepare otherwise 50.
        if self.prepare:
            self.conn.set_timestamp('stable_timestamp=' + timestamp_str(60))
        else:
            self.conn.set_timestamp('stable_timestamp=' + timestamp_str(50))

        # Here's the update operations we'll perform, encapsulated so we can easily retry
        # it if we get a rollback. Rollbacks may occur when checkpoint is running.
        def prepare_range_updates(session, cursor, ds, value, nrows, prepare_config):
            self.pr("updates")
            for i in range(1, nrows):
                key = ds.key(i)
                cursor.set_key(key)
                cursor.set_value(value)
                self.assertEquals(cursor.update(), 0)
            self.pr("prepare")
            session.prepare_transaction(prepare_config)

        # Create a checkpoint thread
        done = threading.Event()
        ckpt = checkpoint_thread(self.conn, done)
        try:
            self.pr("start checkpoint")
            ckpt.start()

            # Perform several updates in parallel with checkpoint.
            session_p1 = self.conn.open_session()
            cursor_p1 = session_p1.open_cursor(uri_1)
            session_p1.begin_transaction('isolation=snapshot')
            retry_rollback(self, 'update ds1', session_p1,
                           lambda: prepare_range_updates(
                               session_p1, cursor_p1, ds_1, value_e, nrows,
                               'prepare_timestamp=' + timestamp_str(69)))

            # Perform several updates in parallel with checkpoint.
            session_p2 = self.conn.open_session()
            cursor_p2 = session_p2.open_cursor(uri_2)
            session_p2.begin_transaction('isolation=snapshot')
            retry_rollback(self, 'update ds2', session_p2,
                           lambda: prepare_range_updates(
                               session_p2, cursor_p2, ds_2, value_e, nrows,
                               'prepare_timestamp=' + timestamp_str(69)))
        finally:
            done.set()
            ckpt.join()

        # Check that the history store file has been used and has non-zero size before the simulated
        # crash.
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        cache_hs_ondisk = stat_cursor[stat.conn.cache_hs_ondisk][2]
        stat_cursor.close()
        self.assertGreater(cache_hs_ondisk, 0)

        # Simulate a crash by copying to a new directory(RESTART).
        copy_wiredtiger_home(self, ".", "RESTART")

        # Commit the prepared transaction.
        session_p1.commit_transaction('commit_timestamp=' + timestamp_str(70) + ',durable_timestamp=' + timestamp_str(71))
        session_p2.commit_transaction('commit_timestamp=' + timestamp_str(70) + ',durable_timestamp=' + timestamp_str(71))
        session_p1.close()
        session_p2.close()

        # Open the new directory.
        self.pr("restart")
        self.conn = self.setUpConnectionOpen("RESTART")
        self.session = self.setUpSessionOpen(self.conn)
        self.pr("restart complete")

        # The history store file size should be greater than zero after the restart.
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        cache_hs_ondisk = stat_cursor[stat.conn.cache_hs_ondisk][2]
        stat_cursor.close()
        self.assertGreater(cache_hs_ondisk, 0)

        # Check that the correct data is seen at and after the stable timestamp.
        self.check(value_a, uri_1, nrows, 50)
        self.check(value_a, uri_1, nrows, 80)
        self.check(value_b, uri_1, nrows, 40)
        self.check(value_c, uri_1, nrows, 30)
        self.check(value_d, uri_1, nrows, 20)

        # Check that the correct data is seen at and after the stable timestamp.
        self.check(value_a, uri_2, nrows, 50)
        self.check(value_a, uri_2, nrows, 80)
        self.check(value_b, uri_2, nrows, 40)
        self.check(value_c, uri_2, nrows, 30)
        self.check(value_d, uri_2, nrows, 20)

        stat_cursor = self.session.open_cursor('statistics:', None, None)
        calls = stat_cursor[stat.conn.txn_rts][2]
        hs_removed = stat_cursor[stat.conn.txn_rts_hs_removed][2]
        hs_sweep = stat_cursor[stat.conn.txn_rts_sweep_hs_keys][2]
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
        self.assertGreaterEqual(hs_removed, 0)
        self.assertGreater(hs_sweep, 0)

        # The test may output the following message in eviction under cache pressure. Ignore that.
        self.ignoreStdoutPatternIfExists("oldest pinned transaction ID rolled back for eviction")

if __name__ == '__main__':
    wttest.run()
