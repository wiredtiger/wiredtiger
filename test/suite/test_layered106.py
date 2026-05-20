#!/usr/bin/env python3
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

# test_layered106.py
#   Regression test for a drain worker racing with a concurrent
#   session.drop(force=True) during follower->leader step-up.  The drain
#   worker must not panic in either race ordering.
#
#   Two timing stress flags exercise the two orderings:
#
#     drain_ingest_table_slow: 300 ms sleep inside __layered_copy_ingest_table
#       while the ingest dhandle read lock is held.  The drop blocks on the
#       exclusive write lock and returns EBUSY; a retry after step-up succeeds.
#
#     drain_ingest_table_pre_lock_slow: 300 ms sleep in __layered_drain_worker_run
#       before acquiring the read lock.  The drop wins the exclusive lock first,
#       sets WT_DHANDLE_DEAD, and returns 0.  Drain then sees DEAD and skips.
#
#   Each scenario runs in a subprocess via run_subprocess_function so that a
#   WT_PANIC / crash is caught as a non-zero exit code without killing the test
#   runner.  This approach works cross-platform (no os.fork required).

import errno, os, threading, time, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

@disagg_test_class
class test_layered106(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_layered106'
    uri = 'layered:' + tablename
    num_rows = 100

    disagg_storages = gen_disagg_storages('test_layered106', disagg_only=True)
    race_scenarios = [
        ('drain_wins', dict(stress_flag='drain_ingest_table_slow', expect_ebusy=True)),
        ('drop_wins', dict(stress_flag='drain_ingest_table_pre_lock_slow', expect_ebusy=False)),
    ]
    scenarios = make_scenarios(disagg_storages, race_scenarios)

    conn_config = 'disaggregated=(role="leader",drain_threads=1)'

    def _race_scenario(self):
        # Set up leader data, reopen as follower, and run the drop/drain race.
        # Called inside a subprocess so WT_PANIC exits non-zero.

        # Create and populate the table via the framework's leader connection.
        self.session.create(self.uri,
                            'key_format=i,value_format=S,block_manager=disagg,type=layered')
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.num_rows):
            cursor[i] = 'value'
        cursor.close()

        # Close the leader connection, flushing a checkpoint.
        self.close_conn()

        # Reopen as a follower with the timing stress flag.
        conn = wiredtiger.wiredtiger_open(
            self.home,
            'statistics=(all)'
            + self.extensionsConfig()
            + ',disaggregated=(role=follower,drain_threads=2)'
            + f',timing_stress_for_test=[{self.stress_flag}]')
        session = conn.open_session('')

        # We closed a leader that wrote rows, so a complete checkpoint must exist.
        meta = self.disagg_get_complete_checkpoint_meta(conn)
        self.assertIsNotNone(meta, 'expected a complete checkpoint from the leader')

        # Touch the table to lazy-open the ingest dhandle before step-up.
        session.open_cursor(self.uri).close()

        # Step up to leader in a thread; the race happens here.
        t = threading.Thread(
            target=lambda: conn.reconfigure(
                f'disaggregated=(role=leader,checkpoint_meta="{meta}")'),
            daemon=True)
        t.start()

        # Sleep 100 ms to land inside the 300 ms stress window for both scenarios.
        # drain_ingest_table_slow: lock held from ~50 ms; 100 ms is safely inside.
        # drain_ingest_table_pre_lock_slow: pre-lock sleep from ~50 ms; 100 ms drops
        #   while drain is still sleeping before the lock, so drop wins the exclusive lock.
        # Without the sleep, the drop runs before drain has even started, so the race
        # is never triggered.
        time.sleep(0.1)

        ebusy = False
        try:
            session.drop(self.uri, 'force=true,checkpoint_wait=false')
        except wiredtiger.WiredTigerError as e:
            if os.strerror(errno.EBUSY) in str(e):
                ebusy = True
            else:
                raise

        t.join()

        if self.expect_ebusy:
            self.assertTrue(ebusy, 'drain wins: expected EBUSY but drop succeeded')
            session.drop(self.uri, 'force=true,checkpoint_wait=false')
        else:
            self.assertFalse(ebusy, 'drop wins: expected success but got EBUSY')

        # Check that the table was successfully dropped.
        with self.assertRaises(wiredtiger.WiredTigerError):
            session.open_cursor(self.uri)

        conn.close()

    def subprocess_race(self):
        self._race_scenario()

    def test_race(self):
        rc, _ = self.run_subprocess_function(
            'SUBPROCESS',
            'test_layered106.test_layered106.subprocess_race')
        self.assertEqual(rc, 0, f'{self.stress_flag}: subprocess exited non-zero (likely abort)')
