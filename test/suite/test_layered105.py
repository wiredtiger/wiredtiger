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

# test_layered105.py
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
#   A forked child runs each scenario so that a WT_PANIC / SIGABRT is caught as
#   a non-zero exit code without killing the test runner.

import errno, os, resource, shutil, threading, time, traceback, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios


@disagg_test_class
class test_layered105(wttest.WiredTigerTestCase):
    tablename = 'test_layered105'
    uri = 'layered:' + tablename
    num_rows = 100

    disagg_storages = gen_disagg_storages('test_layered105', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Used only by the framework's self.conn; the test manages its own DB.
    conn_config = 'disaggregated=(role="leader",drain_threads=1)'

    def _setup_leader(self, home):
        """Create a fresh leader DB, write rows, and close to flush a checkpoint."""
        shutil.rmtree(home, ignore_errors=True)
        os.makedirs(os.path.join(home, 'kv_home'))

        cfg = ('create,statistics=(all)'
               + self.extensionsConfig()
               + ',disaggregated=(role=leader,drain_threads=1)')
        conn = wiredtiger.wiredtiger_open(home, cfg)
        session = conn.open_session('')
        session.create(self.uri,
                       'key_format=i,value_format=S,block_manager=disagg,type=layered')
        cursor = session.open_cursor(self.uri)
        for i in range(self.num_rows):
            cursor[i] = 'value'
        cursor.close()
        conn.close()

    def _run_scenario(self, label, stress_flag, expect_ebusy):
        """
        Fork a child to run the race scenario.  WT_PANIC (SIGABRT) appears as
        a negative exit code and is reported as a failure without killing the
        test runner.
        """
        home = os.path.join(self.home, 'scenario_db')
        ext_config = self.extensionsConfig()
        page_log_name = self.page_log()

        def child():
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

            conn = wiredtiger.wiredtiger_open(
                home,
                'statistics=(all)'
                + ext_config
                + ',disaggregated=(role=follower,drain_threads=2)'
                + f',timing_stress_for_test=[{stress_flag}]')
            session = conn.open_session('')

            # Sync the leader's last checkpoint so database_size is correct before step-up.
            pl_session = conn.open_session('')
            page_log = conn.get_page_log(page_log_name)
            (_, _, _, meta) = page_log.pl_get_complete_checkpoint_ext(pl_session)
            page_log.terminate(pl_session)
            pl_session.close()
            if meta:
                conn.reconfigure(f'disaggregated=(checkpoint_meta="{meta}")')

            # Touch the table to lazy-open the ingest dhandle before step-up.
            session.open_cursor(self.uri).close()

            t = threading.Thread(
                target=lambda: conn.reconfigure('disaggregated=(role=leader)'),
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

            if expect_ebusy:
                assert ebusy, 'drain wins: expected EBUSY but drop succeeded'
                session.drop(self.uri, 'force=true')
            else:
                assert not ebusy, 'drop wins: expected 0 but got EBUSY'

            conn.close()

        self._setup_leader(home)

        pid = os.fork()
        if pid == 0:
            try:
                child()
                os._exit(0)
            except Exception:
                traceback.print_exc()
                os._exit(1)

        _, status = os.waitpid(pid, 0)
        if os.WIFSIGNALED(status):
            self.fail(f'{label}: child killed by signal {os.WTERMSIG(status)} (likely WT_PANIC)')
        self.assertEqual(os.WEXITSTATUS(status), 0, f'{label}: child exited with failure')

    def setUp(self):
        super().setUp()
        self.ignoreStdoutPattern('WT_VERB_RTS|WT_VERB_DEFAULT|WT_VERB_ERROR_RETURNS')

    def test_drain_wins(self):
        # Drain holds the read lock; drop blocks and returns EBUSY, then retries.
        self._run_scenario('drain wins', 'drain_ingest_table_slow', True)

    def test_drop_wins(self):
        # Drop wins the exclusive lock before drain; drain sees DHANDLE_DEAD and skips.
        self._run_scenario('drop wins', 'drain_ingest_table_pre_lock_slow', False)
