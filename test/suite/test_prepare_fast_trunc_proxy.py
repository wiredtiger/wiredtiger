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

import wttest
from wiredtiger import stat
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from helper import simulate_crash_restart

# test_prepare_fast_trunc_proxy.py
#
# Verify that internal-page reconciliation writes the correct proxy cell encoding
# (prepared vs. committed vs. absent) for fast-truncated pages under precise
# checkpoint.  Six cases are covered:
#
#   Case 1 (prepare case):  page_del still in-flight prepared,
#                           prepare_ts <= stable_ts - write prepared proxy cell
#   Case 2 (commit case):   page_del committed after prepare,
#                           durable_ts > stable_ts but prepare_ts <= stable_ts
#                           - write prepared proxy cell; after RTS data comes back
#   Case 3 (rollback case, proxy):  page_del prepared then aborted,
#                           rollback_ts > stable_ts but prepare_ts <= stable_ts
#                           - write prepared proxy cell; pages accessible after recovery
#   Case 4 (rollback case, no proxy): page_del prepared then aborted,
#                           rollback_ts <= stable_ts
#                           - do NOT write proxy cell; pages accessible after reopen
#   Case 5 (proxy revert):  checkpoint written mid-prepare, then rollback becomes stable
#                           - second checkpoint must revert prepared proxy cell
#   Case 6 (commit stable): page_del committed with durable_ts <= stable_ts
#                           - write committed proxy cell; deletion visible after reopen
class test_prepare_fast_trunc_proxy(wttest.WiredTigerTestCase):

    # precise_checkpoint=true is required for the new selection logic to activate.
    conn_config = 'statistics=(all),precise_checkpoint=true'
    session_config = 'isolation=snapshot'

    format_values = [
        ('column', dict(key_format='r', extraconfig='')),
        ('integer_row', dict(key_format='i', extraconfig='')),
    ]
    scenarios = make_scenarios(format_values)

    nrows = 10000

    def _setup_table(self, uri):
        """Create, populate with baseline data at ts=10, then reopen so fast-truncate is possible."""
        ds = SimpleDataSet(
            self, uri, 0, key_format=self.key_format, value_format='S',
            config=self.extraconfig)
        ds.populate()

        value_a = "aaaaa" * 100
        self.conn.set_timestamp(
            'oldest_timestamp=' + self.timestamp_str(1) +
            ',stable_timestamp=' + self.timestamp_str(1))

        cursor = self.session.open_cursor(ds.uri)
        self.session.begin_transaction()
        for i in range(1, self.nrows + 1):
            cursor[ds.key(i)] = value_a
            # Commit in chunks to force multiple leaf pages for fast-truncate.
            if i % 487 == 0:
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
                self.session.begin_transaction()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))

        # Reopen so pages are on disk and fast-truncate is possible.
        self.reopen_conn()
        return ds

    def _fast_truncate(self, session, uri, ds, lo, hi):
        lo_cursor = session.open_cursor(uri)
        hi_cursor = session.open_cursor(uri)
        lo_cursor.set_key(ds.key(lo))
        hi_cursor.set_key(ds.key(hi))
        ret = session.truncate(None, lo_cursor, hi_cursor, None)
        lo_cursor.close()
        hi_cursor.close()
        self.assertEqual(ret, 0)

    def _fast_delete_count(self):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        v = stat_cursor[stat.conn.rec_page_delete_fast][2]
        stat_cursor.close()
        return v

    def _count_visible_rows(self, uri, ds, lo, hi, read_ts):
        """Count rows visible in [lo, hi] at read_ts."""
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        found = 0
        try:
            for i in range(lo, hi + 1):
                cursor.set_key(ds.key(i))
                if cursor.search() == 0:
                    found += 1
        finally:
            self.session.commit_transaction()
            cursor.close()
        return found

    # -------------------------------------------------------------------------
    # Case 1: In-flight prepared fast-truncate with prepare_ts (20) <= stable_ts (25)
    #
    # Checkpoint with precise_checkpoint=true should succeed and write prepared
    # proxy cells (not skip them). After crash-restart the table can be opened and
    # rows outside the truncated range remain visible at ts=10.
    # -------------------------------------------------------------------------
    def test_case1_inflight_prepare(self):
        uri = 'table:prepare_ft_proxy_case1'
        ds = self._setup_table(uri)

        session2 = self.conn.open_session(self.session_config)

        # Count fast-deletes before truncation.
        before_trunc = self._fast_delete_count()

        session2.begin_transaction()
        lo = self.nrows // 4 + 1
        hi = 3 * self.nrows // 4
        self._fast_truncate(session2, uri, ds, lo, hi)
        # prepare_ts=20 <= stable_ts=25 set below.
        session2.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20))

        # Verify fast-deletes happened.
        if not self.runningHook('tiered'):
            self.assertGreater(self._fast_delete_count(), before_trunc)

        # Advance stable past prepare_ts.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25))

        # Checkpoint must succeed even with an in-flight prepared fast-truncate
        # whose prepare_ts is stable.
        self.session.checkpoint()

        # Crash-restart (recovery).
        simulate_crash_restart(self, ".", "RESTART")

        # Rows OUTSIDE the truncated range should be visible at ts=10.
        found_before = self._count_visible_rows(uri, ds, 1, lo - 1, 10)
        found_after = self._count_visible_rows(uri, ds, hi + 1, self.nrows, 10)
        self.assertEqual(found_before, lo - 1)
        self.assertEqual(found_after, self.nrows - hi)
        # The prepared truncation was never committed, so rows INSIDE the range
        # must also be visible after recovery rolls it back.
        found_inside = self._count_visible_rows(uri, ds, lo, hi, 10)
        self.assertEqual(found_inside, hi - lo + 1)

    # -------------------------------------------------------------------------
    # Case 2: Fast-truncate prepared (ts=20) then committed with durable_ts=35 > stable_ts=25
    #
    # The checkpoint at stable_ts=25 must write a PREPARED proxy cell (not a
    # committed one) because the durable commit is beyond stable. After
    # crash+restart the stable is still 25 and the data in the truncated range
    # must still be visible at ts=10 (the deletion is not stable yet).
    # -------------------------------------------------------------------------
    def test_case2_commit_durable_beyond_stable(self):
        uri = 'table:prepare_ft_proxy_case2'
        ds = self._setup_table(uri)

        session2 = self.conn.open_session(self.session_config)

        before_trunc = self._fast_delete_count()

        session2.begin_transaction()
        lo = self.nrows // 4 + 1
        hi = 3 * self.nrows // 4
        self._fast_truncate(session2, uri, ds, lo, hi)
        session2.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20))

        if not self.runningHook('tiered'):
            self.assertGreater(self._fast_delete_count(), before_trunc)

        # Commit at 30, durable at 35. stable_ts is still 10 at commit time;
        # it is advanced to 25 afterwards to set the checkpoint boundary.
        session2.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(30) +
            ',durable_timestamp=' + self.timestamp_str(35))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25))
        self.session.checkpoint()

        # Crash+restart.
        simulate_crash_restart(self, ".", "RESTART")

        # After recovery the stable_ts is 25 and the durable commit (35) is
        # beyond stable. The truncated rows should still be visible at ts=10.
        found = self._count_visible_rows(uri, ds, lo, hi, 10)
        self.assertEqual(found, hi - lo + 1)
        # Rows outside the truncated range must also be accessible.
        found_before = self._count_visible_rows(uri, ds, 1, lo - 1, 10)
        found_after = self._count_visible_rows(uri, ds, hi + 1, self.nrows, 10)
        self.assertEqual(found_before, lo - 1)
        self.assertEqual(found_after, self.nrows - hi)

    # -------------------------------------------------------------------------
    # Case 3: Fast-truncate prepared (ts=20) then rolled back (rollback_ts=30 > stable_ts=25)
    #
    # The checkpoint must write a prepared proxy cell (prepare_ts=20 is stable but
    # rollback_ts=30 is not). After crash+restart the rollback is replayed from the
    # log and the truncated pages are accessible again at ts=10.
    # -------------------------------------------------------------------------
    def test_case3_rollback_beyond_stable(self):
        uri = 'table:prepare_ft_proxy_case3'
        ds = self._setup_table(uri)

        session2 = self.conn.open_session(self.session_config)

        before_trunc = self._fast_delete_count()

        session2.begin_transaction()
        lo = self.nrows // 4 + 1
        hi = 3 * self.nrows // 4
        self._fast_truncate(session2, uri, ds, lo, hi)
        session2.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25))

        if not self.runningHook('tiered'):
            self.assertGreater(self._fast_delete_count(), before_trunc)

        # Roll back with rollback_ts=30 > stable_ts=25.
        session2.rollback_transaction(
            'rollback_timestamp=' + self.timestamp_str(30))

        # Checkpoint: prepare_ts (20) is stable but rollback_ts (30) is not.
        # A prepared proxy cell should be written.
        self.session.checkpoint()

        # Crash+restart. Recovery reads the checkpoint's prepared proxy cell
        # and rolls back the orphaned prepared transaction.
        simulate_crash_restart(self, ".", "RESTART")

        # After recovery the truncated pages should be accessible.
        found = self._count_visible_rows(uri, ds, lo, hi, 10)
        self.assertEqual(found, hi - lo + 1)

    # -------------------------------------------------------------------------
    # Case 4: Fast-truncate prepared (ts=20) then rolled back (rollback_ts=30),
    #         then stable advanced to 35 so rollback_ts (30) <= stable_ts (35).
    #
    # Rollback must use rollback_ts > stable_ts at rollback time, so we first
    # rollback at ts=30 when stable=25, then advance stable to 35. The checkpoint
    # at stable_ts=35 sees rollback_ts (30) <= stable_ts (35) and must NOT write
    # a proxy cell.  After reopen the pages are accessible.
    # -------------------------------------------------------------------------
    def test_case4_rollback_before_stable(self):
        uri = 'table:prepare_ft_proxy_case4'
        ds = self._setup_table(uri)

        session2 = self.conn.open_session(self.session_config)

        before_trunc = self._fast_delete_count()

        session2.begin_transaction()
        lo = self.nrows // 4 + 1
        hi = 3 * self.nrows // 4
        self._fast_truncate(session2, uri, ds, lo, hi)
        # prepare_ts=20 <= stable_ts=25 (set below before rollback).
        session2.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20))

        # Set stable to 25; prepare_ts (20) is now stable.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25))

        if not self.runningHook('tiered'):
            self.assertGreater(self._fast_delete_count(), before_trunc)

        # Roll back with rollback_ts=30 > stable_ts=25 (required by WiredTiger).
        session2.rollback_transaction(
            'rollback_timestamp=' + self.timestamp_str(30))

        # Now advance stable past rollback_ts so rollback_ts (30) <= stable_ts (35).
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(35))

        # Checkpoint at stable_ts=35: rollback_ts (30) <= stable_ts (35),
        # so no prepared proxy cell should be written.
        self.session.checkpoint()

        # After reopen the truncated pages must be accessible (deletion was aborted
        # and that abort is stable).
        self.reopen_conn()

        found = self._count_visible_rows(uri, ds, lo, hi, 10)
        self.assertEqual(found, hi - lo + 1)

    # -------------------------------------------------------------------------
    # Case 5: Like Case 4 but a checkpoint is taken while the prepare is in
    #         flight (before rollback), so the first checkpoint writes a
    #         prepared proxy DEL cell.  After rollback and stable advancing past
    #         rollback_ts a second checkpoint must revert that DEL cell to a
    #         plain addr cell.
    #
    # Sequence:
    #   prepare at ts=20, stable=25    Checkpoint 1 writes prepared proxy cell
    #   rollback at ts=30              txnid=ABORTED, page_del preserved
    #   stable advance to 35           rollback_ts (30) now stable
    #   Checkpoint 2                   must revert DEL cell to plain addr cell
    #   reopen                         truncated rows accessible at ts=10
    # -------------------------------------------------------------------------
    def test_case5_proxy_written_then_rollback_stable(self):
        uri = 'table:prepare_ft_proxy_case5'
        ds = self._setup_table(uri)

        session2 = self.conn.open_session(self.session_config)

        before_trunc = self._fast_delete_count()

        session2.begin_transaction()
        lo = self.nrows // 4 + 1
        hi = 3 * self.nrows // 4
        self._fast_truncate(session2, uri, ds, lo, hi)
        # prepare_ts=20 will be stable when we set stable=25 below.
        session2.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20))

        if not self.runningHook('tiered'):
            self.assertGreater(self._fast_delete_count(), before_trunc)

        # Advance stable past prepare_ts so the first checkpoint writes a
        # prepared proxy DEL cell (prepare_ts=20 <= stable_ts=25).
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25))
        self.session.checkpoint()

        # Roll back with rollback_ts=30 > stable_ts=25 (required by WiredTiger).
        session2.rollback_transaction(
            'rollback_timestamp=' + self.timestamp_str(30))

        # Advance stable past rollback_ts: the rollback is now stable.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(35))

        # Checkpoint 2: rollback_ts (30) <= stable_ts (35).
        # Must revert the on-page prepared proxy DEL cell to a plain addr cell
        # rather than copying it verbatim or firing an assertion.
        self.session.checkpoint()

        # After reopen the truncated pages must be accessible.
        self.reopen_conn()

        found = self._count_visible_rows(uri, ds, lo, hi, 10)
        self.assertEqual(found, hi - lo + 1)

    # -------------------------------------------------------------------------
    # Case 6: Fast-truncate prepared (ts=20) then committed with durable_ts=30,
    #         stable advanced to 35 so durable_ts (30) <= stable_ts (35).
    #
    # If durable_ts <= stable_ts: write as committed page del.
    # The checkpoint at stable_ts=35 must write a committed (not prepared)
    # proxy cell.  After reopen the truncated rows must NOT be visible
    # (the deletion is fully stable).
    # -------------------------------------------------------------------------
    def test_case6_commit_durable_within_stable(self):
        uri = 'table:prepare_ft_proxy_case6'
        ds = self._setup_table(uri)

        session2 = self.conn.open_session(self.session_config)

        before_trunc = self._fast_delete_count()

        session2.begin_transaction()
        lo = self.nrows // 4 + 1
        hi = 3 * self.nrows // 4
        self._fast_truncate(session2, uri, ds, lo, hi)
        session2.prepare_transaction('prepare_timestamp=' + self.timestamp_str(20))

        if not self.runningHook('tiered'):
            self.assertGreater(self._fast_delete_count(), before_trunc)

        # Commit at 25, durable at 30. stable_ts is still 10 at commit time.
        session2.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(25) +
            ',durable_timestamp=' + self.timestamp_str(30))

        # Advance stable past durable_ts: the deletion is now fully stable.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(35))
        self.session.checkpoint()

        # After reopen, verify the committed deletion is stable.
        self.reopen_conn()

        # At commit_ts (25) the deletion is visible: rows must be gone.
        found_deleted = self._count_visible_rows(uri, ds, lo, hi, 25)
        self.assertEqual(found_deleted, 0)
        # At ts=10 (before commit_ts) the rows were written and must still be visible.
        found_historical = self._count_visible_rows(uri, ds, lo, hi, 10)
        self.assertEqual(found_historical, hi - lo + 1)
        # Rows outside the truncated range remain accessible at ts=10.
        found_before = self._count_visible_rows(uri, ds, 1, lo - 1, 10)
        found_after = self._count_visible_rows(uri, ds, hi + 1, self.nrows, 10)
        self.assertEqual(found_before, lo - 1)
        self.assertEqual(found_after, self.nrows - hi)

if __name__ == '__main__':
    wttest.run()
