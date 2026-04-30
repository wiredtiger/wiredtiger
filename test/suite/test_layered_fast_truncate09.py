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

import wttest, wiredtiger
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered_fast_truncate_stepup.py
#   Coverage for the step-up drain that runs in __layered_copy_ingest_table:
#   for keys in each pending follower truncate range, the drain ensures the
#   stable table reflects the truncate's effect after the connection becomes
#   leader. Tests cover boundary ranges, overlapping truncates, post-truncate
#   reinserts, snapshot-time visibility, and full-table truncates.
@disagg_test_class
class test_layered_fast_truncate_stepup(wttest.WiredTigerTestCase):

    conn_config = 'disaggregated=(role="leader")'

    uri = 'layered:test_layered_fast_truncate_stepup'
    nitems = 1000

    disagg_storages = gen_disagg_storages('test_layered_fast_truncate_stepup', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def setUp(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest("fast truncate support is not enabled.")
        super().setUp()

    def populate_on_leader(self, n=None, ts=10):
        # Write keys 0..n-1 on leader, set stable_timestamp, and checkpoint to stable.
        if n is None:
            n = self.nitems
        cursor = self.session.open_cursor(self.uri)
        for i in range(n):
            self.session.begin_transaction()
            cursor[i] = "v" + str(i)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(ts))
        self.session.checkpoint()

    def switch_to_follower(self):
        # Step down before reopen to avoid the shutdown checkpoint poisoning page-log state.
        self.conn.reconfigure('disaggregated=(role="follower")')
        follower_config = ('disaggregated=(role="follower",'
            f'checkpoint_meta="{self.disagg_get_complete_checkpoint_meta()}")')
        self.reopen_conn(config=follower_config)

    def step_up(self):
        self.conn.reconfigure('disaggregated=(role="leader")')
        # Suppress the benign repeat-pickup warning emitted on the reopen+stepup path.
        self.ignoreStdoutPattern('Picking up the same checkpoint')

    def write_kv(self, key, value, ts):
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[key] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    def truncate_range(self, start_key, stop_key, ts):
        c_start = self.session.open_cursor(self.uri)
        c_start.set_key(start_key)
        c_stop = self.session.open_cursor(self.uri)
        c_stop.set_key(stop_key)
        self.session.begin_transaction()
        self.session.truncate(None, c_start, c_stop, None)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        c_start.close()
        c_stop.close()

    def assert_keys_gone(self, ranges):
        # ranges is a list of (lo, hi) inclusive pairs; assert keys in ranges are not visible
        # and keys outside (within nitems) are visible.
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            cursor.set_key(i)
            ret = cursor.search()
            in_range = any(lo <= i <= hi for lo, hi in ranges)
            if in_range:
                self.assertEqual(ret, wiredtiger.WT_NOTFOUND, f"key {i} should be tombstoned")
            else:
                self.assertEqual(ret, 0, f"key {i} should remain visible")
        cursor.close()

    # ---- Basic coverage ----

    # Stable-only keys: the follower's range walk only touches the boundary keys in ingest.
    # The drain must add stable tombstones for all interior keys.
    def test_stable_only_keys(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(100, 700, 20)
        self.step_up()
        self.assert_keys_gone([(100, 700)])

    # Some keys had a follower update before the truncate; the rest are stable-only.
    # All must be gone after step-up.
    def test_mixed_keys(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        for i in [200, 300, 400, 500, 600]:
            self.write_kv(i, "follower-update", 15)
        self.truncate_range(100, 700, 20)
        self.step_up()
        self.assert_keys_gone([(100, 700)])

    # Multiple non-overlapping truncates must all land.
    def test_multiple_truncates(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(100, 200, 20)
        self.truncate_range(400, 500, 25)
        self.truncate_range(800, 900, 30)
        self.step_up()
        self.assert_keys_gone([(100, 200), (400, 500), (800, 900)])

    # Reinsert after truncate must survive step-up; the drain must not shadow newer writes.
    def test_truncate_then_reinsert(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(100, 700, 20)
        self.write_kv(300, "reinserted", 25)
        self.step_up()
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(300)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "reinserted")
        # And the rest of [100, 700] except 300 must still be gone.
        for i in [100, 150, 250, 400, 500, 700]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)
        cursor.close()

    # ---- Boundary ranges ----

    # Single-key truncate.
    def test_single_key_truncate(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(500, 500, 20)
        self.step_up()
        self.assert_keys_gone([(500, 500)])

    # Truncate covering the very first keys in the table.
    def test_truncate_at_table_start(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(0, 50, 20)
        self.step_up()
        self.assert_keys_gone([(0, 50)])

    # Truncate covering the very last keys in the table.
    def test_truncate_at_table_end(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(950, 999, 20)
        self.step_up()
        self.assert_keys_gone([(950, 999)])

    # Truncate covering the entire populated table.
    def test_truncate_full_table(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(0, 999, 20)
        self.step_up()
        self.assert_keys_gone([(0, 999)])

    # ---- Overlap and repetition ----

    # Two truncate ranges that overlap. Every key in the union must be gone.
    def test_overlapping_truncates(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(100, 400, 20)
        self.truncate_range(300, 600, 25)
        self.step_up()
        self.assert_keys_gone([(100, 600)])

    # The same range truncated twice (identical bounds)  second truncate is redundant
    # but both should be drained without conflict.
    def test_duplicate_truncates(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(200, 500, 20)
        self.truncate_range(200, 500, 25)
        self.step_up()
        self.assert_keys_gone([(200, 500)])

    # ---- Reinsert and re-truncate ----

    # Truncate, reinsert, then re-truncate at a higher timestamp. The chain on stable
    # must reflect each layer at its own timestamp.
    def test_truncate_reinsert_truncate(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(100, 700, 20)
        self.write_kv(300, "reinserted", 25)
        self.truncate_range(100, 700, 30)
        self.step_up()
        # At current/latest read: K=300 must be gone (re-truncate at 30 dominates).
        cursor = self.session.open_cursor(self.uri)
        for i in [100, 300, 500, 700]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
                f"key {i} should be tombstoned after re-truncate")
        cursor.close()
        # Snapshot in (truncate1, reinsert): K=300 must be NOT_FOUND.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(22))
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(300)
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
            "key 300 should be tombstoned at ts=22 (between truncate@20 and reinsert@25)")
        cursor.close()
        self.session.rollback_transaction()
        # Snapshot in (reinsert, re-truncate): K=300 must be the reinserted value.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(27))
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(300)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "reinserted")
        cursor.close()
        self.session.rollback_transaction()

    # ---- Visibility / snapshot reads ----

    # A read at a timestamp before the truncate's commit_ts must still see the
    # original values, both before and after step-up.
    def test_snapshot_read_before_truncate(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(100, 700, 20)
        self.step_up()
        # Read at ts=15 (after populate at ts=10, before truncate at ts=20).
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        cursor = self.session.open_cursor(self.uri)
        for i in [100, 250, 500, 700]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0, f"key {i} should be visible at ts=15")
            self.assertEqual(cursor.get_value(), "v" + str(i))
        cursor.close()
        self.session.rollback_transaction()

    # A read at a timestamp at or after the truncate's commit_ts must see the
    # truncate's effect.
    def test_snapshot_read_after_truncate(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(100, 700, 20)
        self.step_up()
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(30))
        cursor = self.session.open_cursor(self.uri)
        for i in [100, 250, 500, 700]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
                f"key {i} should be tombstoned at ts=30")
        for i in [50, 800]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0, f"key {i} should remain visible at ts=30")
        cursor.close()
        self.session.rollback_transaction()

    # ---- Pure leader-only path (no follower truncate at all) ----

    # Step up on a connection with an empty truncate list. The drain must be a no-op
    # and not perturb the existing stable contents.
    def test_stepup_with_empty_truncate_list(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        # No follower truncates.
        self.step_up()
        cursor = self.session.open_cursor(self.uri)
        for i in [0, 100, 500, 999]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0, f"key {i} should remain visible")
            self.assertEqual(cursor.get_value(), "v" + str(i))
        cursor.close()

    # ---- Post-stepup writes ----

    # After step-up, the new leader must be able to write to keys that were just
    # truncated and have those writes be visible.
    def test_post_stepup_writes_to_truncated_range(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(100, 700, 20)
        self.step_up()
        # Write at a timestamp newer than the truncate's commit_ts.
        self.write_kv(300, "after-stepup", 30)
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(300)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "after-stepup")
        cursor.close()

    # ---- Intermediate-timestamp reads (the cases that catch missing-tombstone bugs) ----

    # Stable-only key truncated then reinserted. Read at a timestamp strictly between
    # the truncate (20) and the reinsert (25) must return NOTFOUND.
    #
    # This is the FIXME at the top of this file: K wasn't in ingest before the truncate,
    # so the follower's range-walk on ingest produced no entry for K. After reinsert, ingest
    # has only real(25) at K. The drain's `ingest_cursor->search` returns 0 → has_prior_ingest
    # = true → drain skips K. Main loop builds chain real(25) → orig(10) on stable, with no
    # tombstone at ts=20. A read at ts=22 incorrectly returns orig(10).
    def test_intermediate_read_truncate_then_reinsert(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(100, 700, 20)
        self.write_kv(300, "reinserted", 25)
        self.step_up()
        # At ts=22 the truncate has happened, the reinsert has not.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(22))
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(300)
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
            "key 300 should be tombstoned at ts=22 (between truncate@20 and reinsert@25)")
        cursor.close()
        self.session.rollback_transaction()

    # Two truncates over overlapping ranges at distinct timestamps. A read at a timestamp
    # strictly between them must reflect the first truncate but not the second.
    def test_intermediate_read_overlapping_truncates(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        self.truncate_range(100, 400, 20)
        self.truncate_range(300, 600, 30)
        self.step_up()
        # At ts=25: keys in [100, 400] are gone (first truncate), keys in (400, 600] are
        # still visible (second truncate not yet at this read).
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(25))
        cursor = self.session.open_cursor(self.uri)
        for i in [100, 200, 350, 400]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
                f"key {i} should be tombstoned at ts=25")
        for i in [450, 500, 600]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0, f"key {i} should still be visible at ts=25")
        cursor.close()
        self.session.rollback_transaction()
        # At ts=35: union [100, 600] is gone.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(35))
        cursor = self.session.open_cursor(self.uri)
        for i in [100, 250, 350, 450, 600]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
                f"key {i} should be tombstoned at ts=35")
        cursor.close()
        self.session.rollback_transaction()

    # Per-key partition: in one truncate range, some keys are stable-only (drain splices),
    # some had a follower update (main loop drains), some had a follower update + reinsert
    # after truncate (mixed chain). All must be gone at a read between truncate and reinsert,
    # and the reinserted ones must be visible at a later read.
    def test_intermediate_read_mixed_per_key_history(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        # Pre-truncate follower updates on some keys.
        for i in [200, 400]:
            self.write_kv(i, "follower-pre", 15)
        self.truncate_range(100, 700, 20)
        # Reinsert a stable-only key (300) and a follower-updated key (400) after truncate.
        self.write_kv(300, "reinserted-stable-only", 25)
        self.write_kv(400, "reinserted-follower-updated", 25)
        self.step_up()
        # ts=22: truncate happened, reinserts not yet.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(22))
        cursor = self.session.open_cursor(self.uri)
        for i in [100, 200, 300, 400, 500, 700]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
                f"key {i} should be tombstoned at ts=22")
        cursor.close()
        self.session.rollback_transaction()
        # ts=30: reinserts visible, the rest of [100, 700] still gone.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(30))
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(300)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "reinserted-stable-only")
        cursor.set_key(400)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), "reinserted-follower-updated")
        for i in [100, 200, 500, 700]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
                f"key {i} should be tombstoned at ts=30")
        cursor.close()
        self.session.rollback_transaction()

    # Truncate range that covers only keys that were never written. Drain's stable
    # iter_cursor must handle WT_NOTFOUND on the range cleanly and produce no chains.
    def test_truncate_empty_range(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()
        # nitems is 1000, so [2000, 3000] is empty on stable.
        self.truncate_range(2000, 3000, 20)
        self.step_up()
        # Existing keys must be untouched.
        cursor = self.session.open_cursor(self.uri)
        for i in [0, 100, 500, 999]:
            cursor.set_key(i)
            self.assertEqual(cursor.search(), 0, f"key {i} should still be visible")
            self.assertEqual(cursor.get_value(), "v" + str(i))
        cursor.close()
