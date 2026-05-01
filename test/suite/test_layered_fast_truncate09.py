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

# Coverage for the step-up replay of pending follower truncates: stable must reflect
# every committed follower truncate after the connection becomes leader, regardless of
# whether the truncated keys had follower-side updates, were reinserted post-truncate,
# or carried only sentinel deletions on ingest.
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

    # --- Fixture helpers ---

    def populate_on_leader(self, ts=10):
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            self.session.begin_transaction()
            cursor[i] = "v" + str(i)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(ts))
        self.session.checkpoint()

    def switch_to_follower(self):
        # Step down before reopen so the shutdown checkpoint doesn't poison page-log state.
        self.conn.reconfigure('disaggregated=(role="follower")')
        follower_config = ('disaggregated=(role="follower",'
            f'checkpoint_meta="{self.disagg_get_complete_checkpoint_meta()}")')
        self.reopen_conn(config=follower_config)

    def step_up(self):
        self.conn.reconfigure('disaggregated=(role="leader")')
        self.ignoreStdoutPattern('Picking up the same checkpoint')

    # Every test runs the same shape: create + populate on the leader + switch to
    # follower. Tests then issue follower ops and call step_up themselves.
    def setup_follower(self):
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.populate_on_leader()
        self.switch_to_follower()

    # --- Operation helpers ---

    def write_kv(self, key, value, ts):
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor[key] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(ts))
        cursor.close()

    def remove_kv(self, key, ts):
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(key)
        self.session.begin_transaction()
        cursor.remove()
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

    # --- Assertion helpers (all support an optional read timestamp) ---

    def _maybe_begin(self, ts):
        if ts is not None:
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))

    def _maybe_rollback(self, ts):
        if ts is not None:
            self.session.rollback_transaction()

    def assert_visible(self, key, value=None, ts=None):
        self._maybe_begin(ts)
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(key)
        self.assertEqual(cursor.search(), 0, f"key {key} should be visible at ts={ts}")
        if value is not None:
            self.assertEqual(cursor.get_value(), value)
        cursor.close()
        self._maybe_rollback(ts)

    def assert_deleted(self, key, ts=None):
        self._maybe_begin(ts)
        cursor = self.session.open_cursor(self.uri)
        cursor.set_key(key)
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
            f"key {key} should be deleted at ts={ts}")
        cursor.close()
        self._maybe_rollback(ts)

    def assert_deleted_keys(self, keys, ts=None):
        self._maybe_begin(ts)
        cursor = self.session.open_cursor(self.uri)
        for k in keys:
            cursor.set_key(k)
            self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
                f"key {k} should be deleted at ts={ts}")
        cursor.close()
        self._maybe_rollback(ts)

    def assert_visible_keys(self, keys, ts=None):
        self._maybe_begin(ts)
        cursor = self.session.open_cursor(self.uri)
        for k in keys:
            cursor.set_key(k)
            self.assertEqual(cursor.search(), 0,
                f"key {k} should be visible at ts={ts}")
        cursor.close()
        self._maybe_rollback(ts)

    def assert_keys_gone(self, ranges):
        # Sweep the populated key space: keys inside any (lo, hi) inclusive range must be
        # deleted, keys outside must remain visible.
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            cursor.set_key(i)
            ret = cursor.search()
            in_range = any(lo <= i <= hi for lo, hi in ranges)
            if in_range:
                self.assertEqual(ret, wiredtiger.WT_NOTFOUND, f"key {i} should be deleted")
            else:
                self.assertEqual(ret, 0, f"key {i} should remain visible")
        cursor.close()

    # ---- Basic coverage ----

    # Stable-only keys: the follower's range walk only ever touched the boundary keys in
    # ingest, so the drain has to add stable tombstones for everything in between.
    def test_stable_only_keys(self):
        self.setup_follower()
        self.truncate_range(100, 700, 20)
        self.step_up()
        self.assert_keys_gone([(100, 700)])

    # Some keys had a follower update before the truncate; the rest are stable-only.
    # Both shapes must end up deleted post step-up.
    def test_mixed_keys(self):
        self.setup_follower()
        for i in [200, 300, 400, 500, 600]:
            self.write_kv(i, "follower-update", 15)
        self.truncate_range(100, 700, 20)
        self.step_up()
        self.assert_keys_gone([(100, 700)])

    # Multiple non-overlapping truncates must all land independently.
    def test_multiple_truncates(self):
        self.setup_follower()
        self.truncate_range(100, 200, 20)
        self.truncate_range(400, 500, 25)
        self.truncate_range(800, 900, 30)
        self.step_up()
        self.assert_keys_gone([(100, 200), (400, 500), (800, 900)])

    # A reinsert after the truncate must survive step-up; the drain mustn't shadow it.
    def test_truncate_then_reinsert(self):
        self.setup_follower()
        self.truncate_range(100, 700, 20)
        self.write_kv(300, "reinserted", 25)
        self.step_up()
        self.assert_visible(300, "reinserted")
        self.assert_deleted_keys([100, 150, 250, 400, 500, 700])

    # ---- Boundary ranges ----

    def test_single_key_truncate(self):
        self.setup_follower()
        self.truncate_range(500, 500, 20)
        self.step_up()
        self.assert_keys_gone([(500, 500)])

    def test_truncate_at_table_start(self):
        self.setup_follower()
        self.truncate_range(0, 50, 20)
        self.step_up()
        self.assert_keys_gone([(0, 50)])

    def test_truncate_at_table_end(self):
        self.setup_follower()
        self.truncate_range(950, 999, 20)
        self.step_up()
        self.assert_keys_gone([(950, 999)])

    def test_truncate_full_table(self):
        self.setup_follower()
        self.truncate_range(0, 999, 20)
        self.step_up()
        self.assert_keys_gone([(0, 999)])

    # The range is fully outside the populated key space; the existing rows must not be
    # disturbed and the drain must handle the empty walk cleanly.
    def test_truncate_empty_range(self):
        self.setup_follower()
        self.truncate_range(2000, 3000, 20)
        self.step_up()
        for i in [0, 100, 500, 999]:
            self.assert_visible(i, "v" + str(i))

    # ---- Repetition ----

    # The same range truncated twice. Second is redundant, but the queue must still drain
    # cleanly.
    def test_duplicate_truncates(self):
        self.setup_follower()
        self.truncate_range(200, 500, 20)
        self.truncate_range(200, 500, 25)
        self.step_up()
        self.assert_keys_gone([(200, 500)])

    # Truncate, reinsert, then re-truncate at a higher timestamp. Reads at intermediate
    # timestamps must observe each layer's effect at its own timestamp.
    def test_truncate_reinsert_truncate(self):
        self.setup_follower()
        self.truncate_range(100, 700, 20)
        self.write_kv(300, "reinserted", 25)
        self.truncate_range(100, 700, 30)
        self.step_up()
        # Latest read: re-truncate dominates.
        self.assert_deleted_keys([100, 300, 500, 700])
        # Between truncate1 and reinsert: deleted.
        self.assert_deleted(300, ts=22)
        # Between reinsert and re-truncate: reinserted value.
        self.assert_visible(300, "reinserted", ts=27)

    # ---- Visibility / snapshot reads ----

    # Reads at timestamps around a single truncate. Pre-truncate reads see the original
    # values, at-or-after reads see the deletion.
    def test_snapshot_read_around_truncate(self):
        self.setup_follower()
        self.truncate_range(100, 700, 20)
        self.step_up()
        for i in [100, 250, 500, 700]:
            self.assert_visible(i, "v" + str(i), ts=15)
        self.assert_deleted_keys([100, 250, 500, 700], ts=30)
        self.assert_visible_keys([50, 800], ts=30)

    # Stable-only key truncated then reinserted: reads in the gap must see the deletion.
    # Without the replay tombstone, stable would only carry the reinsert and a read at
    # the gap timestamp would incorrectly return the original value.
    def test_intermediate_read_truncate_then_reinsert(self):
        self.setup_follower()
        self.truncate_range(100, 700, 20)
        self.write_kv(300, "reinserted", 25)
        self.step_up()
        self.assert_deleted(300, ts=22)

    # Two truncates at distinct timestamps over overlapping ranges. Reads between them
    # must reflect the first but not the second; reads after both must reflect the union.
    def test_intermediate_read_overlapping_truncates(self):
        self.setup_follower()
        self.truncate_range(100, 400, 20)
        self.truncate_range(300, 600, 30)
        self.step_up()
        self.assert_deleted_keys([100, 200, 350, 400], ts=25)
        self.assert_visible_keys([450, 500, 600], ts=25)
        self.assert_deleted_keys([100, 250, 350, 450, 600], ts=35)

    # Per-key partition: stable-only, follower-updated, follower-updated-then-reinserted,
    # and stable-only-then-reinserted all coexist in one truncate range. Each shape must
    # behave correctly at the gap and post-reinsert timestamps.
    def test_intermediate_read_mixed_per_key_history(self):
        self.setup_follower()
        for i in [200, 400]:
            self.write_kv(i, "follower-pre", 15)
        self.truncate_range(100, 700, 20)
        self.write_kv(300, "reinserted-stable-only", 25)
        self.write_kv(400, "reinserted-follower-updated", 25)
        self.step_up()
        # Gap: every key in the range is deleted.
        self.assert_deleted_keys([100, 200, 300, 400, 500, 700], ts=22)
        # Post-reinserts: only the reinserted keys come back.
        self.assert_visible(300, "reinserted-stable-only", ts=30)
        self.assert_visible(400, "reinserted-follower-updated", ts=30)
        self.assert_deleted_keys([100, 200, 500, 700], ts=30)

    # ---- Step-up edge cases ----

    # No follower truncates at all. Step-up's replay block must be a no-op and leave
    # stable contents unchanged.
    def test_stepup_with_empty_truncate_list(self):
        self.setup_follower()
        self.step_up()
        for i in [0, 100, 500, 999]:
            self.assert_visible(i, "v" + str(i))

    # The new leader must accept writes to the freshly-truncated range and have them be
    # visible.
    def test_post_stepup_writes_to_truncated_range(self):
        self.setup_follower()
        self.truncate_range(100, 700, 20)
        self.step_up()
        self.write_kv(300, "after-stepup", 30)
        self.assert_visible(300, "after-stepup")

    # A read at exactly the truncate's commit timestamp must observe the deletion. This
    # mirrors the inclusive snapshot used internally during replay and is the boundary
    # case that distinguishes inclusive-vs-exclusive snapshot semantics.
    def test_read_at_truncate_timestamp(self):
        self.setup_follower()
        self.truncate_range(100, 700, 20)
        self.step_up()
        self.assert_deleted_keys([100, 250, 500, 700], ts=20)

    # ---- Sentinel-only ingest state and same-range remove + truncate ----

    # A follower remove with no follower truncate. The deletion lives only on the ingest
    # chain at step-up time; stable must reflect it after the drain runs.
    def test_layered_remove_only(self):
        self.setup_follower()
        self.remove_kv(300, 20)
        self.step_up()
        self.assert_deleted(300)
        self.assert_visible(300, "v300", ts=15)

    # A remove and a truncate sharing the same commit timestamp on K's range. With the
    # remove's sentinel committed at exactly the truncate's start_ts, the truncate's
    # snapshot must include start_ts to recognize the sentinel; otherwise replay sees an
    # apparently empty ingest chain for K and stacks a redundant stable tombstone.
    def test_layered_remove_and_truncate_same_ts(self):
        self.setup_follower()
        self.remove_kv(300, 20)
        self.truncate_range(100, 700, 20)
        self.step_up()
        for ts in [20, 22, 30]:
            self.assert_deleted(300, ts=ts)
        self.assert_visible(300, "v300", ts=19)
        self.assert_deleted_keys([100, 250, 500, 700])

    # Staggered shape: remove first, truncate later. The intervening gap means the
    # sentinel is visible under any reasonable snapshot, so the replay is unambiguous;
    # included for typical-flow coverage.
    def test_layered_remove_then_truncate(self):
        self.setup_follower()
        self.remove_kv(300, 20)
        self.truncate_range(100, 700, 25)
        self.step_up()
        for ts in [20, 22, 25, 30]:
            self.assert_deleted(300, ts=ts)
        self.assert_visible(300, "v300", ts=15)
        self.assert_deleted_keys([100, 250, 500, 700])

    # A truncate covers a key whose only ingest entry is a sentinel left by a much
    # earlier remove. The sentinel is a regular value at the ingest btree level, so the
    # replay path correctly defers to the regular drain rather than stamping a redundant
    # tombstone.
    def test_truncate_over_pre_existing_sentinel(self):
        self.setup_follower()
        self.remove_kv(300, 15)
        self.truncate_range(100, 700, 30)
        self.step_up()
        for ts in [15, 20, 30, 40]:
            self.assert_deleted(300, ts=ts)
        self.assert_visible(300, "v300", ts=12)
