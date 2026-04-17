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

import unittest
import wttest, wiredtiger
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered_fast_truncate02.py
#   Validate cursor read-path behaviour over fast-truncated ranges on a
#   standby (follower) node: next/prev scans, search_near positioning,
#   open-ended truncation, multiple truncated ranges, and mixed
#   update-then-truncate workloads.
@disagg_test_class
class test_layered_fast_truncate02(wttest.WiredTigerTestCase):

    conn_config = 'disaggregated=(role="leader"),'

    uris = [
        ('layered', dict(uri='layered:test_layered_fast_truncate02')),
        ('table', dict(uri='table:test_layered_fast_truncate02')),
    ]

    disagg_storages = gen_disagg_storages('test_layered_fast_truncate02', disagg_only=True)

    scenarios = make_scenarios(disagg_storages, uris)

    # Total number of keys inserted. String keys are zero-padded to four
    # digits so that lexicographic order matches numeric order.
    nitems = 1000

    def key(self, n):
        return f'{n:04d}'

    def session_create_config(self):
        cfg = 'key_format=S,value_format=S'
        if self.uri.startswith('table'):
            cfg += ',block_manager=disagg,type=layered'
        return cfg

    # Populate the table on the leader, checkpoint, then reopen as follower.
    def setup_follower(self):
        self.session.create(self.uri, self.session_create_config())
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            self.session.begin_transaction()
            cursor[self.key(i)] = 'value'
            self.session.commit_transaction()
        cursor.close()
        self.session.checkpoint()

        follower_config = (
            'disaggregated=(role="follower",'
            f'checkpoint_meta="{self.disagg_get_complete_checkpoint_meta()}")'
        )
        self.reopen_conn(config=follower_config)

    # Truncate the range [start, stop] (inclusive). If stop is None, truncate
    # from start to the end of the table.
    def truncate_range(self, start, stop):
        c1 = self.session.open_cursor(self.uri)
        c1.set_key(self.key(start))
        c2 = None
        if stop is not None:
            c2 = self.session.open_cursor(self.uri)
            c2.set_key(self.key(stop))
        self.session.begin_transaction()
        self.session.truncate(None, c1, c2, None)
        self.session.commit_transaction()
        c1.close()
        if c2 is not None:
            c2.close()

    # Return all keys visible via a forward scan.
    def scan_forward(self):
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        keys = []
        while cursor.next() == 0:
            keys.append(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()
        return keys

    # Return all keys visible via a backward scan.
    def scan_backward(self):
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        keys = []
        while cursor.prev() == 0:
            keys.append(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()
        return [k for k in reversed(keys)]  # reverse so order matches forward scan

    def test_cursor_scan_skips_truncated_range(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        self.setup_follower()
        self.truncate_range(100, 700)

        expected = [self.key(i) for i in range(self.nitems)
                    if i < 100 or i > 700]

        # Forward scan must return exactly the non-truncated keys.
        forward = self.scan_forward()
        self.assertEqual(forward, expected,
            'forward scan returned wrong keys after truncating range [100, 700]')

        # Backward scan must return the same set in the same sorted order.
        backward = self.scan_backward()
        self.assertEqual(backward, expected,
            'backward scan returned wrong keys after truncating range [100, 700]')

    def test_search_near_inside_truncated_range(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        self.setup_follower()
        self.truncate_range(100, 700)

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()

        # Search for a key deep inside the truncated range.
        cursor.set_key(self.key(400))
        exact = cursor.search_near()

        # The returned key must be outside the truncated range.
        result_key = cursor.get_key()
        in_truncated_range = (self.key(100) <= result_key <= self.key(700))
        self.assertFalse(in_truncated_range,
            f'search_near("0400") returned truncated key {result_key}')

        # search_near must not report an exact match inside a deleted range.
        self.assertNotEqual(exact, 0,
            'search_near("0400") reported exact=0 for a key in the truncated range')

        self.session.rollback_transaction()
        cursor.close()

    def test_search_near_at_truncate_boundary(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        self.setup_follower()
        self.truncate_range(100, 700)

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()

        # Searching exactly at the start boundary must land outside the range.
        cursor.set_key(self.key(100))
        cursor.search_near()
        start_result = cursor.get_key()
        self.assertFalse(self.key(100) <= start_result <= self.key(700),
            f'search_near("0100") returned truncated key {start_result}')

        # Searching exactly at the stop boundary must land outside the range.
        cursor.set_key(self.key(700))
        cursor.search_near()
        stop_result = cursor.get_key()
        self.assertFalse(self.key(100) <= stop_result <= self.key(700),
            f'search_near("0700") returned truncated key {stop_result}')

        self.session.rollback_transaction()
        cursor.close()

    def test_truncate_to_end_of_table(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        self.setup_follower()
        self.truncate_range(500, None)

        expected = [self.key(i) for i in range(500)]

        forward = self.scan_forward()
        self.assertEqual(forward, expected,
            'scan returned wrong keys after truncating from key 500 to end')

        backward = self.scan_backward()
        self.assertEqual(backward, expected,
            'backward scan returned wrong keys after truncating from key 500 to end')

    def test_multiple_truncate_ranges(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        self.setup_follower()
        self.truncate_range(100, 300)
        self.truncate_range(600, 800)

        expected = [self.key(i) for i in range(self.nitems)
                    if not (100 <= i <= 300) and not (600 <= i <= 800)]

        forward = self.scan_forward()
        self.assertEqual(forward, expected,
            'scan returned wrong keys with two truncated ranges [100,300] and [600,800]')

        backward = self.scan_backward()
        self.assertEqual(backward, expected,
            'backward scan returned wrong keys with two truncated ranges')

    def test_mixed_bounded_and_open_ended_truncates(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        # Combine a bounded truncate [100, 300] with an open-ended truncate
        # [600, end]. Visible keys must be exactly 0-99 and 301-599.
        self.setup_follower()
        self.truncate_range(100, 300)
        self.truncate_range(600, None)

        expected = [self.key(i) for i in range(self.nitems)
                    if i < 100 or (301 <= i <= 599)]

        forward = self.scan_forward()
        self.assertEqual(forward, expected,
            'forward scan returned wrong keys with mixed truncates [100,300] and [600,end]')

        backward = self.scan_backward()
        self.assertEqual(backward, expected,
            'backward scan returned wrong keys with mixed truncates [100,300] and [600,end]')

    def test_open_ended_truncate_then_append_then_bounded_to_new_end(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        # Open-ended truncate captures a snapshot of "end" at commit time. Keys
        # appended after the truncate are new data in ingest (the source of
        # truth for writes on the follower) and must remain visible.
        self.setup_follower()

        # Open-ended truncate [800, end]: hides the existing keys 800-999 only.
        self.truncate_range(800, None)

        # Append new keys 1000-1099 to the follower's ingest. These are written
        # after the truncate committed, so they must remain visible.
        cursor = self.session.open_cursor(self.uri)
        for i in range(1000, 1100):
            self.session.begin_transaction()
            cursor[self.key(i)] = 'appended'
            self.session.commit_transaction()
        cursor.close()

        expected = ([self.key(i) for i in range(800)]
                    + [self.key(i) for i in range(1000, 1100)])
        forward = self.scan_forward()
        self.assertEqual(forward, expected,
            'open-ended truncate must not hide keys appended after it committed')

        backward = self.scan_backward()
        self.assertEqual(backward, expected,
            'backward scan must agree with forward scan')

    # FIXME-WT-17133: ingest truncate doesn't remove live ingest keys when the
    # start key is absent from ingest.
    @unittest.skip("FIXME-WT-17133")
    def test_mixed_truncate_and_update(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        # Insert base data on the leader.
        self.session.create(self.uri, self.session_create_config())
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            self.session.begin_transaction()
            cursor[self.key(i)] = 'original'
            self.session.commit_transaction()
        cursor.close()
        self.session.checkpoint()

        follower_config = (
            'disaggregated=(role="follower",'
            f'checkpoint_meta="{self.disagg_get_complete_checkpoint_meta()}")'
        )
        self.reopen_conn(config=follower_config)

        # Update a subset of keys inside what will become the truncated range.
        cursor = self.session.open_cursor(self.uri)
        for i in range(200, 401):
            self.session.begin_transaction()
            cursor[self.key(i)] = 'updated'
            self.session.commit_transaction()
        cursor.close()

        # Truncate a range that covers the updated keys.
        self.truncate_range(100, 700)

        # All keys from 100 to 700 must be invisible, even those that were
        # updated after the original insert.
        expected = [self.key(i) for i in range(self.nitems)
                    if i < 100 or i > 700]

        forward = self.scan_forward()
        self.assertEqual(forward, expected,
            'scan returned wrong keys after mixed update+truncate workload')

        # Spot-check that updated keys inside the range are not findable.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor.set_key(self.key(300))
        result = cursor.search()
        self.assertEqual(result, wiredtiger.WT_NOTFOUND,
            'search for updated-then-truncated key "0300" should return WT_NOTFOUND')
        self.session.rollback_transaction()
        cursor.close()

    def test_search_returns_not_found_in_truncated_range(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        # Exercise the __clayered_lookup + truncate-check code path directly via
        # cursor.search(), which is separate from the scan and search_near paths.
        self.setup_follower()
        self.truncate_range(100, 700)

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()

        # Key deep inside the truncated range must not be found.
        cursor.set_key(self.key(400))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
            'search("0400") inside truncated range [100,700] must return WT_NOTFOUND')

        # Keys at the inclusive start and stop boundaries must not be found.
        cursor.set_key(self.key(100))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
            'search("0100") at start boundary of truncated range must return WT_NOTFOUND')

        cursor.set_key(self.key(700))
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND,
            'search("0700") at stop boundary of truncated range must return WT_NOTFOUND')

        # Keys just outside the truncated range must still be found.
        cursor.set_key(self.key(99))
        self.assertEqual(cursor.search(), 0,
            'search("0099") just before truncated range must succeed')

        cursor.set_key(self.key(701))
        self.assertEqual(cursor.search(), 0,
            'search("0701") just after truncated range must succeed')

        self.session.rollback_transaction()
        cursor.close()

    def test_search_near_direction_in_truncated_range(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        # search_near for a key inside a truncated range must land outside the
        # range: try forward first, fall back to backward only if forward exhausts.
        self.setup_follower()

        # Scenario 1: bounded range [100, 700]. Visible keys 701-999 exist after
        # the range, so __clayered_iterate(NEXT) succeeds and lands at 0701.
        self.truncate_range(100, 700)

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor.set_key(self.key(400))
        exact = cursor.search_near()
        result_key = cursor.get_key()
        self.assertEqual(exact, 1,
            f'bounded range: search_near should advance forward (exact=1), got {exact}')
        self.assertEqual(result_key, self.key(701),
            f'bounded range: expected to land at 0701, got {result_key}')
        self.session.rollback_transaction()
        cursor.close()

        # Scenario 2: add an open-ended truncation [800, end]. Combined truncated
        # ranges are [100, 700] and [800, 999]. search_near("0900"): NEXT exhausts
        # the table (all keys >= 800 are truncated), so the fix falls back to PREV
        # and lands at 0799 (the highest visible key).
        self.truncate_range(800, None)

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        cursor.set_key(self.key(900))
        exact = cursor.search_near()
        result_key = cursor.get_key()
        self.assertEqual(exact, -1,
            f'open-ended range: search_near should fall back backward (exact=-1), got {exact}')
        self.assertEqual(result_key, self.key(799),
            f'open-ended range: expected to land at 0799, got {result_key}')
        self.session.rollback_transaction()
        cursor.close()

    def test_overlapping_truncated_ranges_scan(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        # Two overlapping truncated ranges: [100, 400] and [300, 700]. When the
        # stable-cursor reposition after the first range lands inside the second
        # range, __clayered_leader_reposition_iterate must loop again to skip past
        # the second range as well.
        self.setup_follower()
        self.truncate_range(100, 400)
        self.truncate_range(300, 700)

        # The union of the two ranges covers [100, 700]; keys outside are visible.
        expected = [self.key(i) for i in range(self.nitems)
                    if i < 100 or i > 700]

        forward = self.scan_forward()
        self.assertEqual(forward, expected,
            'forward scan must skip the full union of overlapping truncated ranges [100,400] and [300,700]')

        backward = self.scan_backward()
        self.assertEqual(backward, expected,
            'backward scan must skip the full union of overlapping truncated ranges [100,400] and [300,700]')

    def test_entire_table_truncated(self):
        if wiredtiger.disagg_fast_truncate_build() == 0:
            self.skipTest('fast truncate support is not enabled.')

        # Truncating every key (0 to nitems-1) must leave the table completely
        # empty. The first cursor.next() call should immediately return WT_NOTFOUND.
        self.setup_follower()
        self.truncate_range(0, self.nitems - 1)

        forward = self.scan_forward()
        self.assertEqual(forward, [],
            'scan must return no keys when the entire table is truncated')

        backward = self.scan_backward()
        self.assertEqual(backward, [],
            'backward scan must return no keys when the entire table is truncated')
