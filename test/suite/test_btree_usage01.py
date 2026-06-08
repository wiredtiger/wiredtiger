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

# test_btree_usage01.py
#   WT-17717: btree usage sampling -- the sweep server samples per-btree
#   activity into a top-N (plus lottery) snapshot, exposed on the connection
#   statistics cursor as a rank leaderboard ("usage_rank_01: ...") plus
#   per-btree detail keyed by identity ("usage_(id=N)_<uri>: ...").

import re
import wttest

# The sampling period is compiled in (WT_BTREE_SAMPLE_PERIOD): one in this many
# cursor ops fires a sample, so a meaningful signal needs many more ops than this.
SAMPLE_PERIOD = 1000

class test_btree_usage01(wttest.WiredTigerTestCase):
    # A short sweep scan so the workload snapshot refreshes ~1/second; keep idle
    # time long so our table's handle isn't swept closed mid-test.
    conn_config = 'statistics=(fast),' + \
        'file_manager=(close_scan_interval=1,close_idle_time=100,close_handle_minimum=250)'
    uri = 'table:bu01'

    def btree_id(self, name):
        # The btree (file) id lives in the file's metadata config as "id=N".
        c = self.session.open_cursor('metadata:', None, None)
        try:
            c.set_key('file:' + name + '.wt')
            self.assertEqual(c.search(), 0)
            conf = c.get_value()
        finally:
            c.close()
        m = re.search(r'id=(\d+)', conf)
        self.assertIsNotNone(m)
        return int(m.group(1))

    def read_usage(self):
        # Iterate the connection statistics cursor and collect the appended btree-usage
        # virtual entries. Detail entries are keyed "usage_(id=N)_<uri>: <field>"; return
        # {(id, field): value}, the sanitized URI seen per id, and whether the leaderboard
        # and sample slots are present in the key space.
        detail_pat = re.compile(r'^usage_\(id=(\d+)\)_([^:]*): (.*)$')
        detail = {}
        idents = {}
        have_rank = have_sample = False
        c = self.session.open_cursor('statistics:', None, None)
        try:
            while c.next() == 0:
                desc, _value_str, val = c.get_value()
                m = detail_pat.match(desc)
                if m is not None:
                    bid, ident, field = int(m.group(1)), m.group(2), m.group(3)
                    detail[(bid, field)] = val
                    idents[bid] = ident
                elif desc.startswith('usage_rank_'):
                    have_rank = True
                elif desc.startswith('usage_sample:'):
                    have_sample = True
        finally:
            c.close()
        return detail, idents, have_rank, have_sample

    def test_workload_sampling(self):
        self.session.create(self.uri,
            'key_format=S,value_format=S,leaf_page_max=8KB,internal_page_max=8KB')
        bid = self.btree_id('bu01')

        value = 'v' * 50
        cursor = self.session.open_cursor(self.uri, None, None)

        # Insert in increasing key order (append pattern) in batches, reading the
        # snapshot between batches with no idle gap. The snapshot resets each sweep
        # interval, so continuous insertion keeps every interval active; we don't
        # sleep (an idle sweep would zero the snapshot before we read).
        I_RIGHT = 'number of sampled inserts on the rightmost leaf'
        I_MID = 'number of sampled inserts on a middle leaf'
        I_LEFT = 'number of sampled inserts on the leftmost leaf'

        F_SPLITS = 'estimated number of leaf splits'
        F_DATA = 'sampled value-size observation count'
        F_STREAK = 'consecutive intervals in top set'

        # Capture a single snapshot for the position-dominance check (which is only
        # meaningful within one interval), and separately track whether splits, value
        # sizes, and a multi-interval persistence streak were ever observed (each is
        # sparse and/or accrues across the per-interval snapshot resets).
        batch = 20000
        key = 0
        found = None
        max_splits = max_data = max_streak = 0
        for _ in range(40):
            for _ in range(batch):
                cursor[('%012d' % key)] = value
                key += 1
            detail, idents, have_rank, have_sample = self.read_usage()
            if (bid, I_RIGHT) not in detail:
                continue
            max_splits = max(max_splits, detail.get((bid, F_SPLITS), 0))
            max_data = max(max_data, detail.get((bid, F_DATA), 0))
            max_streak = max(max_streak, detail.get((bid, F_STREAK), 0))
            if found is None and detail.get((bid, I_RIGHT), 0) > 0:
                found = (detail, idents, have_rank, have_sample)
            if found is not None and max_splits > 0 and max_data > 0 and max_streak >= 2:
                break

        cursor.close()

        self.assertIsNotNone(found,
            'no rightmost-insert samples for btree id %d after %d keys' % (bid, key))
        detail, idents, have_rank, have_sample = found

        right = detail.get((bid, I_RIGHT), 0)
        mid = detail.get((bid, I_MID), 0)
        left = detail.get((bid, I_LEFT), 0)

        # Append workload: once the tree is multi-leaf, inserts land on the
        # rightmost leaf, so the rightmost bucket should dominate.
        self.assertGreater(right, 0, 'expected rightmost inserts to be sampled')
        self.assertGreaterEqual(right, mid, 'rightmost inserts should dominate middle')
        self.assertGreaterEqual(right, left, 'rightmost inserts should dominate left')

        # The snapshot is per-interval, so compare within the interval: rightmost should be the
        # overwhelming majority of this interval's sampled inserts (append workload), not a fluke.
        insert_total = right + mid + left
        self.assertGreater(insert_total, 0, 'expected insert samples in the captured interval')
        self.assertGreater(right * 100, insert_total * 80,
            'rightmost inserts %d should dominate interval insert total %d' % (right, insert_total))

        # Splits happened (append into 8KB leaves), and value sizes were sampled.
        self.assertGreater(max_splits, 0, 'expected leaf splits to be sampled')
        self.assertGreater(max_data, 0, 'expected value sizes to be sampled')

        # Identity now lives in the key: the detail section is keyed by btree id and the
        # sanitized URI (file:bu01.wt -> bu01_wt).
        self.assertEqual(idents.get(bid), 'bu01_wt',
            'detail key should embed the sanitized btree URI')

        # Our single hot table stays in the top set across intervals, so the persistence
        # streak carries forward past 1 (validates the id-keyed streak inheritance).
        self.assertGreaterEqual(max_streak, 2, 'expected a persistence streak to grow past 1')

        # The rank leaderboard and the sample slot exist in the key space.
        self.assertTrue(have_rank, 'expected a usage_rank leaderboard in the stat key space')
        _detail, _idents, _have_rank, have_sample = self.read_usage()
        self.assertTrue(have_sample, 'expected a usage_sample slot in the stat key space')
