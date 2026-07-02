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

import random
import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered_async_stepdown02.py
#    Deeper correctness scenarios for async (elegant) step-down routing: time-travel reads across
#    the cutoff, readers surviving the arm, iteration re-seating across the arm, merged point and
#    range lookups, duplicate detection, read-your-own-writes, a shared global cutoff across tables,
#    and a model-based randomized stress test that compares the merged layered view to an oracle.
@disagg_test_class
class test_layered_async_stepdown02(wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def set_global_ts(self, oldest, stable):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(oldest) +
                                ',stable_timestamp=' + self.timestamp_str(stable))

    def arm(self, ts):
        self.conn.set_timestamp('step_down_ts=' + self.timestamp_str(ts))

    # Write k/v pairs (dict) to a layered table in one transaction at commit_ts.
    def write_at(self, uri, items, commit_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        for k, v in items.items():
            cursor[k] = v
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    # The merged key/value map visible through a layered cursor at read_ts.
    def kv_of(self, uri, read_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        kv = {}
        while cursor.next() == 0:
            kv[cursor.get_key()] = cursor.get_value()
        self.session.rollback_transaction()
        cursor.close()
        return kv

    # The keys physically resident in a constituent (ingest/stable) file at read_ts.
    def keys_of(self, uri, read_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
        keys = set()
        while cursor.next() == 0:
            keys.add(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()
        return keys

    # A key written in stable before the cutoff and overwritten/removed in ingest after the cutoff
    # must read its stable version below the post-cutoff commit and its ingest version at or above.
    def test_time_travel_reads_across_cutoff(self):
        uri = 'layered:tt'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        # Pre-cutoff stable versions at ts 10.
        self.write_at(uri, {'upd': 'old', 'del': 'present'}, 10)

        self.arm(20)

        # Post-cutoff ingest versions at ts 30: one overwrite, one removal.
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        cursor['upd'] = 'new'
        cursor.set_key('del')
        cursor.remove()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        # Reading below the post-cutoff commit sees the stable versions (ingest not yet visible).
        self.assertEqual(self.kv_of(uri, 25), {'upd': 'old', 'del': 'present'})

        # Reading at/after the post-cutoff commit sees the ingest versions: overwrite and tombstone.
        self.assertEqual(self.kv_of(uri, 35), {'upd': 'new'})

    # A read-only transaction open across the arm is never rolled back and keeps reading correctly.
    def test_reader_survives_arm(self):
        uri = 'layered:reader'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'a': 'pre', 'b': 'pre', 'c': 'pre'}, 10)

        # Open a reader and take its snapshot before the cutoff is armed.
        rcur = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        self.assertEqual(rcur['a'], 'pre')

        # The server arms the step-down while the reader is open.
        self.arm(20)

        # The reader keeps working: point reads and a full scan, no rollback.
        self.assertEqual(rcur['b'], 'pre')
        rcur.reset()
        seen = set()
        while rcur.next() == 0:
            seen.add(rcur.get_key())
        self.assertEqual(seen, {'a', 'b', 'c'})
        self.session.rollback_transaction()
        rcur.close()

    # An iteration that began before the arm continues correctly afterwards: the cursor re-seats both
    # constituents and returns each key exactly once, in order, with no duplicates or gaps.
    def test_iteration_across_arm(self):
        uri = 'layered:iter'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        keys = ['k%02d' % i for i in range(10)]
        self.write_at(uri, {k: 'v' for k in keys}, 10)

        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(100))

        # Walk part way, then arm mid-iteration.
        seen = []
        for _ in range(4):
            self.assertEqual(cursor.next(), 0)
            seen.append(cursor.get_key())
        self.arm(50)

        # Finish the walk. The arm transition forces a re-seat; the rest must still come back in order.
        while cursor.next() == 0:
            seen.append(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(seen, keys, 'iteration across the arm must yield every key once, in order')

    # Point and range lookups on the leader merge ingest over stable.
    def test_search_and_search_near_merged(self):
        uri = 'layered:search'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        # Even-positioned keys go to stable before the cutoff.
        self.write_at(uri, {'b': 's', 'd': 's', 'f': 's'}, 10)
        self.arm(20)
        # Interleaved keys go to ingest after the cutoff.
        self.write_at(uri, {'a': 'i', 'c': 'i', 'e': 'i'}, 30)

        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))

        # Exact search finds keys from both constituents.
        self.assertEqual(cursor['c'], 'i')
        self.assertEqual(cursor['d'], 's')

        # A miss is a miss across the merged view.
        cursor.set_key('z')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)

        # search_near on a non-existent key positions on an adjacent key from either constituent.
        cursor.set_key('cc')
        cmp = cursor.search_near()
        self.assertNotEqual(cmp, wiredtiger.WT_NOTFOUND)
        self.assertIn(cursor.get_key(), ('c', 'd'))

        # Full merged order interleaves the two constituents.
        cursor.reset()
        order = []
        while cursor.next() == 0:
            order.append(cursor.get_key())
        self.assertEqual(order, ['a', 'b', 'c', 'd', 'e', 'f'])
        self.session.rollback_transaction()
        cursor.close()

    # Duplicate detection consults both constituents: a non-overwrite insert of a key that lives in
    # stable still conflicts after the cutoff, even though the write would target ingest.
    def test_duplicate_key_detection_armed(self):
        uri = 'layered:dup'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'dup': 'stable'}, 10)

        self.arm(20)

        cursor = self.session.open_cursor(uri, None, "overwrite=false")
        self.session.begin_transaction()
        cursor.set_key('dup')
        cursor.set_value('again')
        self.assertRaisesException(wiredtiger.WiredTigerError, lambda: cursor.insert(),
            wiredtiger.wiredtiger_strerror(wiredtiger.WT_DUPLICATE_KEY))
        self.session.rollback_transaction()
        cursor.close()

    # Within a post-cutoff transaction, a write to ingest is visible to a later read in the same
    # transaction, alongside the unchanged stable content.
    def test_read_your_own_writes_post_arm(self):
        uri = 'layered:ryow'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'old': 'stable'}, 10)

        self.arm(20)

        wcur = self.session.open_cursor(uri, None, None)
        rcur = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        wcur['fresh'] = 'mine'
        wcur['old'] = 'mine-too'
        # Same transaction sees its own ingest writes merged over stable.
        self.assertEqual(rcur['fresh'], 'mine')
        self.assertEqual(rcur['old'], 'mine-too')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        wcur.close()
        rcur.close()

        self.assertEqual(self.kv_of(uri, 40), {'old': 'mine-too', 'fresh': 'mine'})

    # The cutoff is global: arming once routes writes to ingest for every layered table.
    def test_multiple_tables_share_cutoff(self):
        uri1 = 'layered:multi1'
        uri2 = 'layered:multi2'
        self.set_global_ts(1, 1)
        self.session.create(uri1, 'key_format=S,value_format=S')
        self.session.create(uri2, 'key_format=S,value_format=S')

        self.write_at(uri1, {'a': 'pre'}, 10)
        self.write_at(uri2, {'b': 'pre'}, 10)

        self.arm(20)

        self.write_at(uri1, {'c': 'post'}, 30)
        self.write_at(uri2, {'d': 'post'}, 30)

        # Each table's post-cutoff write landed in its own ingest constituent.
        self.assertEqual(self.keys_of('file:multi1.wt_ingest', 40), {'c'})
        self.assertEqual(self.keys_of('file:multi2.wt_ingest', 40), {'d'})
        self.assertEqual(self.kv_of(uri1, 40), {'a': 'pre', 'c': 'post'})
        self.assertEqual(self.kv_of(uri2, 40), {'b': 'pre', 'd': 'post'})

    # Model-based stress: a randomized mix of insert/update/modify/remove split across the cutoff,
    # with the merged layered view checked against an in-memory oracle at several points.
    def test_stress_random_ops(self):
        uri = 'layered:stress'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        rng = random.Random(42)
        nkeys = 40
        oracle = {}     # key -> latest value
        self.ts = 1
        cursor = self.session.open_cursor(uri, None, None)

        def rand_key():
            return 'k%02d' % rng.randrange(nkeys)

        # Cross-check a handful of point reads against the oracle (exercises the merge on search()).
        def check_point_reads(read_ts):
            rc = self.session.open_cursor(uri, None, None)
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
            for _ in range(10):
                k = rand_key()
                rc.set_key(k)
                if k in oracle:
                    self.assertEqual(rc.search(), 0)
                    self.assertEqual(rc.get_value(), oracle[k])
                else:
                    self.assertEqual(rc.search(), wiredtiger.WT_NOTFOUND)
            self.session.rollback_transaction()
            rc.close()

        def run_ops(n, verify_every=0):
            for i in range(n):
                self.ts += 1
                k = rand_key()
                roll = rng.random()
                self.session.begin_transaction()
                if roll < 0.55:
                    # Insert or overwrite.
                    v = 'v%d' % self.ts
                    cursor[k] = v
                    oracle[k] = v
                elif roll < 0.75 and k in oracle:
                    # Modify: replace the first byte, built on the current value.
                    cursor.set_key(k)
                    cursor.modify([wiredtiger.Modify('Z', 0, 1)])
                    oracle[k] = 'Z' + oracle[k][1:]
                elif k in oracle:
                    # Remove an existing key.
                    cursor.set_key(k)
                    cursor.remove()
                    del oracle[k]
                else:
                    # Nothing to modify/remove; make it a harmless insert instead.
                    v = 'v%d' % self.ts
                    cursor[k] = v
                    oracle[k] = v
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts))
                if verify_every and (i + 1) % verify_every == 0:
                    self.assertEqual(self.kv_of(uri, self.ts), dict(oracle))
                    check_point_reads(self.ts)

        # Phase 1: pre-cutoff churn, everything routed to stable.
        run_ops(120, verify_every=40)
        self.assertEqual(self.kv_of(uri, self.ts), dict(oracle))
        snapshot_ts = self.ts
        snapshot = dict(oracle)

        # Arm at the current frontier, then phase 2: post-cutoff churn routed to ingest.
        self.arm(self.ts + 1)
        run_ops(120, verify_every=40)

        # The merged view reflects every operation across both constituents.
        self.assertEqual(self.kv_of(uri, self.ts), dict(oracle),
            'merged layered view must match the oracle after the cutoff')
        check_point_reads(self.ts)

        # Time-travel: the view at the arm boundary is unchanged by the later ingest writes.
        self.assertEqual(self.kv_of(uri, snapshot_ts), snapshot,
            'reading at the pre-cutoff frontier must be unaffected by post-cutoff writes')

        cursor.close()
