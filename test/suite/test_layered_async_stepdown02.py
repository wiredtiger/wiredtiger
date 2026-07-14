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
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# test_layered_async_stepdown02.py
#    Read semantics: time-travel reads, iteration across arm, merged lookups, stress test.
@disagg_test_class
class test_layered_async_stepdown02(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # Key in stable pre-arm, overwritten in ingest post-arm; reads see both versions correctly.
    def test_time_travel_reads_across_arm(self):
        uri = 'layered:tt'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        # Pre-arm stable versions at ts 10.
        self.write_at(uri, {'upd': 'old', 'del': 'present'}, 10)

        self.arm(20)

        # Post-arm ingest versions at ts 30: one overwrite, one removal.
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        cursor['upd'] = 'new'
        cursor.set_key('del')
        cursor.remove()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        # Reading below the post-arm commit sees the stable versions (ingest not yet visible).
        self.assertEqual(self.read_kvs_at(uri, 25), {'upd': 'old', 'del': 'present'})

        # Reading at/after the post-arm commit sees the ingest versions: overwrite and tombstone.
        self.assertEqual(self.read_kvs_at(uri, 35), {'upd': 'new'})

        # Ground truth: the post-arm versions never touched the stable table.
        self.assertEqual(self.read_kvs_at(self.stable_uri(uri), 35), {'upd': 'old', 'del': 'present'})

    # Read-only txn open across arm is not rolled back.
    def test_reader_survives_arm(self):
        uri = 'layered:reader'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'a': 'pre', 'b': 'pre', 'c': 'pre'}, 10)

        # Open a reader and take its snapshot before the arm.
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

    # Iteration across arm re-seats correctly, sees merged keys without duplicates or gaps.
    def test_iteration_across_arm(self):
        uri = 'layered:iter'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        # Even-numbered keys form the stable content the scan snapshot will see.
        stable_keys = [f'k{i:02d}' for i in range(0, 20, 2)]
        self.write_at(uri, {k: 'v' for k in stable_keys}, 10)

        # Use a second session for the concurrent post-arm writer so the scan's transaction
        # stays untouched.
        wsession = self.conn.open_session()

        # Read below the concurrent writer's later commit.
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))

        # Walk part way, then arm mid-iteration.
        seen = []
        for _ in range(4):
            self.assertEqual(cursor.next(), 0)
            seen.append(cursor.get_key())
        self.arm(50)

        # A concurrent post-arm transaction interleaves odd-numbered keys into ingest, both
        # behind and ahead of the scan position.
        wcur = wsession.open_cursor(uri, None, None)
        wsession.begin_transaction()
        for i in range(1, 20, 2):
            wcur[f'k{i:02d}'] = 'ingest'
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(60))
        wcur.close()
        wsession.close()

        # Finish the walk. The arm transition forces a re-seat; the ingest keys are invisible to
        # the scan's snapshot, so the rest must still come back in order with no duplicates.
        while cursor.next() == 0:
            seen.append(cursor.get_key())
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(seen, stable_keys,
            'iteration across the arm must yield exactly the snapshot keys once, in order')

        # A fresh scan above the ingest commit merges the interleaved keys from both constituents.
        merged = sorted([f'k{i:02d}' for i in range(20)])
        self.assertEqual(sorted(self.read_kvs_at(uri, 70).keys()), merged)

    # Point/range lookups merge ingest over stable.
    def test_search_and_search_near_merged(self):
        uri = 'layered:search'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        # Even-positioned keys go to stable before the arm.
        self.write_at(uri, {'b': 's', 'd': 's', 'f': 's'}, 10)
        self.arm(20)
        # Interleaved keys go to ingest after the arm; stable keeps only the pre-arm keys.
        self.write_at(uri, {'a': 'i', 'c': 'i', 'e': 'i'}, 30)
        self.assertEqual(self.read_keys_at(self.stable_uri(uri), 40), {'b', 'd', 'f'})

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

    # Non-overwrite insert of a stable key conflicts even when targeting ingest.
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

    # Write to ingest is visible to later read in same post-arm txn.
    def test_read_your_own_writes_post_arm(self):
        uri = 'layered:ryow'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'old': 'stable'}, 10)

        self.arm(20)

        wcur = self.session.open_cursor(uri, None, None)
        rcur = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        wcur['fresh'] = 'ingest'
        wcur['old'] = 'ingest'
        # Same transaction sees its own ingest writes merged over stable.
        self.assertEqual(rcur['fresh'], 'ingest')
        self.assertEqual(rcur['old'], 'ingest')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        wcur.close()
        rcur.close()

        self.assertEqual(self.read_kvs_at(uri, 40), {'old': 'ingest', 'fresh': 'ingest'})

        # Ground truth: both writes landed in ingest; the stable version is untouched.
        self.assertEqual(self.read_kvs_at(self.stable_uri(uri), 40), {'old': 'stable'})

    # Reverse iteration and largest_key pre/post-arm; largest_key is non-transactional.
    def test_prev_and_largest_key_across_arm(self):
        uri = 'layered:revscan'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'b': 's', 'd': 's', 'f': 's'}, 10)

        def prev_keys(read_ts):
            c = self.session.open_cursor(uri, None, None)
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
            keys = []
            while c.prev() == 0:
                keys.append(c.get_key())
            self.session.rollback_transaction()
            c.close()
            return keys

        # largest_key ignores visibility, so no transaction is needed.
        def largest():
            c = self.session.open_cursor(uri, None, None)
            self.assertEqual(c.largest_key(), 0)
            key = c.get_key()
            c.close()
            return key

        # Pre-arm: stable only.
        self.assertEqual(prev_keys(15), ['f', 'd', 'b'])
        self.assertEqual(largest(), 'f')

        self.arm(20)
        # The merged maximum lives in ingest.
        self.write_at(uri, {'a': 'i', 'c': 'i', 'e': 'i', 'z': 'i'}, 30)

        # Post-arm: reverse merged order across both constituents.
        self.assertEqual(prev_keys(40), ['z', 'f', 'e', 'd', 'c', 'b', 'a'])
        self.assertEqual(largest(), 'z')

    # Read ops through straddling reader: snapshot pins stable; ingest invisible except largest_key.
    def test_read_ops_across_arm(self):
        uri = 'layered:readops'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'b': 's', 'd': 's', 'f': 's'}, 10)

        rcur = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        self.assertEqual(rcur['d'], 's')

        self.arm(20)

        # A concurrent post-arm transaction interleaves ingest keys, including a new maximum.
        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(uri, None, None)
        wsession.begin_transaction()
        for k in ('a', 'c', 'e', 'z'):
            wcur[k] = 'i'
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        wcur.close()
        wsession.close()

        # search: stable hit still works, the invisible ingest key is a miss.
        self.assertEqual(rcur['d'], 's')
        rcur.set_key('a')
        self.assertEqual(rcur.search(), wiredtiger.WT_NOTFOUND)

        # search_near: lands on a visible stable neighbour, never the invisible ingest 'c'.
        rcur.set_key('c')
        cmp = rcur.search_near()
        self.assertNotEqual(cmp, wiredtiger.WT_NOTFOUND)
        self.assertIn(rcur.get_key(), ('b', 'd'))

        # prev: the full reverse walk yields exactly the snapshot's keys.
        rcur.reset()
        seen = []
        while rcur.prev() == 0:
            seen.append(rcur.get_key())
        self.assertEqual(seen, ['f', 'd', 'b'])

        # largest_key ignores visibility: it reports the ingest maximum even though this
        # snapshot cannot read it.
        self.assertEqual(rcur.largest_key(), 0)
        self.assertEqual(rcur.get_key(), 'z')

        self.session.rollback_transaction()
        rcur.close()

    # Stress test: randomized ops split across the arm, merged view checked against a shadow map.
    def test_stress_random_ops(self):
        uri = 'layered:stress'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        # The fixed seed keeps the op sequence deterministic.
        seed = 42
        self.pr(f'test_stress_random_ops: random seed {seed}')
        rng = random.Random(seed)
        nkeys = 40
        expected = {} 
        self.ts = 1
        cursor = self.session.open_cursor(uri, None, None)

        def rand_key():
            return f'k{rng.randrange(nkeys):02d}'

        # Cross-check a handful of point reads against the expected contents (exercises the merge
        # on search()).
        def check_point_reads(read_ts):
            rc = self.session.open_cursor(uri, None, None)
            self.session.begin_transaction('read_timestamp=' + self.timestamp_str(read_ts))
            for _ in range(10):
                k = rand_key()
                ctx = f'seed={seed} read_ts={read_ts} key={k}'
                rc.set_key(k)
                if k in expected:
                    self.assertEqual(rc.search(), 0, f'expected hit: {ctx}')
                    self.assertEqual(rc.get_value(), expected[k], f'value mismatch: {ctx}')
                else:
                    self.assertEqual(rc.search(), wiredtiger.WT_NOTFOUND, f'expected miss: {ctx}')
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
                    v = f'v{self.ts}'
                    cursor[k] = v
                    expected[k] = v
                elif roll < 0.75 and k in expected:
                    # Modify: replace the first byte, built on the current value.
                    cursor.set_key(k)
                    cursor.modify([wiredtiger.Modify('Z', 0, 1)])
                    expected[k] = 'Z' + expected[k][1:]
                elif k in expected:
                    # Remove an existing key.
                    cursor.set_key(k)
                    cursor.remove()
                    del expected[k]
                else:
                    # Nothing to modify/remove; make it an insert instead.
                    v = f'v{self.ts}'
                    cursor[k] = v
                    expected[k] = v
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts))
                if verify_every and (i + 1) % verify_every == 0:
                    self.assertEqual(self.read_kvs_at(uri, self.ts), dict(expected),
                        f'table does not match expected contents: seed={seed} op={i + 1} ts={self.ts}')
                    check_point_reads(self.ts)

        # Phase 1: pre-arm churn, everything routed to stable.
        run_ops(120, verify_every=40)
        self.assertEqual(self.read_kvs_at(uri, self.ts), dict(expected))
        snapshot_ts = self.ts
        snapshot = dict(expected)

        # Arm at the current frontier (the last committed timestamp), so every post-arm commit is
        # strictly above the cutoff. Phase 2: post-arm churn routed to ingest.
        self.arm(self.ts)
        run_ops(120, verify_every=40)

        # The merged view reflects every operation across both constituents.
        self.assertEqual(self.read_kvs_at(uri, self.ts), dict(expected),
            'merged layered view must match the expected contents after the arm')
        check_point_reads(self.ts)

        # Time-travel: the view at the arm boundary is unchanged by the later ingest writes.
        self.assertEqual(self.read_kvs_at(uri, snapshot_ts), snapshot,
            'reading at the pre-arm frontier must be unaffected by post-arm writes')

        # Ground truth: the post-arm churn never touched the stable table.
        self.assertEqual(self.read_kvs_at(self.stable_uri(uri), self.ts), snapshot,
            'post-arm writes must not leak into the stable table')

        cursor.close()
