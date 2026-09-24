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
#    Read semantics across a step-down: iteration spanning the demotion, merged lookups over ingest
#    and the frozen stable table, a per-timestamp oracle and a randomized stress phase.
@disagg_test_class
class test_layered_async_stepdown02(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = \
        'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),precise_checkpoint=true,'
    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # A timestamped scan positioned before the demotion continues across it and still yields its
    # own snapshot exactly once, in order, whatever later writers do to the keys underneath it.
    def test_iteration_across_step_down(self):
        uri = f'layered:{self.test_name}_iter'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        # Keys divisible by three form the content the scan snapshot will see.
        snapshot_keys = [f'k{i:02d}' for i in range(0, 30, 3)]
        self.write_at(uri, {k: 'v' for k in snapshot_keys}, 10)

        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        seen = []
        for _ in range(4):
            self.assertEqual(cursor.next(), 0)
            seen.append(cursor.get_key())

        # Above the final checkpoint the leader interleaves keys behind and ahead of the scan
        # position into the live stable table, and updates a key the scan has not reached.
        updated = snapshot_keys[6]
        removed = snapshot_keys[8]
        self.checkpoint_at(20)
        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(uri, None, None)
        wsession.begin_transaction()
        for i in range(1, 30, 3):
            wcur[f'k{i:02d}'] = 'window'
        wcur[updated] = 'window-update'
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(30))

        self.demote()

        # After the demotion a follower writer interleaves more keys into ingest and removes a key
        # the scan has not reached.
        wsession.begin_transaction()
        for i in range(2, 30, 3):
            wcur[f'k{i:02d}'] = 'ingest'
        wcur.set_key(removed)
        self.assertEqual(wcur.remove(), 0)
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(60))
        wcur.close()
        wsession.close()

        kvs = []
        while cursor.next() == 0:
            seen.append(cursor.get_key())
            kvs.append((cursor.get_key(), cursor.get_value()))
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(seen, snapshot_keys,
            'the scan must yield exactly the snapshot keys once, in order')
        self.assertIn((updated, 'v'), kvs, 'the invisible update must not reach this snapshot')
        self.assertIn((removed, 'v'), kvs, 'the invisible tombstone must not reach this snapshot')

        # A fresh scan above both writers sees the merge.
        expected = {k: 'v' for k in snapshot_keys}
        expected.update({f'k{i:02d}': 'window' for i in range(1, 30, 3)})
        expected.update({f'k{i:02d}': 'ingest' for i in range(2, 30, 3)})
        expected[updated] = 'window-update'
        del expected[removed]
        self.assertEqual(self.read_kvs_at(uri, 70), expected)
        self.assertEqual(self.read_kvs(uri), expected)

    # Point and range lookups merge ingest over the frozen stable table.
    def test_search_and_search_near_merged(self):
        uri = f'layered:{self.test_name}_search'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        self.write_at(uri, {'b': 's', 'd': 's'}, 10)
        self.checkpoint_at(20)
        self.write_at(uri, {'f': 'w'}, 25)
        self.demote()
        self.write_at(uri, {'a': 'i', 'c': 'i', 'e': 'i'}, 30)
        self.assertEqual(self.read_keys_at(self.stable_checkpoint_uri(uri), 40), {'b', 'd'})

        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))

        # Exact search finds keys from every layer.
        self.assertEqual(cursor['c'], 'i')
        self.assertEqual(cursor['d'], 's')
        self.assertEqual(cursor['f'], 'w')

        # A miss is a miss across the merged view.
        cursor.set_key('z')
        self.assertEqual(cursor.search(), wiredtiger.WT_NOTFOUND)

        # search_near on a non-existent key positions on an adjacent key from either constituent.
        cursor.set_key('cc')
        cmp = cursor.search_near()
        self.assertNotEqual(cmp, wiredtiger.WT_NOTFOUND)
        self.assertIn(cursor.get_key(), ('c', 'd'))
        cursor.set_key('ee')
        self.assertNotEqual(cursor.search_near(), wiredtiger.WT_NOTFOUND)
        self.assertIn(cursor.get_key(), ('e', 'f'))

        # Full merged order interleaves the two constituents.
        cursor.reset()
        order = []
        while cursor.next() == 0:
            order.append(cursor.get_key())
        self.assertEqual(order, ['a', 'b', 'c', 'd', 'e', 'f'])
        self.session.rollback_transaction()
        cursor.close()

    # A follower write to ingest is visible to a later read in the same transaction.
    def test_read_your_own_writes_after_step_down(self):
        uri = f'layered:{self.test_name}_ryow'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'old': 'stable'}, 10)
        self.checkpoint_at(20)
        self.write_at(uri, {'win': 'window'}, 25)
        self.demote()

        wcur = self.session.open_cursor(uri, None, None)
        rcur = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        wcur['fresh'] = 'ingest'
        wcur['old'] = 'ingest'
        wcur['win'] = 'ingest'
        # The same transaction sees its own ingest writes merged over stable.
        self.assertEqual(rcur['fresh'], 'ingest')
        self.assertEqual(rcur['old'], 'ingest')
        self.assertEqual(rcur['win'], 'ingest')
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        wcur.close()
        rcur.close()

        self.assertEqual(self.read_kvs_at(uri, 40),
            {'old': 'ingest', 'fresh': 'ingest', 'win': 'ingest'})
        self.assertEqual(self.read_kvs_at(uri, 28), {'old': 'stable', 'win': 'window'})

    # Reverse iteration and largest_key on either side of the demotion; largest_key is
    # non-transactional.
    def test_prev_and_largest_key_across_step_down(self):
        uri = f'layered:{self.test_name}_revscan'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'b': 's', 'd': 's', 'f': 's'}, 10)

        def reverse_keys(read_ts):
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

        self.assertEqual(reverse_keys(15), ['f', 'd', 'b'])
        self.assertEqual(largest(), 'f')

        self.checkpoint_at(20)
        self.write_at(uri, {'g': 'w'}, 25)
        self.assertEqual(largest(), 'g')
        self.demote()
        self.assertEqual(largest(), 'g')

        # The merged maximum lives in ingest.
        self.write_at(uri, {'a': 'i', 'c': 'i', 'e': 'i', 'z': 'i'}, 30)
        self.assertEqual(reverse_keys(40), ['z', 'g', 'f', 'e', 'd', 'c', 'b', 'a'])
        self.assertEqual(reverse_keys(25), ['g', 'f', 'd', 'b'])
        self.assertEqual(largest(), 'z')

    # Read ops through a timestamped reader spanning the demotion: later writes stay invisible to
    # it, except to largest_key.
    def test_read_ops_across_step_down(self):
        uri = f'layered:{self.test_name}_readops'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')
        self.write_at(uri, {'b': 's', 'd': 's', 'f': 's'}, 10)

        rcur = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        self.assertEqual(rcur['d'], 's')

        wsession = self.conn.open_session()
        self.checkpoint_at(20)
        self.write_at(uri, {'g': 'w'}, 25, wsession)
        self.demote()

        # A follower writer interleaves ingest keys, including a new maximum.
        wcur = wsession.open_cursor(uri, None, None)
        wsession.begin_transaction()
        for k in ('a', 'c', 'e', 'z'):
            wcur[k] = 'i'
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        wcur.close()
        wsession.close()

        # search: a stable hit still works, the invisible keys are misses.
        self.assertEqual(rcur['d'], 's')
        for k in ('a', 'g'):
            rcur.set_key(k)
            self.assertEqual(rcur.search(), wiredtiger.WT_NOTFOUND)

        # search_near lands on a visible stable neighbor, never the invisible ingest 'c'.
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

    # Check every read op against a per-timestamp oracle, with tombstones and re-inserts spread over
    # the checkpointed stable content, the frozen window and ingest.
    def test_oracle_reads_merges(self):
        uri = f'layered:{self.test_name}_oracle'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        universe = {'gone', 'reborn', 'upd', 'keep', 'new', 'late'}

        # Below the checkpoint: four keys at 10, then 'reborn' is deleted at 12.
        self.write_at(uri, {'gone': 's', 'reborn': 's', 'upd': 's', 'keep': 's'}, 10)
        self.remove_at(uri, ['reborn'], 12)
        self.checkpoint_at(20)

        # Above the checkpoint, frozen at demotion: a tombstone over 'gone' and a re-insert of the
        # key deleted below the checkpoint.
        self.remove_at(uri, ['gone'], 30)
        self.write_at(uri, {'reborn': 'w'}, 35)

        oracle = {
            10: {'gone': 's', 'reborn': 's', 'upd': 's', 'keep': 's'},
            12: {'gone': 's', 'upd': 's', 'keep': 's'},
            25: {'gone': 's', 'upd': 's', 'keep': 's'},
            30: {'upd': 's', 'keep': 's'},
            35: {'reborn': 'w', 'upd': 's', 'keep': 's'},
        }

        # Verify every read op against the oracle at every timestamp: full forward scan, point
        # reads over the whole key universe, and a reverse scan.
        def check_oracle(phase):
            for ts, expected in oracle.items():
                ctx = f'{phase} read_ts={ts}'
                self.assertEqual(self.read_kvs_at(uri, ts), expected, f'scan mismatch: {ctx}')

                rc = self.session.open_cursor(uri, None, None)
                self.session.begin_transaction('read_timestamp=' + self.timestamp_str(ts))
                for k in sorted(universe):
                    rc.set_key(k)
                    if k in expected:
                        self.assertEqual(rc.search(), 0, f'expected hit: {ctx} key={k}')
                        self.assertEqual(rc.get_value(), expected[k],
                            f'value mismatch: {ctx} key={k}')
                    else:
                        self.assertEqual(rc.search(), wiredtiger.WT_NOTFOUND,
                            f'expected miss: {ctx} key={k}')
                rc.reset()
                rev = []
                while rc.prev() == 0:
                    rev.append(rc.get_key())
                self.assertEqual(rev, sorted(expected.keys(), reverse=True),
                    f'reverse scan mismatch: {ctx}')
                self.session.rollback_transaction()
                rc.close()

        check_oracle('leader')
        self.demote()
        check_oracle('demoted')

        # Follower writes in ingest: an overwrite of a checkpointed value, a key that never existed,
        # and a tombstone over a key only the frozen tree holds, re-inserted later.
        self.write_at(uri, {'upd': 'i2'}, 40)
        self.write_at(uri, {'new': 'i'}, 45)
        self.remove_at(uri, ['reborn'], 47)
        self.write_at(uri, {'late': 'i', 'reborn': 'i'}, 49)
        oracle.update({
            40: {'reborn': 'w', 'upd': 'i2', 'keep': 's'},
            45: {'reborn': 'w', 'upd': 'i2', 'keep': 's', 'new': 'i'},
            47: {'upd': 'i2', 'keep': 's', 'new': 'i'},
            49: {'reborn': 'i', 'upd': 'i2', 'keep': 's', 'new': 'i', 'late': 'i'},
        })
        check_oracle('follower')

        self.assertEqual(self.read_kvs_at(self.stable_checkpoint_uri(uri), 50),
            {'gone': 's', 'upd': 's', 'keep': 's'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(uri), 50),
            {'upd', 'new', 'reborn', 'late'})

    # Randomized ops on the leader either side of the final checkpoint, then as a follower, with
    # the merged view checked against a shadow map.
    #
    # FIXME-WT-18209: extend the layered cursor stress test to cover async step-down and retire this.
    def test_stress_random_ops(self):
        uri = f'layered:{self.test_name}_stress'
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
                    self.assertEqual(cursor.remove(), 0)
                    del expected[k]
                else:
                    # Nothing to modify/remove; make it an insert instead.
                    v = f'v{self.ts}'
                    cursor[k] = v
                    expected[k] = v
                self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(self.ts))
                if verify_every and (i + 1) % verify_every == 0:
                    self.assertEqual(self.read_kvs_at(uri, self.ts), dict(expected),
                        f'table does not match expected: seed={seed} op={i + 1} ts={self.ts}')
                    check_point_reads(self.ts)

        # Phase 1: leader churn up to the final checkpoint.
        run_ops(120, verify_every=40)
        checkpoint_ts = self.ts
        checkpoint_view = dict(expected)
        self.checkpoint_at(checkpoint_ts)

        # Phase 2: leader churn above the checkpoint, frozen by the demotion.
        run_ops(60, verify_every=20)
        demote_ts = self.ts
        demote_view = dict(expected)
        cursor.close()
        self.demote()
        self.assertEqual(self.read_kvs_at(uri, demote_ts), demote_view,
            'the demoted node must serve every leader commit')
        self.assertEqual(self.read_kvs_at(self.stable_checkpoint_uri(uri), checkpoint_ts),
            checkpoint_view)

        # Phase 3: follower churn into ingest.
        cursor = self.session.open_cursor(uri, None, None)
        run_ops(120, verify_every=40)
        cursor.close()

        # The merged view reflects every operation across the layers.
        self.assertEqual(self.read_kvs_at(uri, self.ts), dict(expected),
            'the merged view must match the expected contents')
        self.assertEqual(self.read_kvs(uri), dict(expected))
        check_point_reads(self.ts)

        # Time travel: the views at the checkpoint and at the demotion are unchanged by later
        # writes.
        self.assertEqual(self.read_kvs_at(uri, checkpoint_ts), checkpoint_view)
        self.assertEqual(self.read_kvs_at(uri, demote_ts), demote_view)
