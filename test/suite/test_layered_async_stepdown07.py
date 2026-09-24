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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from helper_layered_stepdown import LayeredStepdownMixin
from wtscenario import make_scenarios

# test_layered_async_stepdown07.py
#    Supplementary coverage: cursor lifecycle across the step-down, search_near and largest_key
#    corners, the final checkpoint, and write conflicts on the demoted node between ingest and the
#    frozen stable table. The write-conflict cases have their own class at the end of the file.
@disagg_test_class
class test_layered_async_stepdown07(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = \
        'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),precise_checkpoint=true,'
    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    test_name = __qualname__

    uri = f'layered:{test_name}'

    # A cursor closed before the step-down and one opened after it in the same transaction serve
    # the same view.
    def test_cursor_close_reopen_within_txn_across_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's'}, 10)

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        c1 = self.session.open_cursor(self.uri, None, None)
        self.assertEqual(c1['b'], 's')
        c1.close()

        self.checkpoint_at(20)
        wsession = self.conn.open_session()
        self.write_at(self.uri, {'x': 'w'}, 30, wsession)
        wsession.close()
        self.demote()

        c2 = self.session.open_cursor(self.uri, None, None)
        seen = {}
        while c2.next() == 0:
            seen[c2.get_key()] = c2.get_value()
        self.assertEqual(seen, {'b': 's', 'd': 's'})
        self.session.commit_transaction()
        c2.close()

    # One cursor handle works for transactions on both sides of the step-down.
    def test_same_cursor_handle_across_step_down_txns(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's', 'f': 's'}, 10)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        self.assertEqual(cursor.next(), 0)
        self.assertEqual(cursor.get_key(), 'b')
        self.session.rollback_transaction()

        self.checkpoint_at(20)
        self.write_at(self.uri, {'a': 'w', 'z': 'w'}, 30)
        self.demote()

        # Reset at the transaction end, the handle walks the merged view from the start.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        seen = []
        while cursor.next() == 0:
            seen.append(cursor.get_key())
        self.assertEqual(seen, ['a', 'b', 'd', 'f', 'z'])
        cursor.set_key('d')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 's')
        self.session.rollback_transaction()
        cursor.close()

    # Duplicating a layered cursor is unsupported, on either side of the step-down.
    def test_dup_positioned_cursor_across_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's', 'f': 's'}, 10)
        self.checkpoint_at(20)

        c1 = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        self.assertEqual(c1.next(), 0)
        self.assertEqual(c1.get_key(), 'b')

        with self.expectedStderrPattern('unsupported object operation'):
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.open_cursor(None, c1, None))

        self.demote()

        with self.expectedStderrPattern('unsupported object operation'):
            self.assertRaisesException(wiredtiger.WiredTigerError,
                lambda: self.session.open_cursor(None, c1, None))

        # The original cursor survives the rejected duplication.
        self.assertEqual(c1.get_key(), 'b')
        self.assertEqual(c1.next(), 0)
        self.assertEqual(c1.get_key(), 'd')

        self.session.rollback_transaction()
        c1.close()

    # Visibility flips at exactly the commit timestamps around the final checkpoint after the
    # demotion.
    def test_boundary_reads_at_checkpoint(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.write_at(self.uri, {'below': 'v'}, 19)
        self.write_at(self.uri, {'at': 'v'}, 20)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'above': 'v'}, 21)
        self.demote()

        self.assertEqual(self.read_kvs_at(self.uri, 19), {'below': 'v'})
        self.assertEqual(self.read_kvs_at(self.uri, 20), {'below': 'v', 'at': 'v'})
        self.assertEqual(self.read_kvs_at(self.uri, 21), {'below': 'v', 'at': 'v', 'above': 'v'})

        self.assertEqual(self.read_keys_at(self.stable_checkpoint_uri(self.uri), 30),
            {'below', 'at'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 30), set())

    # search_near on an exact match reports equality, whichever layer holds the key, and a read
    # timestamp below the window narrows it to the checkpointed half.
    def test_search_near_exact_and_read_ts(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's', 'f': 's'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'a': 'w', 'c': 'w'}, 30)
        self.demote()
        self.write_at(self.uri, {'e': 'i'}, 35)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        for key, value in (('d', 's'), ('c', 'w'), ('e', 'i')):
            cursor.set_key(key)
            self.assertEqual(cursor.search_near(), 0, f'exact match expected for {key}')
            self.assertEqual(cursor.get_key(), key)
            self.assertEqual(cursor.get_value(), value)
        self.session.rollback_transaction()

        # Below the window only the checkpointed half is visible, so a key written later is no
        # longer an exact match and search_near falls to a neighbor.
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))
        cursor.set_key('d')
        self.assertEqual(cursor.search_near(), 0)
        for key in ('c', 'e'):
            cursor.set_key(key)
            self.assertIn(cursor.search_near(), (-1, 1))
            self.assertIn(cursor.get_key(), ('b', 'd', 'f'))
        self.session.rollback_transaction()
        cursor.close()

    # search_near works when only the frozen window has content, over an empty checkpoint, and when
    # nothing has content anywhere.
    def test_search_near_with_empty_constituent(self):
        empty_uri = f'layered:{self.test_name}_empty'
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.session.create(empty_uri, 'key_format=S,value_format=S')
        self.checkpoint_at(20)
        self.write_at(self.uri, {'b': 'w', 'd': 'w'}, 30)
        self.demote()
        self.assertEqual(self.read_keys_at(self.stable_checkpoint_uri(self.uri), 40), set())

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        for key, expected in (('a', 'b'), ('c', ('b', 'd')), ('z', 'd')):
            cursor.set_key(key)
            self.assertNotEqual(cursor.search_near(), wiredtiger.WT_NOTFOUND,
                f'search_near must find a neighbor in the frozen window for {key}')
            if isinstance(expected, tuple):
                self.assertIn(cursor.get_key(), expected)
            else:
                self.assertEqual(cursor.get_key(), expected)
        self.session.rollback_transaction()
        cursor.close()

        cursor = self.session.open_cursor(empty_uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(40))
        cursor.set_key('a')
        self.assertEqual(cursor.search_near(), wiredtiger.WT_NOTFOUND)
        self.session.rollback_transaction()
        cursor.close()

    # largest_key ignores visibility, so on the demoted node it reports a key from a follower
    # transaction that has not committed.
    def test_largest_key_with_uncommitted_write(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'d': 'w'}, 30)
        self.demote()

        def largest():
            c = self.session.open_cursor(self.uri, None, None)
            self.assertEqual(c.largest_key(), 0)
            key = c.get_key()
            c.close()
            return key

        self.assertEqual(largest(), 'd')

        # A second session inserts a new maximum into ingest and holds the transaction open.
        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(self.uri, None, None)
        wsession.begin_transaction()
        wcur['zz'] = 'uncommitted'

        self.assertEqual(largest(), 'zz',
            'largest_key must report the uncommitted key it cannot read')

        # The aborted update stays in the tree until reconciliation discards it, and largest_key
        # consults no visibility state, so the abandoned key may still be the reported maximum.
        wsession.rollback_transaction()
        wcur.close()
        wsession.close()
        self.assertIn(largest(), ('d', 'zz'))

    # A timestamped reverse walk positioned before the demotion continues across it the same way a
    # forward walk does.
    def test_reverse_iteration_across_step_down(self):
        uri = f'layered:{self.test_name}_reviter'
        self.set_global_ts(1, 1)
        self.session.create(uri, 'key_format=S,value_format=S')

        stable_keys = [f'k{i:02d}' for i in range(0, 20, 2)]
        self.write_at(uri, {k: 'v' for k in stable_keys}, 10)

        wsession = self.conn.open_session()
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))

        seen = []
        for _ in range(4):
            self.assertEqual(cursor.prev(), 0)
            seen.append(cursor.get_key())

        # Above the final checkpoint, interleave keys both behind and ahead of the scan position,
        # and update and remove keys the backward walk has not reached yet.
        self.checkpoint_at(50)
        updated = stable_keys[3]
        removed = stable_keys[1]
        wcur = wsession.open_cursor(uri, None, None)
        wsession.begin_transaction()
        for i in range(1, 20, 2):
            wcur[f'k{i:02d}'] = 'window'
        wcur[updated] = 'window-update'
        wcur.set_key(removed)
        self.assertEqual(wcur.remove(), 0)
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(60))
        wcur.close()
        wsession.close()
        self.demote()

        kvs = []
        while cursor.prev() == 0:
            seen.append(cursor.get_key())
            kvs.append((cursor.get_key(), cursor.get_value()))
        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(seen, list(reversed(stable_keys)),
            'the reverse walk must yield exactly the snapshot keys once, in order')
        self.assertIn((updated, 'v'), kvs, 'the invisible update must not reach this snapshot')
        self.assertIn((removed, 'v'), kvs, 'the invisible tombstone must not reach this snapshot')

    # A layered tree never opens by checkpoint, before or after the demotion. Reading the final
    # checkpoint means opening the stable constituent's checkpoint view, which holds only what was
    # committed at or below it.
    def test_checkpoint_cursor_after_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'a': 'w', 'z': 'w'}, 30)

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(self.uri, None, 'checkpoint=WiredTigerCheckpoint'),
            '/do not support opening by checkpoint/')

        self.demote()

        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.open_cursor(self.uri, None, 'checkpoint=WiredTigerCheckpoint'),
            '/do not support opening by checkpoint/')

        seen = self.read_kvs_at(self.stable_checkpoint_uri(self.uri), 20)
        self.assertEqual(seen, {'b': 's', 'd': 's'},
            'the final checkpoint must hold exactly the content at or below it')

# Write-conflict detection on the demoted node, where follower writes land in ingest over the frozen
# stable table, plus an extra checkpoint at the pinned stable timestamp.
@disagg_test_class
class test_layered_async_stepdown07_write_conflicts(LayeredStepdownMixin,
                                                   wttest.WiredTigerTestCase):
    conn_base_config = \
        'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),precise_checkpoint=true,'
    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    test_name = __qualname__

    uri = f'layered:{test_name}'

    # Checkpoint k1, write k2 above the checkpoint, and demote.
    def setup_demoted(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'base'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'k2': 'window'}, 25)
        self.demote()

    # Two follower writers collide on keys the frozen tree holds, on either side of the checkpoint.
    def test_conflict_on_frozen_keys(self):
        self.setup_demoted()

        for key in ('k1', 'k2'):
            cursor = self.session.open_cursor(self.uri, None, None)
            self.session.begin_transaction()
            cursor[key] = 'first'

            wsession = self.conn.open_session()
            wcur = wsession.open_cursor(self.uri, None, None)
            wsession.begin_transaction()
            wcur.set_key(key)
            wcur.set_value('second')
            self.expect_conflict_rollback(wcur.update, wsession)
            wsession.rollback_transaction()
            wcur.close()
            wsession.close()

            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
            cursor.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'first', 'k2': 'first'})

    # An uncommitted ingest write conflicts with another follower writer.
    def test_conflict_after_demotion(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'stable'}, 10)
        self.complete_step_down(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'held'

        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(self.uri, None, None)
        wsession.begin_transaction()
        wcur.set_key('k1')
        wcur.set_value('follower')
        self.expect_conflict_rollback(wcur.update, wsession)
        wsession.rollback_transaction()
        wcur.close()
        wsession.close()

        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'held'})

    # A follower writer reading below a frozen update must collide with it: the update it would
    # overwrite is one it cannot see.
    def test_conflict_read_ts_below_frozen_update(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'old'}, 10)
        self.checkpoint_at(12)
        self.write_at(self.uri, {'k1': 'newer'}, 15)
        self.demote()

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(12))
        self.assertEqual(cursor['k1'], 'old')
        cursor.set_key('k1')
        cursor.set_value('doomed')
        self.expect_conflict_rollback(cursor.update)
        self.session.rollback_transaction()

        # The rejected write left everything alone.
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'newer'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), set())

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(12))
        cursor['other'] = 'fine'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'newer', 'other': 'fine'})
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), {'other'})

    # A second checkpoint at the pinned stable timestamp while writes continue above it changes
    # nothing for readers, and the step-down still completes.
    def test_extra_checkpoint_at_pinned_stable(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'a': 'w', 'z': 'w'}, 40)

        before = self.read_kvs_at(self.uri, 50)
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()

        # The checkpoint holds only the content committed at or below the stable timestamp.
        ckpt_cursor = self.session.open_cursor(self.stable_uri(self.uri), None,
            'checkpoint=WiredTigerCheckpoint')
        checkpointed = {}
        while ckpt_cursor.next() == 0:
            checkpointed[ckpt_cursor.get_key()] = ckpt_cursor.get_value()
        ckpt_cursor.close()
        self.assertEqual(checkpointed, {'b': 's', 'd': 's'})
        self.assertEqual(self.read_kvs_at(self.uri, 50), before)

        self.write_at(self.uri, {'y': 'w'}, 45)
        self.demote()
        self.assertEqual(self.read_kvs_at(self.uri, 50),
            {'b': 's', 'd': 's', 'a': 'w', 'z': 'w', 'y': 'w'})

    # Two follower writers collide trying to modify the same frozen key.
    def test_modify_conflict_leaves_ingest_alone(self):
        self.setup_demoted()

        # Writer A holds an uncommitted modify of k2.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor.set_key('k2')
        self.assertEqual(cursor.modify([wiredtiger.Modify('X', 0, 1)]), 0)

        # Writer B's modify of the same key conflicts and is discarded. B wrote nothing else, so the
        # transaction still commits (empty).
        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(self.uri, None, None)
        wsession.begin_transaction()
        wcur.set_key('k2')
        self.expect_conflict_rollback(lambda: wcur.modify([wiredtiger.Modify('Z', 0, 1)]),
            wsession)
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        wcur.close()
        wsession.close()

        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        # A's modified value survives.
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'base', 'k2': 'Xindow'})
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 40), {'k2': 'Xindow'})

    # A follower writer that writes a key cleanly, then conflicts on another, cannot commit: the
    # conflict drops that write and the transaction must roll back, losing the clean write too.
    def test_conflict_then_cannot_commit(self):
        self.setup_demoted()

        # Writer A holds an uncommitted write of k1.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'a'

        # Writer B writes k3 cleanly, then conflicts with A on k1.
        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(self.uri, None, None)
        wsession.begin_transaction()
        wcur['k3'] = 'b'
        wcur.set_key('k1')
        wcur.set_value('b')
        self.expect_conflict_rollback(wcur.update, wsession)

        # B wrote k3 before the conflict, so the transaction has work to commit; the engine still
        # refuses, so B must roll back and k3 is lost with the rest of the transaction.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(30)),
            '/transaction requires rollback/')
        wsession.close()

        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'a', 'k2': 'window'})
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 40), {'k1': 'a'})

    # Two follower writers collide on a remove of the same frozen key.
    def test_remove_conflict_on_frozen_key(self):
        self.setup_demoted()

        # Writer A holds an uncommitted remove of k2.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor.set_key('k2')
        self.assertEqual(cursor.remove(), 0)

        # Writer B's remove of the same key conflicts; it wrote nothing else, so its transaction
        # still commits (empty).
        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(self.uri, None, None)
        wsession.begin_transaction()
        wcur.set_key('k2')
        self.expect_conflict_rollback(wcur.remove, wsession)
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        wcur.close()
        wsession.close()

        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        cursor.close()

        # A's remove alone took effect, as a tombstone in ingest.
        self.assertEqual(self.read_kvs_at(self.uri, 40), {'k1': 'base'})
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 40), {'k2': '\x14'})

    # A remove leaves ingest holding the bare tombstone marker over the frozen value, so re-inserting
    # the key has to succeed: a plain insert, and one with overwrite=false that must not report a
    # duplicate.
    def test_reinsert_after_remove(self):
        self.setup_demoted()
        self.remove_at(self.uri, ['k1', 'k2'], 30)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor.set_key('k1')
        cursor.set_value('again')
        self.assertEqual(cursor.insert(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(31))
        cursor.close()

        cursor = self.session.open_cursor(self.uri, None, 'overwrite=false')
        self.session.begin_transaction()
        cursor.set_key('k2')
        cursor.set_value('again')
        self.assertEqual(cursor.insert(), 0)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(32))
        cursor.close()

        expected = {'k1': 'again', 'k2': 'again'}
        self.assertEqual(self.read_kvs_at(self.uri, 40), expected)
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 40), expected)
        self.assertEqual(self.read_kvs_at(self.uri, 28), {'k1': 'base', 'k2': 'window'})
