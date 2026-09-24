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

# test_layered_async_stepdown06.py
#    Completing the step-down: data survival above and below the final checkpoint, follower
#    behavior, handles held open across the demotion, and the step-up leg that proves the node is
#    reusable.
@disagg_test_class
class test_layered_async_stepdown06(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = \
        'statistics=(all),statistics_log=(wait=1,json=true,on_close=true),precise_checkpoint=true,'
    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    test_name = __qualname__

    uri = f'layered:{test_name}'

    # Every commit mix survives the step-down: below the final checkpoint only, above it only, and
    # both; and a follower write afterwards routes to ingest.
    def test_data_survives_step_down_all_mixes(self):
        t_pre = f'layered:{self.test_name}_mix_pre'
        t_post = f'layered:{self.test_name}_mix_post'
        t_both = f'layered:{self.test_name}_mix_both'
        self.set_global_ts(1, 1)
        for uri in (t_pre, t_post, t_both):
            self.session.create(uri, 'key_format=S,value_format=S')

        self.write_at(t_pre, {'a': 'stable'}, 10)
        self.write_at(t_both, {'a': 'stable'}, 10)
        self.checkpoint_at(20)
        self.write_at(t_post, {'b': 'window'}, 30)
        self.write_at(t_both, {'b': 'window'}, 30)

        self.demote()

        self.assertEqual(self.read_kvs_at(t_pre, 40), {'a': 'stable'})
        self.assertEqual(self.read_kvs_at(t_post, 40), {'b': 'window'})
        self.assertEqual(self.read_kvs_at(t_both, 40), {'a': 'stable', 'b': 'window'})
        self.assertEqual(self.read_kvs(t_both), {'a': 'stable', 'b': 'window'})

        # Nothing was moved into ingest: the window commits are served from the frozen tree, and
        # the checkpoint holds only what was below it.
        for uri in (t_pre, t_post, t_both):
            self.assertEqual(self.read_keys_at(self.ingest_uri(uri), 40), set())
        self.assertEqual(self.read_keys_at(self.stable_checkpoint_uri(t_post), 40), set())
        self.assertEqual(self.read_keys_at(self.stable_checkpoint_uri(t_both), 40), {'a'})

        # A follower write commits fine and routes to ingest.
        self.write_at(t_both, {'c': 'follower'}, 50)
        self.assertEqual(self.read_keys_at(self.ingest_uri(t_both), 60), {'c'})
        self.assertEqual(self.read_keys_at(self.stable_checkpoint_uri(t_both), 60), {'a'})
        self.assertEqual(self.read_kvs_at(t_both, 60),
            {'a': 'stable', 'b': 'window', 'c': 'follower'})

    # A restart without local files serves exactly the final checkpoint: the content committed at
    # or below it survives, and the frozen commits above it are gone with the process.
    def test_step_down_checkpoint_survives_restart(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        pre = {'pre' + str(i) for i in range(5)}
        self.write_at(self.uri, {k: 'stable' for k in pre}, 10)
        self.checkpoint_at(20)
        post = {'post' + str(i) for i in range(5)}
        self.write_at(self.uri, {k: 'window' for k in post}, 30)

        self.demote()
        self.assertEqual(self.read_keys_at(self.uri, 40), pre | post,
            'the window content must survive the step-down')

        self.restart_without_local_files(
            config=self.conn_base_config + 'disaggregated=(role="follower")')

        self.assertEqual(self.read_kvs_at(self.uri, 40), {k: 'stable' for k in pre},
            'the restarted node must serve exactly the checkpointed content')
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 40), set())

    # next_random keeps sampling correctly through every step-down phase.
    def test_next_random_across_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        keys = {f'k{i:02d}' for i in range(10)}
        self.write_at(self.uri, {k: 's' for k in keys}, 10)

        cursor = self.session.open_cursor(self.uri, None, 'next_random=true')
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(15))

        def sample():
            self.assertEqual(cursor.next(), 0)
            self.assertIn(cursor.get_key(), keys)

        sample()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        sample()
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        sample()
        self.demote()
        sample()

        self.session.rollback_transaction()
        cursor.close()

    # Shared body for the reader-held-open tests: an open read transaction iterates the merged
    # view while each step-down phase completes in turn.
    def reader_across_step_down(self, begin_config):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's', 'f': 's'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'a': 'i', 'c': 'i', 'e': 'i', 'z': 'i'}, 30)

        rcur = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction(begin_config)

        # Walk part way through the merged view.
        self.assertEqual(rcur.next(), 0)
        self.assertEqual(rcur.get_key(), 'a')
        self.assertEqual(rcur.next(), 0)
        self.assertEqual(rcur.get_key(), 'b')

        self.assertEqual(rcur.next(), 0)
        self.assertEqual(rcur.get_key(), 'c')

        # The cursor stays positioned across another checkpoint at the pinned stable timestamp.
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        self.assertEqual(rcur.next(), 0)
        self.assertEqual(rcur.get_key(), 'd')

        # The cursor stays positioned across the demotion and the walk continues in place, with
        # no duplicates or gaps. An untimestamped snapshot spanning the demotion may instead be
        # refused at its next stable bind: the role change swaps what the stable content is, so no
        # binding is consistent for it. A timestamped reader must never be refused.
        self.demote()
        try:
            seen = []
            while rcur.next() == 0:
                seen.append(rcur.get_key())
            self.assertEqual(seen, ['e', 'f', 'z'],
                'the walk must continue correctly across the completed step-down')

            # Point reads keep answering from both constituents; a miss is still a miss.
            self.assertEqual(rcur['d'], 's')
            self.assertEqual(rcur['e'], 'i')
            rcur.set_key('x')
            self.assertEqual(rcur.search(), wiredtiger.WT_NOTFOUND)

            # search_near on the follower positions on an adjacent key from the merged view.
            rcur.set_key('x')
            self.assertNotEqual(rcur.search_near(), wiredtiger.WT_NOTFOUND)
            self.assertIn(rcur.get_key(), ('f', 'z'))

            # A reverse walk in the same held transaction yields the full merged view.
            rcur.reset()
            seen = []
            while rcur.prev() == 0:
                seen.append(rcur.get_key())
            self.assertEqual(seen, ['z', 'f', 'e', 'd', 'c', 'b', 'a'])
        except wiredtiger.WiredTigerError as e:
            if begin_config is not None or 'WT_ROLLBACK' not in str(e):
                raise
            self.ignoreStderrPatternIfExists('WT_ROLLBACK')

        self.session.rollback_transaction()
        rcur.close()

    # A reader with a read timestamp held open across the completed step-down stays correct.
    def test_reader_with_read_ts_survives_step_down(self):
        self.reader_across_step_down('read_timestamp=' + self.timestamp_str(40))

    # A reader without a read timestamp held open across the completed step-down stays correct.
    def test_reader_without_read_ts_survives_step_down(self):
        self.reader_across_step_down(None)

    # Shared body: a snapshot taken before the final checkpoint is held through the whole
    # step-down, and the later writes must stay invisible at every phase.
    def reader_from_before_final_checkpoint(self, begin_config):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'b': 's', 'd': 's', 'f': 's', 'h': 's', 'j': 's'}, 10)

        rcur = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction(begin_config)
        self.assertEqual(rcur.next(), 0)
        self.assertEqual(rcur.get_key(), 'b')

        # The cursor stays positioned across the final checkpoint.
        self.checkpoint_at(20)
        self.assertEqual(rcur.next(), 0)
        self.assertEqual(rcur.get_key(), 'd')

        # A concurrent later transaction interleaves keys, invisible to this snapshot.
        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(self.uri, None, None)
        wsession.begin_transaction()
        for k in ('a', 'c', 'e', 'z'):
            wcur[k] = 'i'
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(30))
        wcur.close()
        wsession.close()
        self.assertEqual(rcur.next(), 0)
        self.assertEqual(rcur.get_key(), 'f')

        # The cursor stays positioned across another checkpoint at the pinned stable timestamp.
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        self.assertEqual(rcur.next(), 0)
        self.assertEqual(rcur.get_key(), 'h')

        # The cursor stays positioned across the demotion and the walk finishes in place: still
        # only the snapshot's keys, in order. An untimestamped snapshot spanning the demotion may
        # instead be refused at its next stable bind: the role change swaps what the stable
        # content is, so no binding is consistent for it. A timestamped reader must never be
        # refused.
        self.demote()
        try:
            seen = []
            while rcur.next() == 0:
                seen.append(rcur.get_key())
            self.assertEqual(seen, ['j'],
                'the snapshot taken beforehand must yield exactly its own keys across the '
                'step-down')

            # The later keys stay invisible to point reads on the follower.
            rcur.set_key('a')
            self.assertEqual(rcur.search(), wiredtiger.WT_NOTFOUND)
            self.assertEqual(rcur['d'], 's')

            # A reverse walk still yields only the snapshot's keys.
            rcur.reset()
            seen = []
            while rcur.prev() == 0:
                seen.append(rcur.get_key())
            self.assertEqual(seen, ['j', 'h', 'f', 'd', 'b'])
        except wiredtiger.WiredTigerError as e:
            if begin_config is not None or 'WT_ROLLBACK' not in str(e):
                raise
            self.ignoreStderrPatternIfExists('WT_ROLLBACK')

        self.session.rollback_transaction()
        rcur.close()

    # A reader with a read timestamp holds its view through the whole step-down.
    def test_reader_from_before_final_checkpoint_with_read_ts(self):
        self.reader_from_before_final_checkpoint('read_timestamp=' + self.timestamp_str(15))

    # A reader gated only by its snapshot holds its view through the whole step-down.
    def test_reader_from_before_final_checkpoint_without_read_ts(self):
        self.reader_from_before_final_checkpoint(None)

    # Shared body for the repeatable-read tests: a snapshot reader spans the completed step-down;
    # a concurrent commit the snapshot excluded must stay invisible afterwards.
    def reader_repeatable_across_step_down(self, begin_config):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'old'}, 10)

        # The reader takes its snapshot before a concurrent commit.
        rcur = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction(begin_config)
        self.assertEqual(rcur['k1'], 'old')

        # A concurrent commit lands in stable, so the final checkpoint includes content the reader's
        # snapshot excludes.
        wsession = self.conn.open_session()
        wcur = wsession.open_cursor(self.uri, None, None)
        wsession.begin_transaction()
        wcur['k1'] = 'new'
        wsession.commit_transaction('commit_timestamp=' + self.timestamp_str(15))
        wcur.close()
        wsession.close()

        self.assertEqual(rcur['k1'], 'old', 'the snapshot must exclude the concurrent commit')
        rcur.reset()

        self.complete_step_down(20)

        # Both a fresh cursor and the original one must still answer from the snapshot. An
        # untimestamped snapshot spanning the role change may instead be refused at its next
        # stable bind: the role change swaps what the stable content is, so no binding is
        # consistent for it.
        rollback_ok = begin_config is None
        try:
            rcur2 = self.session.open_cursor(self.uri, None, None)
            self.assertEqual(rcur2['k1'], 'old',
                'a fresh cursor after the step-down must still read the snapshot value')
            self.assertEqual(rcur['k1'], 'old',
                'the original cursor after the step-down must still read the snapshot value')
            rcur2.close()
        except wiredtiger.WiredTigerError as e:
            if not (rollback_ok and 'WT_ROLLBACK' in str(e)):
                raise
        self.session.rollback_transaction()
        rcur.close()

    # Without a read timestamp the snapshot is the only visibility gate; the completed
    # step-down must not break repeatable read.
    def test_repeatable_read_no_read_ts_across_step_down(self):
        self.reader_repeatable_across_step_down(None)

    # With a read timestamp the timestamp gate must protect the reader across the step-down.
    def test_repeatable_read_with_read_ts_across_step_down(self):
        self.reader_repeatable_across_step_down(
            'read_timestamp=' + self.timestamp_str(12))

    # Shared body for the follower-pickup repeatable-read tests: a snapshot reader on a second
    # node spans the pickup of the leader's step-down checkpoint; content the reader never saw
    # must stay invisible.
    def follower_reader_across_pickup(self, begin_config):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'k1': 'old'}, 10)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        # A second node picks up that checkpoint and opens a snapshot reader on it.
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
            self.conn_base_config + 'disaggregated=(role="follower")')
        self.disagg_advance_checkpoint(conn_follow)
        fsession = conn_follow.open_session('')
        fcur = fsession.open_cursor(self.uri, None, None)
        fsession.begin_transaction(begin_config)
        self.assertEqual(fcur['k1'], 'old')

        # The leader commits above the reader's view and completes the step-down.
        self.write_at(self.uri, {'k1': 'new'}, 15)
        self.complete_step_down(20)

        # The follower picks up the step-down checkpoint under the open snapshot; an
        # untimestamped snapshot defers the adoption, so do not wait for it.
        self.disagg_advance_checkpoint(conn_follow)

        # Both a fresh cursor and the original one must still answer from the snapshot.
        fcur2 = fsession.open_cursor(self.uri, None, None)
        self.assertEqual(fcur2['k1'], 'old',
            'a fresh cursor must not see content picked up after its snapshot')
        self.assertEqual(fcur['k1'], 'old',
            'the original cursor must not see content picked up after its snapshot')
        fsession.rollback_transaction()
        fcur2.close()
        fcur.close()
        fsession.close()
        conn_follow.close()

    # Without a read timestamp the snapshot is the only visibility gate; the id wipe at checkpoint
    # pickup must not break repeatable read on the follower.
    def test_follower_repeatable_read_no_read_ts_across_pickup(self):
        self.follower_reader_across_pickup(None)

    # With a read timestamp the timestamp gate must protect the reader across the id wipe.
    def test_follower_repeatable_read_with_read_ts_across_pickup(self):
        self.follower_reader_across_pickup('read_timestamp=' + self.timestamp_str(12))

    # A leader writer commits above the final checkpoint before the demotion, and a follower writer
    # can then continue through the same cursor into ingest.
    def test_writes_before_and_after_demotion(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.checkpoint_at(20)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction()
        cursor['k1'] = 'v'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(30))

        # The step-down requires all application write transactions to be complete.
        cursor.reset()
        self.demote()

        # A follower writer continues, routing writes to ingest.
        self.session.begin_transaction()
        cursor['k2'] = 'follower'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(40))
        cursor.close()
        self.assertEqual(self.read_keys_at(self.ingest_uri(self.uri), 50), {'k2'})
        self.assertEqual(self.read_kvs_at(self.uri, 50), {'k1': 'v', 'k2': 'follower'})

        # Neither write reached the final checkpoint.
        self.assertEqual(self.read_kvs_at(self.stable_checkpoint_uri(self.uri), 50), {})

    # Step-down, then a successor that has applied the window takes over and checkpoints; this node
    # picks that checkpoint up and steps back up. Nothing is lost at any point.
    def test_step_up_after_pickup_recovers_window_writes(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'a': 'stable'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'b': 'window'}, 30)
        self.demote()

        # A second node picks the final checkpoint up and applies the window commit, as its log
        # application would, before taking over.
        conn_b = self.open_node('follower', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_b, self.conn)
        session_b = conn_b.open_session('')
        self.write_at(self.uri, {'b': 'window'}, 30, session_b)
        self.assertEqual(self.read_kvs_at(self.stable_checkpoint_uri(self.uri, conn_b), 30,
            session_b), {'a': 'stable'})

        # Node B takes over, writes and publishes its own checkpoint.
        self.promote(conn_b)
        self.write_at(self.uri, {'c': 'from-b'}, 40, session_b)
        self.checkpoint_at(40, conn_b)

        # This node picks B's checkpoint up; it covers the frozen window, so it supersedes it.
        self.assertEqual(self.read_kvs_at(self.uri, 50), {'a': 'stable', 'b': 'window'})
        self.disagg_advance_checkpoint_and_wait(self.conn, conn_b)
        self.assertEqual(self.read_kvs_at(self.stable_checkpoint_uri(self.uri), 50),
            {'a': 'stable', 'b': 'window', 'c': 'from-b'})
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 50), {})
        self.assertEqual(self.read_kvs_at(self.uri, 50),
            {'a': 'stable', 'b': 'window', 'c': 'from-b'})

        session_b.close()
        self.demote(conn_b)
        conn_b.close()
        self.promote()

        self.assertEqual(self.read_kvs_at(self.stable_uri(self.uri), 50),
            {'a': 'stable', 'b': 'window', 'c': 'from-b'})
        self.assertEqual(self.read_kvs_at(self.uri, 50),
            {'a': 'stable', 'b': 'window', 'c': 'from-b'})
        self.checkpoint_at(50)

    # Two full step-down/step-up cycles on one node with no pickup in between: the promotion
    # un-freezes the window, drains ingest into stable, and the node is fully reusable.
    def test_step_up_drains_ingest_then_second_cycle(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Cycle 1: checkpointed content, then window content, then the demotion.
        self.write_at(self.uri, {'a': 'cycle1-stable'}, 10)
        self.checkpoint_at(20)
        self.write_at(self.uri, {'b': 'cycle1-window'}, 30)
        self.demote()

        # More ingest content arrives while a follower.
        self.write_at(self.uri, {'c': 'follower-ingest'}, 40)

        # Step up. The window is live again and the promotion drains ingest into it.
        self.promote()
        self.assertEqual(self.read_kvs_at(self.stable_uri(self.uri), 50),
            {'a': 'cycle1-stable', 'b': 'cycle1-window', 'c': 'follower-ingest'},
            'the step-up must keep the window and drain ingest into the stable table')
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 50), {})
        expected = {'a': 'cycle1-stable', 'b': 'cycle1-window', 'c': 'follower-ingest'}
        self.assertEqual(self.read_kvs_at(self.uri, 50), expected)

        # Cycle 2: a new final checkpoint, window content above it, and the demotion.
        self.checkpoint_at(60)
        self.assertEqual(self.read_kvs_at(self.stable_checkpoint_uri(self.uri), 60), expected)
        self.write_at(self.uri, {'d': 'cycle2-window'}, 70)
        self.demote()

        expected['d'] = 'cycle2-window'
        self.assertEqual(self.read_kvs_at(self.uri, 80), expected,
            'the full view must survive the second step-down')

        # A second step-up keeps the second cycle's window as well.
        self.promote()
        # With a precise checkpoint, a commit above the stable timestamp stays dirty and cannot be
        # verified: advance stable over this cycle's write and checkpoint, as cycle 1 does.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(70))
        self.session.checkpoint()
        self.assertEqual(self.read_keys_at(self.stable_uri(self.uri), 80), {'a', 'b', 'c', 'd'})
        self.assertEqual(self.read_kvs_at(self.uri, 80), expected)

        # Settle the final leader state after the behavior assertions so teardown can verify it.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(70))
        self.session.checkpoint()
