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

# A walk blocked on the stable constituent's own prepared conflict survives into a role change:
# the reader is left mid-walk (not reset) while the leader steps down to follower. Only write
# cursors are required to be unpositioned across a step-down. The walk keeps its blocked position
# and waits on the prepared update across the step-down rather than restarting; once the update is
# resolved it resumes where it stopped, returning every key exactly once.
@disagg_test_class
class test_layered_async_stepdown17(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,' \
        'debug_mode=(disagg_stepdown_prepare=true),'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = disagg_storages

    uri = 'layered:test_layered_async_stepdown17'

    nrows = 40
    blocked_index = nrows // 2

    def keys(self):
        return [f'key{i:04d}' for i in range(self.nrows)]

    # Interleave so the walk toggles between constituents, keeping ingest genuinely positioned
    # once the walk reaches (and blocks on) the stable-side prepared key.
    def stable_keys(self):
        return [k for i, k in enumerate(self.keys()) if i % 2 == 0]

    def ingest_keys(self):
        return [k for i, k in enumerate(self.keys()) if i % 2 == 1]

    def write_at(self, uri, items, commit_ts):
        cursor = self.session.open_cursor(uri, None, None)
        self.session.begin_transaction()
        for k, v in items.items():
            cursor[k] = v
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def test_role_change_while_stable_blocked(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')

        keys = self.keys()
        blocked_key = keys[self.blocked_index]

        # Stable content, prepared conflict lands here (even index -> stable key).
        self.write_at(self.uri, {k: 'v-initial' for k in self.stable_keys()}, 10)
        self.set_global_ts(1, 10)

        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri, None, None)
        prepare_session.begin_transaction()
        prepare_cursor[blocked_key] = 'v-prepared'
        prepare_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(12))

        # Announce the step-down, then give ingest its own genuinely positioned content.
        self.set_step_down_ts(15)
        self.write_at(self.uri, {k: 'v-initial' for k in self.ingest_keys()}, 20)

        # Walk until the prepared conflict blocks it. The current cursor at that point is stable,
        # blocked on the prepared key, while ingest is independently positioned as the alternate.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(25))
        seen = []
        hit_conflict = False
        while not hit_conflict:
            try:
                self.assertEqual(cursor.next(), 0)
            except wiredtiger.WiredTigerError as e:
                self.assertIn(
                    wiredtiger.wiredtiger_strerror(wiredtiger.WT_PREPARE_CONFLICT), str(e))
                hit_conflict = True
                continue
            seen.append(cursor.get_key())

        # A role change now runs while the reader is still mid-walk, blocked on the stable-side
        # conflict. The walk keeps waiting on the prepared update across it.
        self.complete_step_down(15)
        self.assertRaisesException(wiredtiger.WiredTigerError, cursor.next,
            '/conflict with a prepared update/')

        # Resolving a straddling prepared transaction on the follower is not supported today
        # (FIXME-WT-18422): the rollback can be refused with a disaggregated conflict. Until the
        # update is resolved the walk keeps reporting the conflict; once it is, the walk resumes
        # where it stopped and returns every key exactly once.
        try:
            prepare_session.rollback_transaction()
            resolved = True
        except wiredtiger.WiredTigerError:
            self.assertEqual(prepare_session.get_last_error()[1], wiredtiger.WT_CONFLICT_DISAGG)
            resolved = False
        if resolved:
            while cursor.next() == 0:
                seen.append(cursor.get_key())
            self.assertEqual(seen, keys)
        else:
            self.assertRaisesException(wiredtiger.WiredTigerError, cursor.next,
                '/conflict with a prepared update/')

            # Resetting the walk releases the blocked position, so the next call completes the
            # step-down: the stable is reopened on the follower's checkpoint, which never carries a
            # prepared update, and a fresh walk returns every key.
            cursor.reset()
            restarted = []
            while cursor.next() == 0:
                restarted.append(cursor.get_key())
            self.assertEqual(restarted, keys)
        self.session.rollback_transaction()
        cursor.close()
        prepare_cursor.close()
        prepare_session.close()

    # A search abandons a walk blocked on a prepared update, so it reads the follower's checkpoint
    # like any other operation after the step-down, and gets no prepare conflict on the prepared
    # key.
    def test_search_while_stable_blocked_across_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'key10': 'v', 'key20': 'v', 'key30': 'v'}, 10)
        self.set_global_ts(1, 10)

        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri, None, None)
        prepare_session.begin_transaction()
        prepare_cursor['key20'] = 'v-prepared'
        prepare_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(12))

        self.set_step_down_ts(15)
        self.write_at(self.uri, {'key25': 'v'}, 20)

        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(35))
        self.assertEqual(cursor.next(), 0)
        self.assertRaisesException(wiredtiger.WiredTigerError, cursor.next,
            '/conflict with a prepared update/')
        self.complete_step_down(15)

        cursor.set_key('key20')
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 'v')

        cursor.reset()
        self.session.rollback_transaction()
        cursor.close()
        try:
            prepare_session.rollback_transaction()
        except wiredtiger.WiredTigerError:
            pass
        prepare_cursor.close()
        prepare_session.close()

# A walk that began before the step-down timestamp was announced reads stable alone. When it blocks
# on a stable prepared update and a step-down then completes, the walk keeps reporting the prepare
# conflict: the role change adds ingest to the walk, which resumes from where it stopped.
@disagg_test_class
class test_layered_async_stepdown17_direct_walk(LayeredStepdownMixin, wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),precise_checkpoint=true,' \
        'debug_mode=(disagg_stepdown_prepare=true),disaggregated=(role="leader")'
    directions = [
        ('next', dict(forward=True)),
        ('prev', dict(forward=False)),
    ]
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages, directions)
    uri = 'layered:test_layered_async_stepdown17_direct_walk'

    def test_direct_walk_blocked_across_step_down(self):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'key10': 'v', 'key30': 'v'}, 10)
        self.set_global_ts(1, 10)

        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri, None, None)
        prepare_session.begin_transaction()
        prepare_cursor['key20'] = 'v-prepared'
        prepare_session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(12))

        # The reader begins before the announcement, so it reads stable alone.
        cursor = self.session.open_cursor(self.uri, None, None)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(14))
        step = cursor.next if self.forward else cursor.prev
        self.assertEqual(step(), 0)
        self.assertEqual(cursor.get_key(), 'key10' if self.forward else 'key30')
        self.assertRaisesException(wiredtiger.WiredTigerError, step,
            '/conflict with a prepared update/')

        self.set_step_down_ts(15)
        writer = self.conn.open_session()
        writer_cursor = writer.open_cursor(self.uri, None, None)
        writer.begin_transaction()
        writer_cursor['key25'] = 'v'
        writer.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        writer_cursor.close()
        writer.close()
        self.complete_step_down(15)

        self.assertRaisesException(wiredtiger.WiredTigerError, step,
            '/conflict with a prepared update/')

        # Resetting releases the position blocked on the prepared update, which pins the live stable
        # table. Resolving a straddling prepared transaction on the follower is not supported yet
        # (FIXME-WT-18422), so the rollback can be refused.
        cursor.reset()
        self.session.rollback_transaction()
        cursor.close()
        try:
            prepare_session.rollback_transaction()
        except wiredtiger.WiredTigerError:
            pass
        prepare_cursor.close()
        prepare_session.close()

if __name__ == '__main__':
    wttest.run()
