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

# test_layered89.py
#   Test that cursor.next(), cursor.prev(), and cursor.search() on a follower
#   return committed values and do not raise WT_PREPARE_CONFLICT when the primary
#   has checkpointed a prepared (uncommitted) transaction.
#
#   Setup: the primary and follower both commit initial values, then prepare the same
#   update (same prepared_id, simulating oplog replay). The primary checkpoints with
#   preserve_prepared=true so the snapshot includes the pending update. The follower
#   advances its checkpoint to pick up the primary's snapshot, then rolls back its
#   own copy of the prepare.
#
#   Expected: all cursor operations return the committed values without error.

import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered89(wttest.WiredTigerTestCase):
    tablename = 'test_layered89'
    uri = 'layered:' + tablename

    disagg_storages = gen_disagg_storages('test_layered89', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_base_config = ',create,statistics=(all),precise_checkpoint=true,preserve_prepared=true,'

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def setup_with_prepared_updates(self, all_keys, prepare_keys, commit_ts=10, prepare_ts=20):
        """
        Both the primary and follower commit initial values for all_keys, then prepare
        updates for prepare_keys using the same prepared_id (simulating oplog replay).
        The primary checkpoints with the prepare still active (preserve_prepared=true)
        so the snapshot includes the pending update. The follower advances its checkpoint
        to pick up the primary's snapshot.

        Returns (conn_follow, session_follow, prepare_session_primary, prepare_session_follow).
        The caller must rollback prepare_session_follow before walking the cursor, then call
        resolve_prepared(prepare_session_primary) for cleanup.
        """
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config +
            'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, 'key_format=i,value_format=S')
        conn_follow.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        cursor = self.session.open_cursor(self.uri)
        for key in all_keys:
            self.session.begin_transaction()
            cursor[key] = 'committed_' + str(key)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

        # Replay the same committed writes on the follower (simulating oplog replay).
        cursor_follow = session_follow.open_cursor(self.uri)
        for key in all_keys:
            session_follow.begin_transaction()
            cursor_follow[key] = 'committed_' + str(key)
            session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor_follow.close()

        # Checkpoint the committed writes before introducing the prepare so that when the
        # primary later checkpoints with the prepare active, both the pending update and
        # the committed values are accessible in the snapshot.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(commit_ts))
        self.session.checkpoint()
        conn_follow.set_timestamp('stable_timestamp=' + self.timestamp_str(commit_ts))
        self.disagg_advance_checkpoint(conn_follow)

        # Prepare updates on the primary.
        prepare_session_primary = self.conn.open_session()
        prepare_cursor_primary = prepare_session_primary.open_cursor(self.uri)
        prepare_session_primary.begin_transaction()
        for key in prepare_keys:
            prepare_cursor_primary[key] = 'prepared_' + str(key)
        prepare_session_primary.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(prepare_ts) +
            ',prepared_id=' + self.prepared_id_str(1))
        prepare_cursor_primary.close()

        # Replay the same prepared transaction on the follower (simulating oplog replay).
        prepare_session_follow = conn_follow.open_session()
        prepare_cursor_follow = prepare_session_follow.open_cursor(self.uri)
        prepare_session_follow.begin_transaction()
        for key in prepare_keys:
            prepare_cursor_follow[key] = 'prepared_' + str(key)
        prepare_session_follow.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(prepare_ts) +
            ',prepared_id=' + self.prepared_id_str(1))
        prepare_cursor_follow.close()

        # Checkpoint the primary while the prepare is still active; with
        # preserve_prepared=true the pending update is included in the snapshot.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(prepare_ts))
        chkpt_session = self.conn.open_session()
        chkpt_session.checkpoint()
        chkpt_session.close()

        # Advance the follower checkpoint to pick up the primary's snapshot.
        conn_follow.set_timestamp('stable_timestamp=' + self.timestamp_str(prepare_ts))
        self.disagg_advance_checkpoint(conn_follow)

        return conn_follow, session_follow, prepare_session_primary, prepare_session_follow

    def setup_with_prepared_tombstones(self, all_keys, delete_keys, commit_ts=10, prepare_ts=20):
        """
        Same as setup_with_prepared_updates but prepares deletes for delete_keys instead
        of value updates. Both primary and follower apply the same committed writes and
        the same prepared deletes (same prepared_id).

        Returns (conn_follow, session_follow, prepare_session_primary, prepare_session_follow).
        """
        self.session.create(self.uri, 'key_format=i,value_format=S')
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config +
            'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, 'key_format=i,value_format=S')
        conn_follow.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))

        cursor = self.session.open_cursor(self.uri)
        for key in all_keys:
            self.session.begin_transaction()
            cursor[key] = 'committed_' + str(key)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

        # Replay the same committed writes on the follower (simulating oplog replay).
        cursor_follow = session_follow.open_cursor(self.uri)
        for key in all_keys:
            session_follow.begin_transaction()
            cursor_follow[key] = 'committed_' + str(key)
            session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor_follow.close()

        # Prepare deletes on the primary.
        prepare_session_primary = self.conn.open_session()
        prepare_cursor_primary = prepare_session_primary.open_cursor(self.uri)
        prepare_session_primary.begin_transaction()
        for key in delete_keys:
            prepare_cursor_primary.set_key(key)
            prepare_cursor_primary.remove()
        prepare_session_primary.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(prepare_ts) +
            ',prepared_id=' + self.prepared_id_str(1))
        prepare_cursor_primary.close()

        # Replay the same prepared deletes on the follower (simulating oplog replay).
        prepare_session_follow = conn_follow.open_session()
        prepare_cursor_follow = prepare_session_follow.open_cursor(self.uri)
        prepare_session_follow.begin_transaction()
        for key in delete_keys:
            prepare_cursor_follow.set_key(key)
            prepare_cursor_follow.remove()
        prepare_session_follow.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(prepare_ts) +
            ',prepared_id=' + self.prepared_id_str(1))
        prepare_cursor_follow.close()

        # Checkpoint the primary while the prepare is still active.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(prepare_ts))
        chkpt_session = self.conn.open_session()
        chkpt_session.checkpoint()
        chkpt_session.close()

        # Advance the follower checkpoint to pick up the primary's snapshot.
        conn_follow.set_timestamp('stable_timestamp=' + self.timestamp_str(prepare_ts))
        self.disagg_advance_checkpoint(conn_follow)

        return conn_follow, session_follow, prepare_session_primary, prepare_session_follow

    def evict_page(self, session, key):
        """Release the cached page so the next access reads from the checkpoint."""
        evict_session = session.connection.open_session('debug=(release_evict_page)')
        evict_cursor = evict_session.open_cursor(self.uri)
        evict_cursor.set_key(key)
        evict_cursor.search()
        evict_cursor.close()
        evict_session.close()

    def collect_keys_next(self, session):
        """Walk forward and return all visible keys."""
        cursor = session.open_cursor(self.uri)
        session.begin_transaction()
        keys = []
        while cursor.next() != wiredtiger.WT_NOTFOUND:
            keys.append(cursor.get_key())
        session.rollback_transaction()
        cursor.close()
        return keys

    def collect_keys_prev(self, session):
        """Walk backward and return all visible keys."""
        cursor = session.open_cursor(self.uri)
        session.begin_transaction()
        keys = []
        while cursor.prev() != wiredtiger.WT_NOTFOUND:
            keys.append(cursor.get_key())
        session.rollback_transaction()
        cursor.close()
        return keys

    def resolve_prepared(self, prepare_session_primary, rollback_ts):
        """
        Roll back the primary's prepared transaction and create a clean checkpoint
        so that test teardown can verify the table without error.
        """
        prepare_session_primary.rollback_transaction(
            'rollback_timestamp=' + self.timestamp_str(rollback_ts))
        prepare_session_primary.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(rollback_ts))
        self.session.checkpoint()

    def run_walk_test(self, all_keys, prepare_keys, walk_func, delete=False):
        """
        Common harness for cursor-walk tests on a follower whose checkpoint contains
        prepared updates (or tombstones). Returns the keys returned by walk_func.
        """
        if delete:
            conn_follow, session_follow, prepare_session_primary, prepare_session_follow = \
                self.setup_with_prepared_tombstones(all_keys, prepare_keys)
        else:
            conn_follow, session_follow, prepare_session_primary, prepare_session_follow = \
                self.setup_with_prepared_updates(all_keys, prepare_keys)

        # Roll back the follower's prepared transaction; the prior committed value
        # is now the most recent visible version on the follower.
        prepare_session_follow.rollback_transaction(
            'rollback_timestamp=' + self.timestamp_str(30))
        prepare_session_follow.close()

        # Evict cached pages so the next access reads from the checkpointed snapshot.
        self.evict_page(session_follow, all_keys[0])

        keys = walk_func(session_follow)

        session_follow.close()
        conn_follow.close()

        self.resolve_prepared(prepare_session_primary, rollback_ts=30)

        return keys

    def test_next_walk_prepared_update(self):
        """
        cursor.next() on a follower must return all committed keys without
        WT_PREPARE_CONFLICT when the primary has checkpointed a prepared update
        for a subset of keys.
        """
        all_keys = [1, 2, 3, 4, 5]
        keys = self.run_walk_test(all_keys, prepare_keys=[2, 4],
            walk_func=self.collect_keys_next)
        self.assertEqual(sorted(keys), all_keys)

    def test_prev_walk_prepared_update(self):
        """
        cursor.prev() on a follower must return all committed keys without
        WT_PREPARE_CONFLICT when the primary has checkpointed a prepared update
        for a subset of keys.
        """
        all_keys = [1, 2, 3, 4, 5]
        keys = self.run_walk_test(all_keys, prepare_keys=[2, 4],
            walk_func=self.collect_keys_prev)
        self.assertEqual(sorted(keys), all_keys)

    def test_next_walk_prepared_tombstone(self):
        """
        cursor.next() on a follower must return all committed keys without
        WT_PREPARE_CONFLICT when the primary has checkpointed a prepared delete
        for a subset of keys. The uncommitted delete must be invisible, so all
        keys must still be returned.
        """
        all_keys = [1, 2, 3, 4, 5]
        keys = self.run_walk_test(all_keys, prepare_keys=[2, 4],
            walk_func=self.collect_keys_next, delete=True)
        self.assertEqual(sorted(keys), all_keys)

    def test_prev_walk_prepared_tombstone(self):
        """
        cursor.prev() on a follower must return all committed keys without
        WT_PREPARE_CONFLICT when the primary has checkpointed a prepared delete
        for a subset of keys. The uncommitted delete must be invisible, so all
        keys must still be returned.
        """
        all_keys = [1, 2, 3, 4, 5]
        keys = self.run_walk_test(all_keys, prepare_keys=[2, 4],
            walk_func=self.collect_keys_prev, delete=True)
        self.assertEqual(sorted(keys), all_keys)

    def test_search_prepared_update(self):
        """
        cursor.search() on a follower must return the committed value without
        WT_PREPARE_CONFLICT when the primary has checkpointed a prepared update
        for that key.
        """
        all_keys = [1, 2, 3]
        prepare_keys = [2]

        conn_follow, session_follow, prepare_session_primary, prepare_session_follow = \
            self.setup_with_prepared_updates(all_keys, prepare_keys)

        # Roll back the follower's prepared transaction.
        prepare_session_follow.rollback_transaction(
            'rollback_timestamp=' + self.timestamp_str(30))
        prepare_session_follow.close()

        self.evict_page(session_follow, 1)

        cursor = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction()

        # Key 1: no prepared update; returns the committed value.
        cursor.set_key(1)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 'committed_1')

        # Key 2: primary checkpointed a prepared update; follower rolled back its copy.
        # cursor.search() must return the committed value without WT_PREPARE_CONFLICT.
        cursor.set_key(2)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_value(), 'committed_2')

        session_follow.rollback_transaction()
        cursor.close()

        session_follow.close()
        conn_follow.close()
        self.resolve_prepared(prepare_session_primary, rollback_ts=30)
