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
#
# test_layered84.py
#   Test layered cursor walks on a follower with an advanced checkpoint, exercising the
#   two-cursor merge path. Verifies correct behavior when:
#   - An overwrite update positions only the ingest cursor, then next() forces the stable
#     cursor to open.
#   - A prepared conflict occurs mid-walk and the cursor retries after the prepare resolves.

import os
import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import WiredTigerError, wiredtiger_strerror, WT_PREPARE_CONFLICT
from wtscenario import make_scenarios

@disagg_test_class
class test_layered84(wttest.WiredTigerTestCase):
    tablename = 'test_layered84'
    uri = 'layered:' + tablename

    disagg_storages = gen_disagg_storages('test_layered84', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_base_config = ',create,statistics=(all),precise_checkpoint=true,preserve_prepared=true,'

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def early_setup(self):
        os.mkdir('follower')
        os.mkdir('kv_home')
        os.symlink('../kv_home', 'follower/kv_home', target_is_directory=True)

    def is_prepare_conflict(self, e):
        return wiredtiger_strerror(WT_PREPARE_CONFLICT) in str(e)

    def populate_leader_and_checkpoint(self, keys):
        """Insert data on the leader and checkpoint so the follower's stable table has data."""
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        for key in keys:
            self.session.begin_transaction()
            cursor[key] = f'value_{key}'
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(10 + key))
        cursor.close()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.session.checkpoint()

    def open_follower(self):
        """Open a follower connection and advance its checkpoint so both cursors are active."""
        conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config +
            'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, 'key_format=i,value_format=S')
        self.disagg_advance_checkpoint(conn_follow)
        return conn_follow, session_follow

    def walk_next_collect(self, cursor):
        """Walk forward collecting keys, stopping on prepared conflict or end-of-table."""
        keys = []
        got_conflict = False
        while True:
            try:
                ret = cursor.next()
            except WiredTigerError as e:
                if self.is_prepare_conflict(e):
                    got_conflict = True
                    break
                raise
            if ret == wiredtiger.WT_NOTFOUND:
                break
            keys.append(cursor.get_key())
        return keys, got_conflict

    def walk_prev_collect(self, cursor):
        """Walk backward collecting keys, stopping on prepared conflict or end-of-table."""
        keys = []
        got_conflict = False
        while True:
            try:
                ret = cursor.prev()
            except WiredTigerError as e:
                if self.is_prepare_conflict(e):
                    got_conflict = True
                    break
                raise
            if ret == wiredtiger.WT_NOTFOUND:
                break
            keys.append(cursor.get_key())
        return keys, got_conflict

    def setup_committed_then_prepared(self, all_keys, committed_keys, prepared_key):
        """
        Set up a follower with committed writes on committed_keys and a prepared write
        on prepared_key. Returns (conn_follow, session_follow, cursor, prepare_session,
        prepare_cursor) with the main session's transaction already begun at read ts 60.
        """
        self.populate_leader_and_checkpoint(all_keys)
        conn_follow, session_follow = self.open_follower()

        ingest_cursor = session_follow.open_cursor(self.uri)
        for key in committed_keys:
            session_follow.begin_transaction()
            ingest_cursor[key] = f'committed_{key}'
            session_follow.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(30 + key))
        ingest_cursor.close()

        prepare_session = conn_follow.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)
        prepare_session.begin_transaction()
        prepare_cursor[prepared_key] = 'prepared_value'
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(50) +
            ',prepared_id=' + self.prepared_id_str(1))

        cursor = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        return conn_follow, session_follow, cursor, prepare_session, prepare_cursor

    def commit_prepared(self, prepare_session, prepare_cursor):
        """Commit a prepared transaction at timestamp 60."""
        prepare_session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(60) +
            ',durable_timestamp=' + self.timestamp_str(60))
        prepare_session.commit_transaction()
        prepare_cursor.close()
        prepare_session.close()

    def test_overwrite_update_then_next_on_follower(self):
        """
        On a follower with an advanced checkpoint, an overwrite update only opens the ingest
        cursor. A subsequent next() call needs the stable cursor, which forces it to be opened.
        Verify that the cursor position is preserved across the stable cursor open and that
        next() returns the correct remaining keys.
        """
        all_keys = [1, 2, 3, 4, 5]
        self.populate_leader_and_checkpoint(all_keys)
        conn_follow, session_follow = self.open_follower()

        # Position the cursor with a search (opens both ingest and stable, sets the layered
        # cursor's internal tracking to the ingest cursor). Then do an overwrite update in the
        # same transaction (keeps ingest positioned). Commit, advance the checkpoint so the stable
        # cursor needs to be reopened, then call next(). The cursor position must be preserved
        # when the stable cursor is reopened.
        follow_cursor = session_follow.open_cursor(self.uri, None, 'overwrite=true')
        session_follow.begin_transaction()

        # Search positions both cursors and tracks ingest as current.
        follow_cursor.set_key(3)
        self.assertEqual(follow_cursor.search(), 0)

        # Overwrite update in the same transaction  cursor stays positioned on ingest.
        follow_cursor.set_key(3)
        follow_cursor.set_value('updated_3')
        follow_cursor.update()
        session_follow.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(25))

        # Advance the checkpoint so the stable cursor needs to be reopened on next read.
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(25))
        self.session.checkpoint()
        self.disagg_advance_checkpoint(conn_follow)

        # next() needs to reopen the stable cursor while the ingest cursor is still positioned.
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(25))
        keys = []
        while follow_cursor.next() != wiredtiger.WT_NOTFOUND:
            keys.append(follow_cursor.get_key())
        session_follow.rollback_transaction()

        # After positioned at key 3, next() should return the remaining keys.
        self.assertTrue(len(keys) > 0, "next() should return keys after the positioned key")
        for key in keys:
            self.assertGreater(key, 3, f"next() returned key {key} which is not after position 3")

        follow_cursor.close()
        session_follow.close()
        conn_follow.close()

    def test_next_walk_prepare_conflict_mid_scan(self):
        """
        Forward cursor walk on a follower where ingest has a sparse subset of keys and
        stable has the full set. A prepared conflict on the ingest cursor occurs while the
        stable cursor is positioned on a key that hasn't been returned yet (key 3, which
        only exists in stable). After the prepare resolves, verify that the walk returns
        every key  including the one that the stable cursor was sitting on at the time
        of the conflict.
        """
        all_keys = [1, 2, 3, 4, 5, 6]
        self.populate_leader_and_checkpoint(all_keys)
        conn_follow, session_follow = self.open_follower()

        # Write only even keys to the follower's ingest table, so stable has keys (like 3, 5)
        # that ingest does not. This makes it possible for a missed advance on the stable
        # cursor to skip a key entirely.
        ingest_cursor = session_follow.open_cursor(self.uri)
        for key in [2, 4, 6]:
            session_follow.begin_transaction()
            ingest_cursor[key] = f'ingest_value_{key}'
            session_follow.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(30 + key))
        ingest_cursor.close()

        # Prepare an update on key 4  the next ingest key after 2.
        prepare_session = conn_follow.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)
        prepare_session.begin_transaction()
        prepare_cursor[4] = 'prepared_value'
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(50) +
            ',prepared_id=' + self.prepared_id_str(1))

        # Walk forward  should hit the prepared conflict at key 4.
        cursor = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        keys_before, got_conflict = self.walk_next_collect(cursor)
        self.assertTrue(got_conflict, "Expected prepared conflict during forward walk")

        # Commit the prepared transaction and retry the walk on the same cursor.
        prepare_session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(60) +
            ',durable_timestamp=' + self.timestamp_str(60))
        prepare_session.commit_transaction()
        prepare_cursor.close()
        prepare_session.close()

        keys_after, _ = self.walk_next_collect(cursor)

        session_follow.rollback_transaction()
        cursor.close()

        # Key 3 only exists in stable and was positioned on but not yet returned when the
        # conflict occurred. Verify it is not skipped.
        all_returned = set(keys_before + keys_after)
        self.assertEqual(all_returned, set(all_keys),
            f"Missing keys. Before: {keys_before}, After: {keys_after}")

        session_follow.close()
        conn_follow.close()

    def test_prev_walk_prepare_conflict_mid_scan(self):
        """
        Backward cursor walk on a follower encounters a prepared conflict with multiple
        keys in both ingest and stable. Same verification as the forward test but in
        reverse direction.
        """
        all_keys = [1, 2, 3, 4, 5]
        self.populate_leader_and_checkpoint(all_keys)
        conn_follow, session_follow = self.open_follower()

        ingest_cursor = session_follow.open_cursor(self.uri)
        for key in all_keys:
            session_follow.begin_transaction()
            ingest_cursor[key] = f'ingest_value_{key}'
            session_follow.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(30 + key))
        ingest_cursor.close()

        prepare_session = conn_follow.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)
        prepare_session.begin_transaction()
        prepare_cursor[3] = 'prepared_value'
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(50) +
            ',prepared_id=' + self.prepared_id_str(1))

        cursor = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        keys_before, got_conflict = self.walk_prev_collect(cursor)
        self.assertTrue(got_conflict, "Expected prepared conflict during backward walk")

        prepare_session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(60) +
            ',durable_timestamp=' + self.timestamp_str(60))
        prepare_session.commit_transaction()
        prepare_cursor.close()
        prepare_session.close()

        keys_after, _ = self.walk_prev_collect(cursor)

        session_follow.rollback_transaction()
        cursor.close()

        all_returned = set(keys_before + keys_after)
        self.assertEqual(all_returned, set(all_keys),
            f"Missing keys. Before: {keys_before}, After: {keys_after}")

        session_follow.close()
        conn_follow.close()

    def test_next_walk_committed_keys_then_prepared(self):
        """
        Forward walk where the follower has committed writes on keys 1-3 and a
        prepared write on key 4. The walk must return keys 1-3 before the conflict,
        and all five keys must appear exactly once across the full walk.
        """
        all_keys = [1, 2, 3, 4, 5]
        # committed_keys=[1,2,3] appear before prepared_key=4 in sort order.
        conn_follow, session_follow, cursor, prepare_session, prepare_cursor = \
            self.setup_committed_then_prepared(all_keys, [1, 2, 3], 4)

        keys_before, got_conflict = self.walk_next_collect(cursor)
        self.assertTrue(got_conflict, "Expected prepared conflict after committed keys 1-3")

        # Committed keys 1-3 must all be visible before the conflict at key 4.
        self.assertEqual(set(keys_before), {1, 2, 3},
            f"Expected keys 1-3 before conflict, got {keys_before}")

        self.commit_prepared(prepare_session, prepare_cursor)

        keys_after, _ = self.walk_next_collect(cursor)
        session_follow.rollback_transaction()
        cursor.close()

        # Each key must appear at most once across both segments.  If a key
        # returned before the conflict reappears after, the walk position was
        # not preserved and the scan restarted from scratch.
        for k in keys_after:
            self.assertNotIn(k, set(keys_before),
                f"Key {k} seen twice — walk position was not preserved across conflict")

        all_returned = set(keys_before + keys_after)
        self.assertEqual(all_returned, set(all_keys),
            f"Missing keys. Before: {keys_before}, After: {keys_after}")

        session_follow.close()
        conn_follow.close()

    def test_prev_walk_committed_keys_then_prepared(self):
        """
        Backward walk where the follower has committed writes on keys 3-5 and a
        prepared write on key 2. The walk must return keys 5-3 before the conflict,
        and all five keys must appear exactly once across the full walk.
        """
        all_keys = [1, 2, 3, 4, 5]
        # committed_keys=[3,4,5] appear before prepared_key=2 in reverse sort order.
        conn_follow, session_follow, cursor, prepare_session, prepare_cursor = \
            self.setup_committed_then_prepared(all_keys, [3, 4, 5], 2)

        keys_before, got_conflict = self.walk_prev_collect(cursor)
        self.assertTrue(got_conflict, "Expected prepared conflict after committed keys 3-5")

        # Committed keys 5, 4, 3 must all be visible before the conflict at key 2.
        self.assertEqual(set(keys_before), {3, 4, 5},
            f"Expected keys 3-5 before conflict, got {keys_before}")

        self.commit_prepared(prepare_session, prepare_cursor)

        keys_after, _ = self.walk_prev_collect(cursor)
        session_follow.rollback_transaction()
        cursor.close()

        # Each key must appear at most once across both segments.  If a key
        # returned before the conflict reappears after, the walk position was
        # not preserved and the scan restarted from scratch.
        for k in keys_after:
            self.assertNotIn(k, set(keys_before),
                f"Key {k} seen twice — walk position was not preserved across conflict")

        all_returned = set(keys_before + keys_after)
        self.assertEqual(all_returned, set(all_keys),
            f"Missing keys. Before: {keys_before}, After: {keys_after}")

        session_follow.close()
        conn_follow.close()

    def test_next_walk_ingest_only_committed_then_prepared(self):
        """
        Forward walk where committed follower writes introduce keys that do not exist
        in stable, followed by a prepared key that also only exists on the follower.
        All six keys must appear exactly once across the full walk.
        """
        # Stable has only odd keys; even keys and key 6 exist only on the follower.
        stable_keys = [1, 3, 5]
        self.populate_leader_and_checkpoint(stable_keys)
        conn_follow, session_follow = self.open_follower()

        # Commit new even keys — follower-only keys that interleave with stable keys.
        ingest_cursor = session_follow.open_cursor(self.uri)
        for key in [2, 4]:
            session_follow.begin_transaction()
            ingest_cursor[key] = f'committed_{key}'
            session_follow.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(30 + key))
        ingest_cursor.close()

        # Prepare key 6 — a follower-only key beyond the end of stable.
        prepare_session = conn_follow.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)
        prepare_session.begin_transaction()
        prepare_cursor[6] = 'prepared_value'
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(50) +
            ',prepared_id=' + self.prepared_id_str(1))

        cursor = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        keys_before, got_conflict = self.walk_next_collect(cursor)
        self.assertTrue(got_conflict, "Expected prepared conflict at key 6")

        # The committed follower keys 2 and 4 must appear before the conflict.
        for k in [2, 4]:
            self.assertIn(k, keys_before,
                f"Committed follower key {k} missing before conflict, got {keys_before}")

        prepare_session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(60) +
            ',durable_timestamp=' + self.timestamp_str(60))
        prepare_session.commit_transaction()
        prepare_cursor.close()
        prepare_session.close()

        keys_after, _ = self.walk_next_collect(cursor)

        session_follow.rollback_transaction()
        cursor.close()

        # Each key must appear at most once across both segments.  If a key
        # returned before the conflict reappears after, the walk position was
        # not preserved and the scan restarted from scratch.
        for k in keys_after:
            self.assertNotIn(k, set(keys_before),
                f"Key {k} seen twice — walk position was not preserved across conflict")

        all_returned = set(keys_before + keys_after)
        self.assertEqual(all_returned, {1, 2, 3, 4, 5, 6},
            f"Missing keys. Before: {keys_before}, After: {keys_after}")

        session_follow.close()
        conn_follow.close()

    def test_next_walk_prepare_conflict_first_key(self):
        """
        Forward walk on a follower where the very first next() hits a prepared conflict.
        Multiple committed keys exist in the follower's ingest table so the merge path is
        fully exercised after the conflict resolves.
        """
        all_keys = [1, 2, 3, 4, 5]
        self.populate_leader_and_checkpoint(all_keys)
        conn_follow, session_follow = self.open_follower()

        # Write committed keys to ingest so the merge has real data after the prepared key.
        ingest_cursor = session_follow.open_cursor(self.uri)
        for key in all_keys:
            session_follow.begin_transaction()
            ingest_cursor[key] = f'ingest_value_{key}'
            session_follow.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(30 + key))
        ingest_cursor.close()

        # Prepare key 1  the first key in sort order.
        prepare_session = conn_follow.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)
        prepare_session.begin_transaction()
        prepare_cursor[1] = 'prepared_value'
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(50) +
            ',prepared_id=' + self.prepared_id_str(1))

        cursor = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        got_conflict = False
        try:
            cursor.next()
        except WiredTigerError as e:
            if self.is_prepare_conflict(e):
                got_conflict = True
            else:
                raise
        self.assertTrue(got_conflict, "Expected prepared conflict on first next()")

        prepare_session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(60) +
            ',durable_timestamp=' + self.timestamp_str(60))
        prepare_session.commit_transaction()
        prepare_cursor.close()
        prepare_session.close()

        keys, _ = self.walk_next_collect(cursor)

        session_follow.rollback_transaction()
        cursor.close()

        self.assertEqual(set(keys), set(all_keys),
            f"Expected all keys after resolve, got {keys}")

        session_follow.close()
        conn_follow.close()
