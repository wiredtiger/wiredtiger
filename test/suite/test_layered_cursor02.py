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
#
# test_layered_cursor02.py
#   Test that a cursor walk returns correct results after encountering
#   WT_PREPARE_CONFLICT on a layered cursor.
#
#   Bug scenario:
#   After next()/prev() returns WT_PREPARE_CONFLICT, the btree cursor clears
#   its key flags but remains positioned on the page. The layered cursor's
#   reset logic skips that constituent (because the key flags are cleared) but
#   still marks the layered cursor as unpositioned. On retry, the cursor detects
#   the constituent is still positioned and takes the wrong code path, leading
#   to an assert failure or wrong results.

import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wiredtiger import WiredTigerError, wiredtiger_strerror, WT_PREPARE_CONFLICT
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_cursor02(wttest.WiredTigerTestCase):
    tablename = 'test_layered_cursor02'
    uri = 'layered:' + tablename

    disagg_storages = gen_disagg_storages('test_layered_cursor02', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_base_config = 'cache_size=10MB,statistics=(all),precise_checkpoint=true,preserve_prepared=true,'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="follower")'

    def is_prepare_conflict(self, e):
        """Check if a WiredTigerError is a prepare conflict."""
        return wiredtiger_strerror(WT_PREPARE_CONFLICT) in str(e)

    def setup_table_with_data(self, keys, session=None, conn=None):
        """Insert committed data into the table."""
        if session is None:
            session = self.session
        if conn is None:
            conn = self.conn

        conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))

        session.create(self.uri, 'key_format=i,value_format=S')
        cursor = session.open_cursor(self.uri)

        session.begin_transaction()
        for key in keys:
            cursor[key] = f'value_{key}'
        session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))

        conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        cursor.close()

    def walk_next_until_conflict(self, cursor):
        """Walk forward collecting keys until PREPARE_CONFLICT or end."""
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

    def walk_prev_until_conflict(self, cursor):
        """Walk backward collecting keys until PREPARE_CONFLICT or end."""
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

    def test_next_walk_prepare_conflict_mid_scan(self):
        """
        Forward cursor walk encounters WT_PREPARE_CONFLICT after several successful
        next() calls. After resolving the prepare, verify the cursor can complete
        a walk with all keys present.
        """
        all_keys = [1, 2, 3, 4, 5]
        self.setup_table_with_data(all_keys)

        # Prepare an update on key 3 (middle of the range).
        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)
        prepare_session.begin_transaction()
        prepare_cursor[3] = 'prepared_value'
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(50) +
            ',prepared_id=' + self.prepared_id_str(1))

        # Walk forward  should hit PREPARE_CONFLICT at key 3.
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        keys_before, got_conflict = self.walk_next_until_conflict(cursor)
        self.assertTrue(got_conflict, "Expected WT_PREPARE_CONFLICT during forward walk")
        self.pr(f'Keys before conflict: {keys_before}')

        # Commit the prepared transaction.
        prepare_session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(60) +
            ',durable_timestamp=' + self.timestamp_str(60))
        prepare_session.commit_transaction()
        prepare_cursor.close()
        prepare_session.close()

        # Retry next() on the SAME cursor without reset.
        # The bug: old code would crash (assert) or return wrong/incomplete results.
        keys_after, _ = self.walk_next_until_conflict(cursor)
        self.pr(f'Keys after resolve: {keys_after}')

        self.session.rollback_transaction()
        cursor.close()

        # Verify all keys appear across both walks.
        all_returned = set(keys_before + keys_after)
        self.assertEqual(all_returned, set(all_keys),
            f"Missing keys. Before: {keys_before}, After: {keys_after}")

    def test_prev_walk_prepare_conflict_mid_scan(self):
        """
        Backward cursor walk encounters WT_PREPARE_CONFLICT. Same verification
        as the forward test but in reverse direction.
        """
        all_keys = [1, 2, 3, 4, 5]
        self.setup_table_with_data(all_keys)

        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)
        prepare_session.begin_transaction()
        prepare_cursor[3] = 'prepared_value'
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(50) +
            ',prepared_id=' + self.prepared_id_str(1))

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        keys_before, got_conflict = self.walk_prev_until_conflict(cursor)
        self.assertTrue(got_conflict, "Expected WT_PREPARE_CONFLICT during backward walk")
        self.pr(f'Keys before conflict (prev): {keys_before}')

        prepare_session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(60) +
            ',durable_timestamp=' + self.timestamp_str(60))
        prepare_session.commit_transaction()
        prepare_cursor.close()
        prepare_session.close()

        keys_after, _ = self.walk_prev_until_conflict(cursor)
        self.pr(f'Keys after resolve (prev): {keys_after}')

        self.session.rollback_transaction()
        cursor.close()

        all_returned = set(keys_before + keys_after)
        self.assertEqual(all_returned, set(all_keys),
            f"Missing keys. Before: {keys_before}, After: {keys_after}")

    def test_next_walk_prepare_conflict_first_key(self):
        """
        Forward walk where the very first next() hits WT_PREPARE_CONFLICT.
        Tests the fresh-start path where neither constituent is positioned yet.
        """
        all_keys = [1, 2, 3, 4, 5]
        self.setup_table_with_data(all_keys)

        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)
        prepare_session.begin_transaction()
        prepare_cursor[1] = 'prepared_value'
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(50) +
            ',prepared_id=' + self.prepared_id_str(1))

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        # First next() should hit PREPARE_CONFLICT immediately.
        got_conflict = False
        try:
            cursor.next()
        except WiredTigerError as e:
            if self.is_prepare_conflict(e):
                got_conflict = True
            else:
                raise
        self.assertTrue(got_conflict, "Expected WT_PREPARE_CONFLICT on first next()")

        # Resolve and retry.
        prepare_session.timestamp_transaction(
            'commit_timestamp=' + self.timestamp_str(60) +
            ',durable_timestamp=' + self.timestamp_str(60))
        prepare_session.commit_transaction()
        prepare_cursor.close()
        prepare_session.close()

        keys, _ = self.walk_next_until_conflict(cursor)
        self.pr(f'Keys after resolve: {keys}')

        self.session.rollback_transaction()
        cursor.close()

        self.assertEqual(set(keys), set(all_keys),
            f"Expected all keys after resolve, got {keys}")

    def test_next_then_prev_after_prepare_conflict(self):
        """
        After next() returns WT_PREPARE_CONFLICT, calling prev() should
        return a valid key without crashing.
        """
        all_keys = [1, 3, 5]
        self.setup_table_with_data(all_keys)

        # Prepare key 2 (between 1 and 3).
        prepare_session = self.conn.open_session()
        prepare_cursor = prepare_session.open_cursor(self.uri)
        prepare_session.begin_transaction()
        prepare_cursor[2] = 'prepared_value'
        prepare_session.prepare_transaction(
            'prepare_timestamp=' + self.timestamp_str(50) +
            ',prepared_id=' + self.prepared_id_str(1))

        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(60))

        # Position at key 1.
        cursor.set_key(1)
        self.assertEqual(cursor.search(), 0)
        self.assertEqual(cursor.get_key(), 1)

        # next() should hit prepared key 2.
        got_conflict = False
        try:
            cursor.next()
        except WiredTigerError as e:
            if self.is_prepare_conflict(e):
                got_conflict = True
            else:
                raise
        self.assertTrue(got_conflict, "Expected WT_PREPARE_CONFLICT on next()")

        # After PREPARE_CONFLICT, calling prev() should not crash and should
        # return a valid key from the table.
        ret = cursor.prev()
        self.assertEqual(ret, 0, "prev() should succeed after prepare conflict on next()")
        key = cursor.get_key()
        self.assertIn(key, all_keys + [2], f"prev() returned unexpected key {key}")

        prepare_session.rollback_transaction()
        prepare_cursor.close()
        prepare_session.close()

        self.session.rollback_transaction()
        cursor.close()

# Test the original bug: an overwrite update on a follower positions only the ingest cursor
# because the stable cursor is not needed for writes. A subsequent next() call requires the
# stable cursor to be opened, and the old code would incorrectly save and reposition the key,
# losing track of where the ingest cursor was positioned. This caused the layered cursor to
# skip keys that had already been passed by the ingest cursor.
@disagg_test_class
class test_layered_cursor02_overwrite(wttest.WiredTigerTestCase):
    tablename = 'test_layered_cursor02_overwrite'
    uri = 'layered:' + tablename

    disagg_storages = gen_disagg_storages('test_layered_cursor02_overwrite', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_base_config = ',create,statistics=(all),'

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    def early_setup(self):
        import os
        os.mkdir('follower')
        os.mkdir('kv_home')
        os.symlink('../kv_home', 'follower/kv_home', target_is_directory=True)

    def test_overwrite_update_then_next_on_follower(self):
        """
        On a follower with an advanced checkpoint, an overwrite update only opens the ingest
        cursor. A subsequent next() call needs the stable cursor, which forces it to be opened.
        The old code would lose the cursor's position during this open, causing the walk to
        skip keys. Verify that all keys are returned after an overwrite update followed by next().
        """
        all_keys = [1, 2, 3, 4, 5]

        # Insert data on the leader and checkpoint.
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(10))
        for key in all_keys:
            self.session.begin_transaction()
            cursor[key] = f'value_{key}'
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(10 + key))
        cursor.close()

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))
        self.session.checkpoint()

        # Set up follower and advance its checkpoint.
        conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + self.conn_base_config +
            'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, 'key_format=i,value_format=S')

        self.disagg_advance_checkpoint(conn_follow)

        # On the follower: overwrite update positions ingest only (stable not opened).
        follow_cursor = session_follow.open_cursor(self.uri, None, 'overwrite=true')
        session_follow.begin_transaction()
        follow_cursor.set_key(3)
        follow_cursor.set_value('updated_3')
        follow_cursor.update()
        session_follow.commit_transaction(
            'commit_timestamp=' + self.timestamp_str(25))

        # Now call next() on the same cursor. This needs stable -> triggers open_cursors.
        # The old code would crash here with "constitute cursor already positioned".
        session_follow.begin_transaction('read_timestamp=' + self.timestamp_str(25))
        keys = []
        while follow_cursor.next() != wiredtiger.WT_NOTFOUND:
            keys.append(follow_cursor.get_key())
        session_follow.rollback_transaction()

        # Verify all keys are returned.
        self.assertEqual(set(keys), set(all_keys),
            f"Expected all keys, got {keys}")

        follow_cursor.close()
        session_follow.close()
        conn_follow.close()
