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
# test_layered_follower_insert_update.py
#   Validate that cursor insert and update on a follower node skip the key
#   search.  On the follower, the primary already validated the operation so
#   the key-existence lookup is redundant and can be skipped to improve
#   performance.

import wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_layered79(wttest.WiredTigerTestCase, DisaggConfigMixin):
    conn_base_config = 'disaggregated=(page_log=palite),'
    disagg_storages = gen_disagg_storages('test_layered79', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    uri = 'layered:test_layered_follower_insert_update'
    nkeys = 50

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader"),'

    def create_and_populate(self):
        """Create the table and insert nkeys initial records on the leader."""
        self.session.create(self.uri, 'key_format=i,value_format=S')
        c = self.session.open_cursor(self.uri)
        for i in range(self.nkeys):
            self.session.begin_transaction()
            c[i] = 'initial_{}'.format(i)
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(i + 1))
        c.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(self.nkeys))
        self.session.checkpoint()

    def switch_to_follower(self):
        """Reopen the connection in follower mode at the latest checkpoint."""
        meta = self.disagg_get_complete_checkpoint_meta()
        self.reopen_conn(config=self.conn_base_config +
            f'disaggregated=(role="follower",checkpoint_meta="{meta}")')

    def test_follower_insert_overwrite(self):
        """Insert new keys on follower with overwrite cursor. verify values are readable."""
        self.create_and_populate()
        self.switch_to_follower()

        # Insert additional keys (beyond those in stable) on the follower.
        c = self.session.open_cursor(self.uri)
        for i in range(self.nkeys, self.nkeys * 2):
            self.session.begin_transaction()
            c[i] = 'follower_{}'.format(i)
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(self.nkeys + i + 1))
        c.close()

        # Verify the inserted keys are readable on the follower.
        c = self.session.open_cursor(self.uri)
        for i in range(self.nkeys, self.nkeys * 2):
            self.assertEqual(c[i], 'follower_{}'.format(i))
        c.close()

    def test_follower_insert_non_overwrite_skips_duplicate_check(self):
        """
        Insert with overwrite=false on a follower skips the duplicate key search.
        The primary already validated the insert. The follower goes straight to writing
        the ingest cursor without checking for an existing key in the stable table.
        The insert must succeed (no WT_DUPLICATE_KEY) and the new value must be visible.
        """
        self.create_and_populate()
        self.switch_to_follower()

        # Re-insert the same keys that exist in stable, using overwrite=false.  Before
        # this optimisation the layered cursor would search stable, find the key, and
        # return WT_DUPLICATE_KEY.  After the optimisation the search is skipped on
        # the follower so the insert succeeds and writes the new value to ingest.
        c = self.session.open_cursor(self.uri, None, 'overwrite=false')
        for i in range(self.nkeys):
            self.session.begin_transaction()
            c.set_key(i)
            c.set_value('updated_follower_{}'.format(i))
            ret = c.insert()
            self.assertEqual(ret, 0,
                'follower non-overwrite insert should succeed without duplicate key check')
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(self.nkeys + i + 1))
        c.close()

        # Verify the new values are visible (ingest takes priority over stable on follower).
        c = self.session.open_cursor(self.uri)
        for i in range(self.nkeys):
            self.assertEqual(c[i], 'updated_follower_{}'.format(i))
        c.close()

    def test_follower_update_overwrite(self):
        """Update existing stable keys on follower with overwrite cursor. Verify updated values."""
        self.create_and_populate()
        self.switch_to_follower()

        # Update all keys that came from the leader checkpoint.
        c = self.session.open_cursor(self.uri)
        for i in range(self.nkeys):
            self.session.begin_transaction()
            c[i] = 'updated_{}'.format(i)
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(self.nkeys + i + 1))
        c.close()

        # Verify the updated values are readable.
        c = self.session.open_cursor(self.uri)
        for i in range(self.nkeys):
            self.assertEqual(c[i], 'updated_{}'.format(i))
        c.close()

    def test_follower_update_non_overwrite_skips_key_existence_check(self):
        """
        Update with overwrite=false on a follower skips the key-existence search.
        The primary already confirmed the key exists before dispatching the update.
        The follower can write directly to the ingest cursor without re-checking stable.
        The update must succeed and the new value must be visible.
        """
        self.create_and_populate()
        self.switch_to_follower()

        # Update keys that live in stable using a non-overwrite cursor.  Before this
        # optimisation the layered cursor would search stable to confirm the key exists.
        # After the optimisation that search is skipped on the follower.
        c = self.session.open_cursor(self.uri, None, 'overwrite=false')
        for i in range(self.nkeys):
            self.session.begin_transaction()
            c.set_key(i)
            c.set_value('updated_follower_{}'.format(i))
            ret = c.update()
            self.assertEqual(ret, 0,
                'follower non-overwrite update should succeed without key existence check')
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(self.nkeys + i + 1))
        c.close()

        # Verify the updated values are visible (ingest takes priority over stable).
        c = self.session.open_cursor(self.uri)
        for i in range(self.nkeys):
            self.assertEqual(c[i], 'updated_follower_{}'.format(i))
        c.close()

    def test_follower_insert_then_update(self):
        """
        Insert new keys on follower then update them. Verify the update sees the
        value written by the prior insert rather than a stale stable value.
        """
        self.create_and_populate()
        self.switch_to_follower()

        # Insert fresh keys into ingest (keys nkeys..2*nkeys-1 do not exist in stable).
        insert_ts_base = self.nkeys + 1
        c = self.session.open_cursor(self.uri)
        for i in range(self.nkeys, self.nkeys * 2):
            self.session.begin_transaction()
            c[i] = 'insert_{}'.format(i)
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(insert_ts_base + i))
        c.close()

        # Now update those same keys on the follower.
        update_ts_base = insert_ts_base + self.nkeys * 2
        c = self.session.open_cursor(self.uri)
        for i in range(self.nkeys, self.nkeys * 2):
            self.session.begin_transaction()
            c[i] = 'update_{}'.format(i)
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(update_ts_base + i))
        c.close()

        # Verify the final values reflect the update, not the earlier insert.
        c = self.session.open_cursor(self.uri)
        for i in range(self.nkeys, self.nkeys * 2):
            self.assertEqual(c[i], 'update_{}'.format(i))
        c.close()
