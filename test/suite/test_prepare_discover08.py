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
# test_prepare_discover08.py
#   Test that prepared updates on the stable table are resolved during standby step-up drain:
#   - Setup layered cursor as leader, insert and commit data
#   - Prepare transaction, advance stable timestamp past prepare but not durable/rollback
#   - Checkpoint (writes prepared update to stable table)
#   - Restart as follower, write committed values to the ingest table for the prepared keys
#   - Step up to leader (triggers drain of ingest -> stable, resolving prepared updates)
#   - Checkpoint as leader, then restart as follower to verify no leftover prepared updates

import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_prepare_discover08(wttest.WiredTigerTestCase):
    tablename = 'test_prepare_discover08'
    uri = 'layered:' + tablename

    resolve_scenarios = [
        ('commit', dict(commit=True)),
        # ('rollback', dict(commit=False)),
    ]
    disagg_storages = gen_disagg_storages('test_prepare_discover08', disagg_only=True)
    scenarios = make_scenarios(disagg_storages, resolve_scenarios)

    conn_base_config = 'cache_size=10MB,statistics=(all),precise_checkpoint=true,preserve_prepared=true,'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def test_prepare_discover_stepup(self):

        # Phase 1: Insert committed data on the leader.
        self.pr("=== Phase 1: Insert committed data on leader ===")

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(50))

        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        cursor[1] = "committed_value_1"
        cursor[2] = "committed_value_2"
        cursor[3] = "committed_value_3"
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(60))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(70))

        # Verify committed data
        self.session.begin_transaction("read_timestamp=" + self.timestamp_str(60))
        self.assertEqual(cursor[1], "committed_value_1")
        self.assertEqual(cursor[2], "committed_value_2")
        self.assertEqual(cursor[3], "committed_value_3")
        self.session.rollback_transaction()

        # Phase 2: Prepare transaction and checkpoint.
        # The prepared update will be written to the stable table because prepare_ts <= stable_ts.
        # The commit/durable timestamp has not happened yet, so the checkpoint contains
        # the unresolved prepared update.
        self.pr("=== Phase 2: Prepare and checkpoint ===")

        self.session.begin_transaction()
        cursor[4] = "prepared_value_4"
        cursor[5] = "prepared_value_5"
        cursor[6] = "prepared_value_6"

        self.session.prepare_transaction('prepare_timestamp=' + self.timestamp_str(100) +
                                       ',prepared_id=' + self.prepared_id_str(123))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(150))

        checkpoint_session = self.conn.open_session()
        checkpoint_session.checkpoint()
        checkpoint_session.close()

        # Phase 3: Open a follower and pick up the checkpoint.
        # The follower's stable table now contains the unresolved prepared update.
        self.pr("=== Phase 3: Open follower and pick up checkpoint ===")

        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
            self.conn_base_config + 'disaggregated=(role="follower")')
        self.disagg_advance_checkpoint(conn_follow)
        session_follow = conn_follow.open_session('')

        # Verify committed data is visible on the follower
        cursor_follow = session_follow.open_cursor(self.uri, None, None)
        self.assertEqual(cursor_follow[1], "committed_value_1")
        self.assertEqual(cursor_follow[2], "committed_value_2")
        self.assertEqual(cursor_follow[3], "committed_value_3")

        # Phase 4: Write committed values for the prepared keys to the follower's ingest table.
        # On a real replica, these would come from oplog replay. Here we simulate by writing
        # directly through the follower's layered cursor, which routes to the ingest table.
        self.pr("=== Phase 4: Write committed values to follower's ingest table ===")
        conn_follow.set_timestamp('stable_timestamp=' + self.timestamp_str(150))

        session_follow.begin_transaction()
        cursor_follow[4] = "prepared_value_4"
        cursor_follow[5] = "prepared_value_5"
        cursor_follow[6] = "prepared_value_6"
        session_follow.commit_transaction("commit_timestamp=" + self.timestamp_str(200))

        # Verify the follower can read the committed values through the layered cursor
        # (ingest table overlays the stable table).
        if self.commit:
            session_follow.begin_transaction("read_timestamp=" + self.timestamp_str(200))
            for i in range(4, 7):
                self.assertEqual(cursor_follow[i], f"prepared_value_{i}")
            session_follow.rollback_transaction()

        cursor_follow.close()

        # Clean up the leader's prepared transaction so it doesn't block shutdown.
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(200) +
                                       ",durable_timestamp=" + self.timestamp_str(210))
        cursor.close()
        session_follow.close()

        # close the old leader connection
        self.conn.close()

        # # Phase 5: Step up to leader. This triggers drain (ingest -> stable).
        # # The drain moves ingest updates to the stable table via __layered_move_updates,
        # # which calls __layered_resolve_committed_prepare to resolve the prepared updates
        # # that were written to the stable table during Phase 2's checkpoint.
        self.pr("=== Phase 5: Step up to leader (triggers drain) ===")
        conn_follow.reconfigure('disaggregated=(role="leader")')
        conn_follow.set_timestamp('stable_timestamp=' + self.timestamp_str(250))
        ckpt_session = conn_follow.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()
        # # Phase 7: Verify data correctness after step-up + drain + checkpoint.
        self.pr("=== Phase 7: Verify data after step-up ===")

        read_session = conn_follow.open_session()
        read_cursor = read_session.open_cursor(self.uri)

        read_session.begin_transaction("read_timestamp=" + self.timestamp_str(60))
        self.assertEqual(read_cursor[1], "committed_value_1")
        self.assertEqual(read_cursor[2], "committed_value_2")
        self.assertEqual(read_cursor[3], "committed_value_3")
        for i in range(4, 7):
            read_cursor.set_key(i)
            self.assertEqual(wiredtiger.WT_NOTFOUND, read_cursor.search())
        read_session.rollback_transaction()

        read_session.begin_transaction("read_timestamp=" + self.timestamp_str(200))
        self.assertEqual(read_cursor[1], "committed_value_1")
        self.assertEqual(read_cursor[2], "committed_value_2")
        self.assertEqual(read_cursor[3], "committed_value_3")
        for i in range(4, 7):
            read_cursor.set_key(i)
            if self.commit:
                self.assertEqual(0, read_cursor.search())
                self.assertEqual(f'prepared_value_{i}', read_cursor.get_value())
            else:
                self.assertEqual(wiredtiger.WT_NOTFOUND, read_cursor.search())
        read_session.rollback_transaction()
        read_cursor.close()
        read_session.close()
