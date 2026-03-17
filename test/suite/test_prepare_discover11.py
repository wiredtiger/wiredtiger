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
# test_prepare_discover11.py
#   Test that a rolled-back prepared delete on the follower's ingest table, sandwiched between
#   two committed values for the same key, is handled correctly during drain. This exercises
#   the tombstone allocation logic in the drain loop when the previous update in the chain
#   was a rolled-back prepare stored in the rollback union layout.
#
#   Ingest chain for the key (newest first):
#     1. Committed value at ts=220
#     2. Rolled-back prepared delete (txnid=ABORTED, rollback union layout)
#     3. Committed value at ts=160 (stop fields reference the rolled-back prepare)
#
#   Without the fix, the tombstone allocation check compares entry 3's stop fields against
#   entry 2's remapped union fields, producing a spurious tombstone.

import wiredtiger
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_prepare_discover11(wttest.WiredTigerTestCase):
    tablename = 'test_prepare_discover11'
    uri = 'layered:' + tablename

    disagg_storages = gen_disagg_storages('test_prepare_discover11', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_base_config = 'cache_size=10MB,statistics=(all),precise_checkpoint=true,preserve_prepared=true,'

    def conn_config(self):
        return self.conn_base_config + 'disaggregated=(role="leader")'

    def test_prepare_rollback_delete_between_values(self):

        # Phase 1: Leader creates the table and inserts initial data, then checkpoints.
        self.pr("=== Phase 1: Leader setup and checkpoint ===")

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(50))
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(50))

        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        cursor[1] = "initial_value"
        self.session.commit_transaction("commit_timestamp=" + self.timestamp_str(60))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(150))

        checkpoint_session = self.conn.open_session()
        checkpoint_session.checkpoint()
        checkpoint_session.close()

        # Phase 2: Open follower and pick up the checkpoint.
        self.pr("=== Phase 2: Open follower ===")

        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
            self.conn_base_config + 'disaggregated=(role="follower")')
        self.disagg_advance_checkpoint(conn_follow)

        # Phase 3: On the follower, build the problematic ingest chain:
        #   committed value at ts=160 -> rolled-back prepared delete -> committed value at ts=220
        self.pr("=== Phase 3: Build ingest chain on follower ===")

        session_f = conn_follow.open_session()
        cursor_f = session_f.open_cursor(self.uri)

        # Write a committed value above the checkpoint timestamp.
        session_f.begin_transaction()
        cursor_f[1] = "value_160"
        session_f.commit_transaction("commit_timestamp=" + self.timestamp_str(160))

        # Prepare a delete on the same key, then rollback.
        session_f.begin_transaction()
        cursor_f.set_key(1)
        self.assertEqual(0, cursor_f.remove())
        session_f.prepare_transaction('prepare_timestamp=' + self.timestamp_str(170) +
                                     ',prepared_id=' + self.prepared_id_str(500))
        session_f.rollback_transaction('rollback_timestamp=' + self.timestamp_str(180))

        # Write a newer committed value.
        session_f.begin_transaction()
        cursor_f[1] = "value_220"
        session_f.commit_transaction("commit_timestamp=" + self.timestamp_str(220))

        cursor_f.close()
        session_f.close()

        # Close the leader.
        cursor.close()
        self.conn.close("debug=(skip_checkpoint=true)")

        # Phase 4: Step up to leader, triggering drain.
        self.pr("=== Phase 4: Step up (triggers drain) ===")
        conn_follow.reconfigure('disaggregated=(role="leader")')
        conn_follow.set_timestamp('stable_timestamp=' + self.timestamp_str(250))
        ckpt_session = conn_follow.open_session()
        ckpt_session.checkpoint()
        ckpt_session.close()

        # Phase 5: Verify data correctness.
        self.pr("=== Phase 5: Verify data ===")

        read_session = conn_follow.open_session()
        read_cursor = read_session.open_cursor(self.uri)

        # At ts=160, the first committed value should be visible.
        read_session.begin_transaction("read_timestamp=" + self.timestamp_str(160))
        self.assertEqual(read_cursor[1], "value_160")
        read_session.rollback_transaction()

        # At ts=220, the newer committed value should be visible (the prepared delete was
        # rolled back, so it should have no effect).
        read_session.begin_transaction("read_timestamp=" + self.timestamp_str(220))
        self.assertEqual(read_cursor[1], "value_220")
        read_session.rollback_transaction()

        read_cursor.close()
        read_session.close()
