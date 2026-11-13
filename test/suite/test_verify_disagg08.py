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

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_verify_disagg08.py
#    Verify a leaf page rebuilt on a follower does not report corruption when the
# leader dropped a deleted key whose time window lies outside the aggregate of the keys it kept.
#
# The leader drops a deleted key when its delete is visible to all, which requires the checkpoint's
# pinned timestamp, and oldest transaction id. Rebuilding the page requires the same information,
# and a follower does not have access to same values from the checkpoint, which can lead to a
# differently built page, which can then disagree with the checkpoint's aggregate.
@disagg_test_class
class test_verify_disagg08(wttest.WiredTigerTestCase):
    test_name = __qualname__
    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    conn_config = 'disaggregated=(role="leader"),page_delta=(delta_pct=100,leaf_page_delta=true)'
    conn_config_follower = 'disaggregated=(role="follower"),statistics=(all)'

    table_cfg = 'key_format=S,value_format=S,block_manager=disagg'
    uri = f'layered:{test_name}'

    def ts(self, t):
        return 'commit_timestamp=' + self.timestamp_str(t)

    def put(self, cursor, key, t):
        self.session.begin_transaction()
        cursor[key] = 'a' * 200
        self.session.commit_transaction(self.ts(t))

    def remove(self, cursor, key, t):
        self.session.begin_transaction()
        cursor.set_key(key)
        cursor.remove()
        self.session.commit_transaction(self.ts(t))

    def verify_leader_and_follower(self, cursor):
        cursor.close()

        # A follower picks up only the latest checkpoint and has never set its oldest timestamp.
        conn_follow = self.wiredtiger_open('follower', self.extensionsConfig() + ',create,' +
                                           self.conn_config_follower)
        session_follow = conn_follow.open_session('')
        self.disagg_advance_checkpoint_and_wait(conn_follow)

        # The leader rebuilds the leaf the same way it wrote it.
        self.verifyUntilSuccess(self.session)

        # The follower keeps the deleted key.
        self.verifyUntilSuccess(session_follow)

        self.reader.rollback_transaction()
        self.reader.close()
        session_follow.close()
        conn_follow.close()

    def test_durable_start_after(self):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                                ',stable_timestamp=' + self.timestamp_str(1))
        self.session.create(self.uri, self.table_cfg)
        cursor = self.session.open_cursor(self.uri, None, None)

        # The dropped key is inserted at 3 and deleted at 4 by an early transaction. The kept key
        # comes from a later transaction with an older timestamp, so the aggregate's newest durable
        # start is 2, below the dropped key's 3.
        self.put(cursor, 'a', 3)
        self.remove(cursor, 'a', 4)
        # Pin the oldest transaction id by starting a long-running transaction, to ensure that
        # kept keys do not have their time windows cleared.
        self.reader = self.conn.open_session('')
        self.reader.begin_transaction()
        self.put(cursor, '1', 2)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(4))
        self.session.checkpoint()

        # Deleting a kept key dirties the leaf, so the leader reconciles it, and with the oldest
        # timestamp being ahead of the delete, key 'a' is dropped in the delta.
        self.remove(cursor, '1', 5)
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(4) +
                                ',stable_timestamp=' + self.timestamp_str(5))
        self.session.checkpoint()

        self.verify_leader_and_follower(cursor)

    def test_durable_stop_after(self):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
                                ',stable_timestamp=' + self.timestamp_str(1))
        self.session.create(self.uri, self.table_cfg)
        cursor = self.session.open_cursor(self.uri, None, None)

        # The dropped key is inserted at 2 and deleted at 5 by early transactions. The kept key is
        # deleted at 3 by a later transaction with an older timestamp, so the aggregate's newest
        # durable stop is 3, below the dropped key's 5.
        self.put(cursor, 'a', 2)
        self.put(cursor, '1', 2)
        self.remove(cursor, 'a', 5)
        # Pin the oldest transaction id by starting a long-running transaction, to ensure that
        # kept keys do not have their time windows cleared.
        self.reader = self.conn.open_session('')
        self.reader.begin_transaction()
        self.remove(cursor, '1', 3)
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(5))
        self.session.checkpoint()

        # Inserting a new key dirties the leaf, so the leader reconciles it, and with the oldest
        # timestamp being ahead of the delete, key 'a' is dropped in the delta.
        self.put(cursor, 'z', 6)
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(5) +
                                ',stable_timestamp=' + self.timestamp_str(6))
        self.session.checkpoint()

        self.verify_leader_and_follower(cursor)
