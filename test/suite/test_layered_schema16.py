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

# Leader drop/recreate of a layered table within a single checkpoint pickup window.
#
# A follower can skip any number of leader checkpoints, so it may see a drop and a recreate of
# the same table in one pickup: the table is then present in both the local and the shared
# metadata, but with different btree IDs. The pickup must detect the ID change, discard the
# follower's state for the old incarnation and pick the table up as a new one. Also covers
# repeated drop/recreate cycles before a single pickup and a plain drop with no recreate.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggConfigMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema16(wttest.WiredTigerTestCase, DisaggConfigMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    stable = f'file:{test_name}.wt_stable'
    table_config = 'key_format=i,value_format=S'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def read_all(self, session):
        """Return the follower's view of the table as a key to value dict."""
        cursor = session.open_cursor(self.uri)
        result = {}
        while cursor.next() == 0:
            result[cursor.get_key()] = cursor.get_value()
        cursor.close()
        return result

    def write_all(self, value, commit_ts):
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for i in range(10):
            cursor[i] = value
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(commit_ts))
        cursor.close()

    def leader_checkpoint(self, stable_ts):
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(stable_ts) +
                                ',oldest_timestamp=' + self.timestamp_str(1))
        self.session.checkpoint()

    def uri_in_local_metadata(self, conn):
        session = conn.open_session('')
        exists = True
        try:
            c = session.open_cursor(self.stable)
            c.close()
        except wiredtiger.WiredTigerError:
            exists = False
        session.close()
        return exists

    def open_follower(self):
        conn = self.wiredtiger_open(
            'follower', self.extensionsConfig() + ',create,' + self.conn_config_follower)
        self.ignoreStdoutPattern('WT_VERB_RTS|(wiredtiger_open:.*WT_VERB_METADATA)')
        self.disagg_advance_checkpoint(conn)
        return conn, conn.open_session('')

    def test_drop_recreate_single_pickup(self):
        # Leader creates the table, writes the first generation of data, and checkpoints.
        self.session.create(self.uri, self.table_config)
        self.write_all('aaa', 10)
        self.leader_checkpoint(10)

        # Follower picks up the checkpoint and sees the first generation.
        conn_follow, session_follow = self.open_follower()
        self.assertEqual(self.read_all(session_follow), {i: 'aaa' for i in range(10)})

        # Leader drops and recreates the table under the same name, all before its next
        # checkpoint. The recreated table gets a fresh btree ID. (A drop may transiently EBUSY and
        # retry with a checkpoint; the follower skips any intermediate checkpoints anyway.)
        self.dropUntilSuccess(self.session, self.uri)
        self.session.create(self.uri, self.table_config)
        self.write_all('bbb', 20)
        self.leader_checkpoint(30)

        # Follower picks up a single checkpoint spanning the drop and the recreate: the table is
        # present on both sides but with different btree IDs. The pickup must replace the old
        # incarnation and the follower must read back the recreated data.
        self.disagg_advance_checkpoint(conn_follow)
        self.assertEqual(self.read_all(session_follow), {i: 'bbb' for i in range(10)})

        # Leader runs several drop/recreate cycles, checkpointing each one (a drop needs the
        # previous incarnation's data to be checkpointed), while the follower does not pick up any
        # of the intermediate checkpoints.
        for value, commit_ts in [('ccc', 40), ('ddd', 50)]:
            self.dropUntilSuccess(self.session, self.uri)
            self.session.create(self.uri, self.table_config)
            self.write_all(value, commit_ts)
            self.leader_checkpoint(commit_ts)

        # Follower catches up in one pickup and must see the last incarnation's data.
        self.disagg_advance_checkpoint(conn_follow)
        self.assertEqual(self.read_all(session_follow), {i: 'ddd' for i in range(10)})

        # Leader drops the table for good and checkpoints.
        self.dropUntilSuccess(self.session, self.uri)
        self.leader_checkpoint(60)

        # Follower picks up the drop. The table stays in its local metadata and stays readable:
        # absence from the shared metadata does not distinguish a dropped table from one that was
        # never published, and a local table can hold rows no checkpoint has captured, so the
        # pickup leaves it alone.
        #
        # FIXME-WT-17746: The table should disappear here, and stop being readable, once a dropped
        # table can be told apart from an unpublished one.
        self.disagg_advance_checkpoint(conn_follow)
        self.assertTrue(self.uri_in_local_metadata(conn_follow))
        self.assertEqual(self.read_all(session_follow), {i: 'ddd' for i in range(10)})

        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')

    # FIXME-WT-17746: A test covering the EBUSY retry when a pinned data handle blocks the discard
    # of a dropped table belonged here. The pickup no longer discards anything, so there is nothing
    # for a busy handle to block. Restore it along with the discard.
