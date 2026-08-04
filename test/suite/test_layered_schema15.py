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

# Leader create/drop/create of the same layered table, observed by a pure follower through
# checkpoint pickups only, using plain schema operations and checkpoints (no publish API and no
# schema epochs).
#
# The follower picks up a checkpoint between the drop and the recreate: the drop pickup must
# discard the table's local metadata, and the recreate pickup must bring in the new incarnation
# so the follower reads the recreated data.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggConfigMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema15(wttest.WiredTigerTestCase, DisaggConfigMixin):
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

    def uri_in_shared_metadata(self, conn):
        session = conn.open_session('')
        cursor = session.open_cursor('file:WiredTigerShared.wt_stable', None, None)
        cursor.set_key(self.stable)
        found = cursor.search() == 0
        cursor.close()
        session.close()
        return found

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

    def test_id_conflict_no_epoch(self):
        # No publish, no schema epochs: the leader drives create/drop/create with plain schema
        # operations, and the follower only picks up checkpoints.

        # Leader creates the table, writes the first generation of data, and checkpoints.
        self.session.create(self.uri, self.table_config)
        self.write_all('aaa', 10)
        self.leader_checkpoint(10)

        # Follower picks up the checkpoint and sees the first generation.
        conn_follow, session_follow = self.open_follower()
        self.assertTrue(self.uri_in_local_metadata(conn_follow))
        self.assertEqual(self.read_all(session_follow), {i: 'aaa' for i in range(10)})

        # Leader drops the table and checkpoints, flushing the remove out of shared metadata.
        self.session.drop(self.uri)
        self.leader_checkpoint(20)

        # Follower picks up the post-drop checkpoint. The table is gone from the shared metadata,
        # but the pickup leaves the follower's local entries in place: absence from the shared
        # metadata does not distinguish a dropped table from one that was never published, and a
        # local table can hold rows no checkpoint has captured.
        #
        # FIXME-WT-17746: The local entries should go once a dropped table can be told apart from
        # an unpublished one.
        self.disagg_advance_checkpoint(conn_follow)
        self.assertFalse(self.uri_in_shared_metadata(conn_follow))
        self.assertTrue(self.uri_in_local_metadata(conn_follow))

        # Leader recreates the table under the same name. It gets a fresh btree ID and a new
        # generation of data.
        self.session.create(self.uri, self.table_config)
        self.write_all('bbb', 30)
        self.leader_checkpoint(40)

        # Follower picks up the recreate as a new table and must read back the recreated data.
        self.disagg_advance_checkpoint(conn_follow)
        self.assertTrue(self.uri_in_local_metadata(conn_follow))
        self.assertEqual(self.read_all(session_follow), {i: 'bbb' for i in range(10)})

        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')
