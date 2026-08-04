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

# A checkpoint pickup must not discard a local table, or its rows, on the strength of a record that
# has since been pruned.
#
# A pickup decides a table is gone by consulting the metadata operations queue: a local create that
# has not reached a shared checkpoint means the local state is newer than the checkpoint and must be
# kept. That queue is pruned once an entry's epoch is covered by a checkpoint, and the pruning does
# not consider whether the table's data is durable anywhere. A leader checkpoint can therefore carry
# an epoch that prunes the follower's create while still knowing nothing about the table itself,
# after which the pickup can no longer tell a live local table from a dropped one and discards it.
#
# The rows lost that way are the ones with the fewest copies: writes the follower holds that no
# checkpoint has captured yet.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages, DisaggSchemaEpochMixin
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_schema23(wttest.WiredTigerTestCase, DisaggSchemaEpochMixin):
    test_name = __qualname__
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader",lose_all_my_data=true)'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower",lose_all_my_data=true)'

    uri = f'layered:{test_name}'
    uri_warmup = f'layered:{test_name}_warmup'
    table_config = 'key_format=i,value_format=S'

    keys = range(10)

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def read_all(self, session, uri=None):
        """Return a table as a key to value dict."""
        cursor = session.open_cursor(self.uri if uri is None else uri)
        result = {}
        while cursor.next() == 0:
            result[cursor.get_key()] = cursor.get_value()
        cursor.close()
        return result

    def test_pickup_keeps_follower_only_rows(self):
        # Publishing requires a stable schema epoch, and every later publish sits above it.
        self.set_stable_epoch(1)

        # The follower needs an existing checkpoint before it can start, so give it a table that
        # takes no further part in the test.
        self.session.create(self.uri_warmup, self.table_config)
        self.publish(self.uri_warmup, 10)
        self.set_stable_epoch(10)
        self.leader_checkpoint(10)

        conn_follow, session_follow = self.open_follower_epoch(10)

        # The follower creates a table of its own and writes to it, standing in for a replicated
        # operation that the leader has not checkpointed yet. The leader knows nothing about it, so
        # the follower's ingest table is the only copy in the cluster.
        session_follow.create(self.uri, self.table_config)
        self.publish(self.uri, 20, session_follow)
        cursor = session_follow.open_cursor(self.uri)
        session_follow.begin_transaction()
        for i in self.keys:
            cursor[i] = 'follower'
        session_follow.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        cursor.close()

        expected = {i: 'follower' for i in self.keys}
        self.assertEqual(self.read_all(session_follow), expected)

        # The leader checkpoints at an epoch that covers the follower's create without carrying the
        # table. Picking it up prunes the follower's record of its own create.
        self.set_stable_epoch(20)
        self.leader_checkpoint(20)
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, self.uri))
        self.disagg_advance_checkpoint(conn_follow)
        self.assertEqual(self.read_all(session_follow), expected)

        # Another such checkpoint, and this time the pickup has no record left to tell it the table
        # is live. It must still not be discarded, because its rows exist nowhere else.
        self.leader_checkpoint(30)
        self.assertFalse(self.uri_in_shared_metadata(conn_follow, self.uri))
        self.disagg_advance_checkpoint(conn_follow)
        self.assertTrue(self.uri_in_local_metadata(conn_follow, self.uri),
          'the pickup discarded a live local table holding the only copy of its rows')
        self.assertEqual(self.read_all(session_follow), expected)

        session_follow.close()
        conn_follow.close('debug=(skip_checkpoint=true)')
