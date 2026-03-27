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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_layered82.py
#    Verify that session.verify() on a follower with no stable timestamp
#    set does not produce a false positive error. Before the fix, the
#    verify timestamp validation compared cell timestamps against
#    stable=0 and incorrectly reported corruption.
@disagg_test_class
class test_layered82(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),'
    conn_config = 'disaggregated=(role="leader")'
    create_session_config = 'key_format=i,value_format=S'
    uri = 'layered:test_layered82'

    disagg_storages = gen_disagg_storages('test_layered82', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def test_follower_verify_no_stable_timestamp(self):
        # Leader: write timestamped data and checkpoint.
        self.session.create(self.uri, self.create_session_config)

        self.session.begin_transaction()
        cursor = self.session.open_cursor(self.uri)
        cursor[1] = 'value1'
        cursor.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))

        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10) +
                                ',oldest_timestamp=' + self.timestamp_str(5))
        self.session.checkpoint()

        # The follower starts without any local WiredTiger metadata, so its
        # recovery_timestamp stays WT_TS_NONE and has_stable_timestamp remains
        # false after recovery. The stable and oldest timestamps are only set
        # when the follower picks up the leader's checkpoint via reconfigure().
        conn_follow = self.wiredtiger_open(
            'follower',
            self.extensionsConfig() + self.conn_base_config +
            'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')

        # Create the table on the follower so the stable file exists.
        session_follow.create(self.uri, self.create_session_config)

        # After recovery but before picking up the checkpoint, the follower's stable and
        # oldest timestamps are both 0 because the local WiredTiger.wt has no
        # system:checkpoint entry.
        self.assertTimestampsEqual("0", conn_follow.query_timestamp('get=stable_timestamp'))
        self.assertTimestampsEqual("0", conn_follow.query_timestamp('get=oldest_timestamp'))

        # Follower picks up the leader's checkpoint. The stable and oldest timestamps
        # from the checkpoint metadata are propagated into the global transaction state,
        # so verify() can use the correct stable point.
        self.disagg_advance_checkpoint(conn_follow)

        # After picking up the checkpoint, the follower should have the leader's stable,
        # oldest and pinned timestamps propagated from the checkpoint metadata.
        self.assertTimestampsEqual(self.timestamp_str(10), conn_follow.query_timestamp('get=stable_timestamp'))
        self.assertTimestampsEqual(self.timestamp_str(5), conn_follow.query_timestamp('get=oldest_timestamp'))
        self.assertTimestampsEqual(self.timestamp_str(5), conn_follow.query_timestamp('get=pinned'))
        self.verifyUntilSuccess(session_follow, self.uri)

        session_follow.close()
        conn_follow.close()
