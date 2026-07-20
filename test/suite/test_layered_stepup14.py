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

import sys
import wttest
from helper_disagg import disagg_test_class

# test_layered_stepup14.py
#
# A node stepping up on checkpoints whose metadata predates the write
# generation high-water mark (written by an older version) cannot adopt
# the mark at pickup, and must instead derive it from its local metadata
# when it becomes leader. Without it, the old leader's transaction ids --
# persisted while another transaction was running -- are read as this
# node's own current ids, and the rows are invisible to its snapshots.
#
# The old leader writes its checkpoint metadata without the high-water
# mark, the second node picks it up and steps up, and every row must be
# visible to the new leader.
@disagg_test_class
class test_layered_stepup14(wttest.WiredTigerTestCase):
    test_name = __qualname__
    uri = f'layered:{test_name}'

    conn_base_config = 'statistics=(all),disaggregated=(lose_all_my_data=true),'
    # The first leader omits the write generation high-water mark from its
    # checkpoint metadata, like a leader from before the mark existed.
    conn_config = conn_base_config \
        + 'timing_stress_for_test=[disagg_legacy_checkpoint_metadata],' \
        + 'disaggregated=(role="leader")'

    create_session_config = 'key_format=S,value_format=S'

    nitems = 5000

    def test_step_up_on_legacy_checkpoint(self):
        if sys.platform.startswith('darwin'):
            return

        self.session.create(self.uri, self.create_session_config)

        conn_follow = self.wiredtiger_open('follower',
            self.extensionsConfig() + ',create,' + self.conn_base_config
            + 'disaggregated=(role="follower")')
        session_follow = conn_follow.open_session('')
        session_follow.create(self.uri, self.create_session_config)

        # Populate under an open transaction so the populating transactions'
        # ids are persisted rather than cleared.
        pin_session = self.conn.open_session('')
        pin_session.begin_transaction()

        value = 'v' * 20
        base_ts = 10
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(1, self.nitems + 1):
            self.session.begin_transaction()
            cursor[str(i)] = value
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(base_ts))
        cursor.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(base_ts))
        self.session.checkpoint()

        pin_session.rollback_transaction()
        pin_session.close()

        # A step-down/step-up cycle starts a new run on the old leader, so
        # its next checkpoint records a run write generation well past the
        # small generations of a freshly started node: the new leader below
        # cannot recognize the checkpoint as cross-run from its own
        # generations alone, it needs the scanned high-water mark.
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.conn.reconfigure('disaggregated=(role="leader")')

        update_ts = base_ts + 10
        cursor = self.session.open_cursor(self.uri, None, None)
        for i in range(1, 100):
            self.session.begin_transaction()
            cursor[str(i)] = value + 'x'
            self.session.commit_transaction(
                'commit_timestamp=' + self.timestamp_str(update_ts))
        cursor.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(update_ts))
        self.session.checkpoint()

        # The second node picks up the checkpoint and becomes the leader.
        self.disagg_switch_follower_and_leader(conn_follow, self.conn)

        # The old leader's transaction ids must be treated as belonging to
        # an earlier run: every row is committed and stable, so all of them
        # are visible to the new leader.
        cursor = session_follow.open_cursor(self.uri, None, None)
        count = 0
        while cursor.next() == 0:
            count += 1
        cursor.close()
        self.assertEqual(count, self.nitems)

        # The new leader's own checkpoint completes over the inherited data.
        session_follow.checkpoint()

        session_follow.close()
        conn_follow.close()
