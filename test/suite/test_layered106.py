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
from helper_disagg import disagg_test_class

# test_layered106.py
# Verify automatic pickup of the latest disaggregated checkpoint at open time:
#  - test_leader_auto_pickup exercises the in-library leader-mode pickup.
#  - test_follower_auto_pickup_via_wt (added in a later commit) exercises the
#    util-driven follower pickup performed by the wt CLI.

@disagg_test_class
class test_layered106(wttest.WiredTigerTestCase):
    uri = 'layered:test_layered106'
    create_session_config = 'key_format=i,value_format=S'
    nrows = 100

    def conn_config(self):
        return 'disaggregated=(role="leader")'

    def test_leader_auto_pickup(self):
        # Leader: create a table, write some rows, checkpoint.
        self.session.create(self.uri, self.create_session_config)
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            cursor[i] = 'value' + str(i)
        cursor.close()
        self.session.checkpoint()

        # Step down so close does not write a shutdown checkpoint after ours.
        self.conn.reconfigure('disaggregated=(role="follower")')

        # Reopen as leader with no explicit checkpoint_meta. Leader-mode pickup
        # inside __wti_disagg_conn_config installs the latest checkpoint and
        # then begins the next checkpoint window so writes can resume.
        self.restart_without_local_files(
            config='disaggregated=(role="leader")',
            pickup_checkpoint=False)

        cursor = self.session.open_cursor(self.uri)
        seen = {k: v for k, v in cursor}
        cursor.close()
        self.assertEqual(len(seen), self.nrows,
            f"expected {self.nrows} rows after leader auto-pickup, got {len(seen)}")
        for i in range(self.nrows):
            self.assertEqual(seen[i], 'value' + str(i))

        # The new leader should be able to drive the next checkpoint: write a
        # few more rows and commit a checkpoint, exercising the begin_checkpoint
        # call that auto-pickup performs for leaders.
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nrows, self.nrows + 10):
            cursor[i] = 'value' + str(i)
        cursor.close()
        self.session.checkpoint()
