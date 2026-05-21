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
# Verify that a disagg follower with disaggregated.pickup_latest_checkpoint=true
# auto-applies the latest complete checkpoint from the page log at open time,
# without requiring an explicit checkpoint_meta.
#
# This is the WT-17338 path used by the `wt` CLI against the ext_pali page log:
# a brand-new WiredTiger home directory with no local files, just a page log
# pointing at an existing page service.

@disagg_test_class
class test_layered106(wttest.WiredTigerTestCase):
    uri = 'layered:test_layered106'
    create_session_config = 'key_format=i,value_format=S'
    nrows = 100

    def conn_config(self):
        return 'disaggregated=(role="leader")'

    def _follower_config(self, pickup_latest=False, checkpoint_meta=None):
        config = 'verbose=[disaggregated_storage:2],' \
                 'disaggregated=(role="follower"'
        if pickup_latest:
            config += ',pickup_latest_checkpoint=true'
        if checkpoint_meta is not None:
            config += f',checkpoint_meta="{checkpoint_meta}"'
        config += ')'
        return config

    def test_follower_auto_pickup(self):
        # Verbose disagg output from the new code path persists into teardown's
        # layered verify; suppress it so the test only checks behaviour.
        self.ignoreStdoutPattern(r'\[WT_VERB_DISAGGREGATED_STORAGE\]')

        # Leader: create a table, write some rows, checkpoint.
        self.session.create(self.uri, self.create_session_config)
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            cursor[i] = 'value' + str(i)
        cursor.close()
        self.session.checkpoint()

        # Move the local WiredTiger files aside and reopen as a follower
        # with the new knob and no explicit checkpoint_meta. This mirrors the
        # wt-CLI scenario: a fresh home directory, just a page log.
        # Without auto-pickup, the follower has no checkpoint loaded and the
        # layered table cursor returns zero rows. With auto-pickup, it pulls
        # the latest checkpoint from the page log and the leader's rows are
        # visible.
        self.restart_without_local_files(
            config=self._follower_config(pickup_latest=True),
            pickup_checkpoint=False)

        cursor = self.session.open_cursor(self.uri)
        seen = {k: v for k, v in cursor}
        cursor.close()

        self.assertEqual(len(seen), self.nrows,
            f"expected {self.nrows} rows after follower auto-pickup, got {len(seen)}")
        for i in range(self.nrows):
            self.assertEqual(seen[i], 'value' + str(i))
