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
# Verify the disaggregated.pickup_latest_checkpoint config: a follower with no
# explicit checkpoint_meta should auto-apply the latest checkpoint from the
# page log at open time (the `wt` utility scenario).

@disagg_test_class
class test_layered106(wttest.WiredTigerTestCase):
    uri = 'layered:test_layered106'
    create_session_config = 'key_format=i,value_format=S'
    nrows = 100

    def conn_config(self):
        return 'disaggregated=(role="leader")'

    # pickup_latest here is the C-level disaggregated.pickup_latest_checkpoint config,
    # which makes the connection itself resolve and apply the latest checkpoint at open
    # time. It is distinct from restart_without_local_files()'s pickup_checkpoint, which
    # is helper-side plumbing that step-downs the old conn, captures checkpoint_meta,
    # and reconfigures the reopened conn with that explicit meta.
    def _follower_config(self, pickup_latest=False, checkpoint_meta=None, verbose_disagg=False):
        config = ''
        if verbose_disagg:
            config += 'verbose=[disaggregated_storage:2],'
        config += 'disaggregated=(role="follower"'
        if pickup_latest:
            config += ',pickup_latest_checkpoint=true'
        if checkpoint_meta is not None:
            config += f',checkpoint_meta="{checkpoint_meta}"'
        config += ')'
        return config

    def test_follower_auto_pickup(self):
        # Leader: create a table, write some rows, checkpoint.
        self.session.create(self.uri, self.create_session_config)
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nrows):
            cursor[i] = 'value' + str(i)
        cursor.close()
        self.session.checkpoint()

        # Step down so close does not write a shutdown checkpoint after ours.
        self.conn.reconfigure('disaggregated=(role="follower")')

        # Reopen as follower with the config, no explicit checkpoint_meta;
        # auto-pickup should pull the latest checkpoint from the page log.
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

    def test_follower_auto_pickup_empty(self):
        # Suppress verify-side disagg verbose lines emitted during teardown.
        self.ignoreStdoutPattern(r'WT_SESSION\.verify: \[WT_VERB_DISAGGREGATED_STORAGE\]')

        # No writes; step down so close does not emit a shutdown checkpoint.
        self.conn.reconfigure('disaggregated=(role="follower")')

        # With no checkpoint on the page log, the new path should log at debug2
        # and the connection should still open cleanly.
        with self.expectedStdoutPattern(
            r'Did not find any complete checkpoint to pick up at startup'):
            self.restart_without_local_files(
                config=self._follower_config(
                    pickup_latest=True, verbose_disagg=True),
                pickup_checkpoint=False)

        # Connection is usable even with no checkpoint.
        session = self.conn.open_session()
        session.close()

    def test_reconfigure_silently_ignores(self):
        # pickup_latest_checkpoint is initial-open only; passing it to reconfigure
        # must be a silent no-op rather than an error.
        self.conn.reconfigure('disaggregated=(pickup_latest_checkpoint=true)')
