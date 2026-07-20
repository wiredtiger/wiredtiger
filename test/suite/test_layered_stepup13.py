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

# test_layered_stepup13.py
#   A follower that steps up must be sitting on the truly latest completed checkpoint, not
#   merely the latest one it happens to know about. Otherwise it would start writing new
#   checkpoints whose history omits a checkpoint that another node already advanced past,
#   corrupting the checkpoint chain. Any step-up failure already aborts the process (see
#   the panic from __wti_disagg_conn_config on step-up failure), so the negative case runs
#   in a subprocess to catch the abort without killing the test runner.

import os, signal, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

@disagg_test_class
class test_layered_stepup13(wttest.WiredTigerTestCase, suite_subprocess):
    test_name = __qualname__
    uri = f"layered:{test_name}"

    conn_base_config = 'statistics=(all),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'
    conn_config_follower = conn_base_config + 'disaggregated=(role="follower")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def _write_and_checkpoint(self, key, value):
        cursor = self.session.open_cursor(self.uri)
        cursor[key] = value
        cursor.close()
        self.session.checkpoint()

    def _stale_stepup_scenario(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

        # Leader takes the first checkpoint (ckpt1).
        self._write_and_checkpoint('a', 'ckpt1_a')

        # Follower joins and picks up ckpt1.
        conn_follow = self.wiredtiger_open(
            'follower', self.extensionsConfig() + ',create,' + self.conn_config_follower)
        self.disagg_advance_checkpoint(conn_follow)

        # Leader advances past the follower: a second checkpoint (ckpt2) completes without
        # the follower knowing about it.
        self._write_and_checkpoint('b', 'ckpt2_b')

        # The follower is still pinned to ckpt1. Stepping up now must be refused (and, per
        # the existing step-up failure handling, aborts this process).
        conn_follow.reconfigure('disaggregated=(role="leader")')

    def subprocess_stale_stepup(self):
        self._stale_stepup_scenario()

    def test_stepup_rejects_stale_checkpoint(self):
        rc, new_home_dir = self.run_subprocess_function(
            'SUBPROCESS',
            'test_layered_stepup13.test_layered_stepup13.subprocess_stale_stepup',
            silent=True)
        self.assertEqual(rc, -signal.SIGABRT,
            f'expected process to abort (rc={-signal.SIGABRT}) but got rc={rc}')
        with open(os.path.join(new_home_dir, 'stderr.txt')) as f:
            err = f.read()
        self.assertIn('Refusing to step up to the leader role', err,
            f'expected the stale-checkpoint message in subprocess stderr, got:\n{err}')

    def test_stepup_succeeds_after_catching_up(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self._write_and_checkpoint('a', 'ckpt1_a')

        conn_follow = self.wiredtiger_open(
            'follower', self.extensionsConfig() + ',create,' + self.conn_config_follower)
        self.disagg_advance_checkpoint(conn_follow)

        self._write_and_checkpoint('b', 'ckpt2_b')

        # Catch up to ckpt2, then step down before closing: leaving the connection in the
        # leader role would write one more (shutdown) checkpoint on close, putting the
        # follower behind again.
        self.disagg_advance_checkpoint(conn_follow)
        self.conn.reconfigure('disaggregated=(role="follower")')
        self.session.close()
        self.close_conn()
        conn_follow.reconfigure('disaggregated=(role="leader")')

        session_follow = conn_follow.open_session('')
        cursor_follow = session_follow.open_cursor(self.uri)
        self.assertEqual(cursor_follow['a'], 'ckpt1_a')
        self.assertEqual(cursor_follow['b'], 'ckpt2_b')
        cursor_follow.close()
        session_follow.close()
        conn_follow.close()
