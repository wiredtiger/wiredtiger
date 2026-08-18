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
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

# Every checkpoint crash point must have exactly one outcome. A crash taken before the checkpoint
# transaction commits always loses the checkpoint. A crash taken after it keeps the checkpoint
# whenever connection logging made the committed metadata durable, because recovery replays it.
class test_checkpoint_crash01(wttest.WiredTigerTestCase, suite_subprocess):
    uri = 'table:test_checkpoint_crash01'

    # The numeric setting selects a tree in the per-tree phase, which always precedes the commit.
    # Both ends of its range are covered: the top of the range used to map past the last handle,
    # leaving the crash point unreached.
    crash_points = [
        ('before_checkpoint_commit',
            dict(debug_config='checkpoint_crash_trigger_point=before_checkpoint_commit',
                 post_commit=False)),
        ('before_metadata_sync',
            dict(debug_config='checkpoint_crash_trigger_point=before_metadata_sync',
                 post_commit=True)),
        ('numeric_first', dict(debug_config='checkpoint_crash_point=1', post_commit=False)),
        ('numeric_last', dict(debug_config='checkpoint_crash_point=1000', post_commit=False)),
    ]
    logging = [
        ('logged', dict(logging=True)),
        ('not_logged', dict(logging=False)),
    ]
    scenarios = make_scenarios(crash_points, logging)

    def conn_config(self):
        return 'log=(enabled=%s)' % ('true' if self.logging else 'false')

    def subprocess_func(self):
        # Leave one key in a complete checkpoint and one written only into the crashed checkpoint,
        # so that the outcome is visible as whether rollback to stable keeps the second key.
        self.session.create(self.uri, 'key_format=S,value_format=S,log=(enabled=false)')
        c = self.session.open_cursor(self.uri)

        self.session.begin_transaction()
        c['k1'] = 'v1'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        self.session.begin_transaction()
        c['k2'] = 'v2'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(20))

        # Expected to kill this process.
        self.session.checkpoint('debug=(%s)' % self.debug_config)

    def test_checkpoint_crash(self):
        self.conn.close()

        [returncode, home] = self.run_subprocess_function('SUBPROCESS',
            'test_checkpoint_crash01.test_checkpoint_crash01.subprocess_func',
            silent=True, scenario=self.scenario_number)

        # A clean exit means the crash point was never reached; any other signal means we crashed
        # somewhere we did not ask to, such as the checkpoint teardown assertion.
        self.assertEqual(returncode, -9)

        conn = wiredtiger.wiredtiger_open(home, self.conn_config())
        try:
            session = conn.open_session()
            c = session.open_cursor(self.uri)
            c.set_key('k1')
            self.assertEqual(c.search(), 0)
            c.set_key('k2')
            keeps_checkpoint = self.post_commit and self.logging
            self.assertEqual(c.search(), 0 if keeps_checkpoint else wiredtiger.WT_NOTFOUND)
        finally:
            conn.close()

    def test_checkpoint_crash_settings_conflict(self):
        # The two settings name crash points on opposite sides of the commit, so they disagree
        # about the outcome rather than refining each other.
        self.assertRaisesWithMessage(wiredtiger.WiredTigerError,
            lambda: self.session.checkpoint(
                'debug=(checkpoint_crash_point=500,'
                'checkpoint_crash_trigger_point=before_metadata_sync)'),
            '/mutually exclusive/')
