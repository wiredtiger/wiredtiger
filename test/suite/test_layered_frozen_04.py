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
from helper_layered_stepdown import LayeredStepdownMixin
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

# test_layered_frozen_04.py
#    Transactions in flight when writes stop: some commit after the final checkpoint and before the
#    demotion, some roll back, and one still open refuses the demotion until it resolves. The
#    demoted node, and a fresh follower after the node steps back up, see exactly the committed set.
@disagg_test_class
class test_layered_frozen_04(LayeredStepdownMixin, wttest.WiredTigerTestCase, suite_subprocess):
    conn_base_config = 'statistics=(all),precise_checkpoint=true,'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    # How the last transaction to resolve before the demotion ends.
    outcomes = [
        ('straggler_commits', dict(straggler_commits=True)),
        ('straggler_rolls_back', dict(straggler_commits=False)),
    ]
    scenarios = make_scenarios(disagg_storages, outcomes)

    uri = 'layered:test_layered_frozen_04'

    def begin_write(self, items):
        session = self.conn.open_session()
        cursor = session.open_cursor(self.uri)
        session.begin_transaction()
        for k, v in items.items():
            cursor[k] = v
        cursor.close()
        return session

    def in_flight_transactions_at_demote(self, attempt_while_open):
        self.set_global_ts(1, 1)
        self.session.create(self.uri, 'key_format=S,value_format=S')
        self.write_at(self.uri, {'base': 'v', 'upd': 'v'}, 10)

        # All of these begin before the final checkpoint and are still open across it.
        committer = self.begin_write({'c1': 'v', 'upd': 'committed'})
        aborter = self.begin_write({'a1': 'v', 'base': 'aborted'})
        straggler = self.begin_write({'s1': 'v'})
        reader = self.conn.open_session()
        reader.begin_transaction('read_timestamp=' + self.timestamp_str(10))
        rcur = reader.open_cursor(self.uri)
        self.assertEqual(rcur['upd'], 'v')

        self.checkpoint_at(20)
        committer.commit_transaction('commit_timestamp=' + self.timestamp_str(25))
        aborter.rollback_transaction()
        committer.close()
        aborter.close()

        expected = {'base': 'v', 'upd': 'committed', 'c1': 'v'}
        if attempt_while_open:
            # An open write transaction refuses the demotion and leaves the node a working leader.
            with self.expectedStderrPattern('write transaction is active'):
                self.assertRaisesException(wiredtiger.WiredTigerError,
                    lambda: self.conn.reconfigure('disaggregated=(role="follower")'))
            self.write_at(self.uri, {'after_refusal': 'v'}, 26)
            expected['after_refusal'] = 'v'

        if self.straggler_commits:
            straggler.commit_transaction('commit_timestamp=' + self.timestamp_str(27))
            expected['s1'] = 'v'
        else:
            straggler.rollback_transaction()
        straggler.close()

        self.demote()

        # The read-only transaction open across the demotion still reads its own snapshot.
        self.assertEqual(rcur['upd'], 'v')
        rcur.set_key('c1')
        self.assertEqual(rcur.search(), wiredtiger.WT_NOTFOUND)
        reader.rollback_transaction()
        reader.close()

        self.assertEqual(self.read_kvs_at(self.uri, 30), expected)
        self.assertEqual(self.read_kvs(self.uri), expected)
        self.assertEqual(self.read_kvs_at(self.uri, 20), {'base': 'v', 'upd': 'v'})
        self.assertEqual(self.read_kvs_at(self.ingest_uri(self.uri), 30), {})

        self.promote()
        self.checkpoint_at(30)
        conn_c = self.open_node('fresh', config=self.conn_base_config)
        self.disagg_advance_checkpoint_and_wait(conn_c)
        session_c = conn_c.open_session('')
        self.assertEqual(self.read_kvs_at(self.uri, 30, session_c), expected)
        session_c.close()
        conn_c.close('debug=(skip_checkpoint=true)')

    def test_in_flight_transactions_at_demote(self):
        self.in_flight_transactions_at_demote(False)

    def subprocess_active_writer_refuses_demote(self):
        self.in_flight_transactions_at_demote(True)

    # A refused demotion must leave a working leader. It runs in a subprocess because a refusal
    # that is not recoverable takes the process down; until the engine returns the refusal without
    # panicking, this fails.
    def test_active_writer_refuses_demote(self):
        self.set_global_ts(1, 1)
        rc, _ = self.run_subprocess_function('SUBPROCESS',
            'test_layered_frozen_04.test_layered_frozen_04.subprocess_active_writer_refuses_demote',
            silent=True, scenario=self.scenario_name)
        self.assertEqual(rc, 0, f'the refused demotion failed in the subprocess, rc={rc}')
