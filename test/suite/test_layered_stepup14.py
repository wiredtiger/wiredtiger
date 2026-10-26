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

# Step-up must not drain and free a follower truncate entry that is still
# referenced by an open application transaction.

import os, signal, threading, wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios


@disagg_test_class
class test_layered_stepup14(wttest.WiredTigerTestCase, suite_subprocess):
    test_name = __qualname__
    uri = f'layered:{test_name}'

    conn_base_config = ('statistics=(all),disaggregated=(lose_all_my_data=true),')
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def setup_follower(self):
        self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1))
        self.session.create(self.uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(self.uri)
        self.session.begin_transaction()
        for key in range(10):
            cursor[key] = 'value'
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(10))
        cursor.close()
        self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        self.session.checkpoint()

        self.conn.reconfigure('disaggregated=(role="follower")')

    def subprocess_uncommitted_follower_truncate(self):
        self.setup_follower()

        start = self.session.open_cursor(self.uri)
        stop = self.session.open_cursor(self.uri)
        start.set_key(2)
        stop.set_key(8)
        self.session.begin_transaction()
        self.session.truncate(None, start, stop, None)
        start.close()
        stop.close()

        self.conn.reconfigure('disaggregated=(role="leader")')
        self.fail('step-up succeeded with an uncommitted follower truncate')

    def subprocess_committed_follower_truncate(self):
        self.setup_follower()
        self.conn.reconfigure('timing_stress_for_test=[disagg_truncate_before_list_snapshot]')

        start = self.session.open_cursor(self.uri)
        stop = self.session.open_cursor(self.uri)
        start.set_key(2)
        stop.set_key(8)
        self.session.begin_transaction()
        self.session.truncate(None, start, stop, None)
        start.close()
        stop.close()

        stats_session = self.conn.open_session('')
        step_up_errors = []

        def step_up():
            try:
                self.conn.reconfigure('disaggregated=(role="leader")')
            except Exception as error:
                step_up_errors.append(error)

        step_up_thread = threading.Thread(target=step_up, daemon=True)
        step_up_thread.start()
        step_up_stat = wiredtiger.stat.conn.disagg_step_up_in_progress
        try:
            self.assertStatGreaterSoon(
              step_up_stat, 0, session=stats_session, timeout=10, msg='step-up did not start')
            self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
            self.assertGreater(
              self.get_stat(step_up_stat, session=stats_session), 0,
              'truncate was not committed while step-up was in progress')
        finally:
            stats_session.close()
            step_up_thread.join(timeout=30)

        self.assertFalse(step_up_thread.is_alive(), 'step-up did not finish')
        self.assertEqual(step_up_errors, [])

    def subprocess_truncate_started_during_stepup(self):
        self.setup_follower()

        start = self.session.open_cursor(self.uri)
        stop = self.session.open_cursor(self.uri)
        start.set_key(2)
        stop.set_key(8)
        self.assertEqual(start.search_near(), 0)
        self.assertEqual(stop.search_near(), 0)

        self.conn.reconfigure('timing_stress_for_test=[disagg_role_transition]')
        step_up_thread = threading.Thread(
          target=lambda: self.conn.reconfigure('disaggregated=(role="leader")'), daemon=True)
        step_up_thread.start()
        self.assertStatGreaterSoon(
          wiredtiger.stat.conn.disagg_step_up_in_progress, 0, timeout=10,
          msg='step-up did not start')

        self.session.begin_transaction()
        self.session.truncate(None, start, stop, None)
        start.close()
        stop.close()
        step_up_thread.join(timeout=10)
        self.assertFalse(step_up_thread.is_alive(), 'step-up did not finish')
        self.fail('truncate succeeded while step-up was in progress')

    def subprocess_truncate_insert_during_stepup(self):
        self.setup_follower()

        start = self.session.open_cursor(self.uri)
        stop = self.session.open_cursor(self.uri)
        start.set_key(0)
        stop.set_key(0)
        self.session.begin_transaction()
        self.session.truncate(None, start, stop, None)
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(20))
        start.close()
        stop.close()

        self.conn.reconfigure(
          'timing_stress_for_test=[disagg_truncate_entry_insert,'
          'disagg_truncate_before_list_snapshot]')
        search_calls = wiredtiger.stat.dsrc.layered_truncate_list_search_calls
        search_calls_before = self.get_stat(search_calls, uri=self.uri)
        truncate_returned = threading.Event()
        rollback = threading.Event()

        def truncate_and_wait():
            session = self.conn.open_session('')
            start = stop = None
            try:
                start = session.open_cursor(self.uri)
                stop = session.open_cursor(self.uri)
                start.set_key(2)
                stop.set_key(8)
                session.begin_transaction()
                session.truncate(None, start, stop, None)
                truncate_returned.set()
                rollback.wait(timeout=30)
                session.rollback_transaction()
            finally:
                if start is not None:
                    start.close()
                if stop is not None:
                    stop.close()
                session.close()

        truncate_thread = threading.Thread(target=truncate_and_wait)
        truncate_thread.start()

        try:
            self.assertStatGreaterSoon(
              search_calls, search_calls_before, uri=self.uri, timeout=10,
              msg='follower truncate did not reach truncate-list conflict detection')
            self.conn.reconfigure('disaggregated=(role="leader")')
            self.assertTrue(truncate_returned.wait(timeout=10),
              'truncate insertion did not complete during the step-up drain')
            self.fail('truncate-list insertion succeeded during step-up')
        finally:
            rollback.set()
            truncate_thread.join(timeout=10)

        self.assertFalse(truncate_thread.is_alive(), 'truncate rollback did not finish')

    def test_step_up_asserts_on_uncommitted_follower_truncate(self):
        rc, home = self.run_subprocess_function(
            'SUBPROCESS',
            'test_layered_stepup14.test_layered_stepup14.subprocess_uncommitted_follower_truncate',
            silent=True,
            scenario=self.scenario_name)
        self.assert_crashed(rc, signal.SIGABRT)
        with open(os.path.join(home, 'stderr.txt')) as err:
            self.assertIn(
              'an uncommitted follower truncate was found during the step-up drain', err.read())

    def test_step_up_succeeds_with_committed_follower_truncate(self):
        rc, _ = self.run_subprocess_function(
          'SUBPROCESS',
          'test_layered_stepup14.test_layered_stepup14.subprocess_committed_follower_truncate',
          silent=True,
          scenario=self.scenario_name)
        self.assertEqual(rc, 0)

    def test_truncate_started_during_stepup_aborts(self):
        rc, home = self.run_subprocess_function(
          'SUBPROCESS',
          'test_layered_stepup14.test_layered_stepup14.subprocess_truncate_started_during_stepup',
          silent=True,
          scenario=self.scenario_name)
        self.assert_crashed(rc, signal.SIGABRT)
        with open(os.path.join(home, 'stderr.txt')) as err:
            self.assertIn('truncate is not supported during step-up', err.read())

    def test_truncate_insert_during_stepup_aborts(self):
        rc, home = self.run_subprocess_function(
          'SUBPROCESS',
          'test_layered_stepup14.test_layered_stepup14.subprocess_truncate_insert_during_stepup',
          silent=True,
          scenario=self.scenario_name)
        self.assert_crashed(rc, signal.SIGABRT)
        with open(os.path.join(home, 'stderr.txt')) as err:
            self.assertIn('truncates are not permitted during step-up', err.read())
