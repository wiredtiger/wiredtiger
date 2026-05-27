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

import wttest, wiredtiger
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat

# test_layered_fast_truncate19.py
#   Validate the debug_mode.disagg_slow_truncate_follower connection config:
#   parsing/reconfigure smoke checks, plus a behaviour test that proves the
#   knob actually selects the slow per-record vs fast range-delete path on a
#   layered-table follower. The slow path walks the range and calls
#   cursor->remove() once per key (incrementing layered_curs_remove); the
#   fast path uses the truncate-list and, when the ingest table is empty,
#   does no per-key cursor work.

@disagg_test_class
class test_layered_fast_truncate19(wttest.WiredTigerTestCase):

    conn_config = 'disaggregated=(role="leader"),statistics=(all)'
    uri = 'layered:test_layered_fast_truncate19'
    table_config = 'key_format=i,value_format=S'
    nitems = 500
    trunc_lo, trunc_hi = 100, 400

    disagg_storages = gen_disagg_storages('test_layered_fast_truncate19', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    # --- config parsing / reconfigure smoke checks ---

    def test_open_accepts_true(self):
        self.reopen_conn(config='debug_mode=(disagg_slow_truncate_follower=true)')

    def test_open_accepts_false(self):
        self.reopen_conn(config='debug_mode=(disagg_slow_truncate_follower=false)')

    def test_open_default(self):
        # Default omits the knob entirely.
        self.reopen_conn(config='')

    def test_reconfigure_toggle(self):
        self.conn.reconfigure('debug_mode=(disagg_slow_truncate_follower=true)')
        self.conn.reconfigure('debug_mode=(disagg_slow_truncate_follower=false)')

    def test_reconfigure_rejects_invalid(self):
        with self.expectedStderrPattern("expected a boolean"):
            self.assertRaisesException(
                wiredtiger.WiredTigerError,
                lambda: self.conn.reconfigure(
                    'debug_mode=(disagg_slow_truncate_follower=bogus)'))

    # --- behaviour: slow vs fast follower truncate path ---

    def get_stat(self, stat_key):
        s = self.conn.open_session('')
        val = s.open_cursor('statistics:')[stat_key][2]
        s.close()
        return val

    def populate_on_leader(self):
        self.session.create(self.uri, self.table_config)
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            self.session.begin_transaction()
            cursor[i] = 'v'
            self.session.commit_transaction()
        cursor.close()
        self.session.checkpoint()

    def reopen_as_follower(self, slow):
        knob = 'true' if slow else 'false'
        follower_config = (
            f'disaggregated=(role="follower",'
            f'checkpoint_meta="{self.disagg_get_complete_checkpoint_meta()}"),'
            f'statistics=(all),'
            f'debug_mode=(disagg_slow_truncate_follower={knob})')
        self.reopen_conn(config=follower_config)

    def truncate_middle_range(self):
        c1 = self.session.open_cursor(self.uri)
        c1.set_key(self.trunc_lo)
        c2 = self.session.open_cursor(self.uri)
        c2.set_key(self.trunc_hi)
        self.session.begin_transaction()
        self.session.truncate(None, c1, c2, None)
        self.session.commit_transaction()
        c1.close()
        c2.close()

    def test_slow_path_calls_cursor_remove_per_key(self):
        self.populate_on_leader()
        self.reopen_as_follower(slow=True)

        before = self.get_stat(stat.conn.layered_curs_remove)
        self.truncate_middle_range()
        after = self.get_stat(stat.conn.layered_curs_remove)

        expected = self.trunc_hi - self.trunc_lo + 1
        self.assertEqual(after - before, expected,
            f'slow path should call cursor->remove() {expected} times')

    def test_fast_path_skips_cursor_remove(self):
        self.populate_on_leader()
        self.reopen_as_follower(slow=False)

        before = self.get_stat(stat.conn.layered_curs_remove)
        self.truncate_middle_range()
        after = self.get_stat(stat.conn.layered_curs_remove)

        self.assertEqual(after, before,
            'fast path should not call cursor->remove() (ingest table is empty)')
