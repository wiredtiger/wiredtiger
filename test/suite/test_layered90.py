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
from helper_disagg import disagg_test_class, gen_disagg_storages
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

# test_layered90.py
#    Regression test for WT-16823: assert that we never see an incomplete layered table.
#
# When a connection is opened, __metadata_clean_incomplete_table runs for every
# table: entry in the metadata. For layered tables it asserts that whenever a
# layered: entry is present, the corresponding file:*.wt_ingest entry also exists.
# On a leader, it additionally asserts that file:*.wt_stable exists. On a follower,
# stable table metadata is not required (followers don't create stable tables; they
# only appear after a checkpoint pickup from a leader), so missing stable is not an
# error.
#
# A follower creating a layered table naturally produces the "ingest but no stable"
# state without requiring any metadata corruption, so we use that to drive the tests.
#
# Tests:
#   test_leader_complete   - leader creates complete table; reopen as leader succeeds.
#   test_follower_complete - follower creates table (no stable); reopen as follower succeeds.
#   test_leader_missing_stable - follower creates table (no stable); reopen as LEADER aborts.

@disagg_test_class
class test_layered90(wttest.WiredTigerTestCase, suite_subprocess):

    conn_base_config = 'statistics=(all),'
    # String conn_config lets @disagg_test_class append disaggregated=(page_log=<impl>).
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_layered90', disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    basename = 'test_layered90'
    uri = 'table:' + basename
    nitems = 10

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _reopen_config(self, role):
        """Build a reopen config that preserves the page_log alongside the given role."""
        return (self.conn_base_config +
            f'disaggregated=(role="{role}"),disaggregated=(page_log={self.page_log()})')

    def _create_layered_table(self):
        """Create a layered table with a small data set and checkpoint."""
        self.session.create(
            self.uri,
            'key_format=S,value_format=S,block_manager=disagg,type=layered')
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            cursor[str(i)] = str(i)
        cursor.close()
        self.session.checkpoint()

    # -----------------------------------------------------------------------
    # Subprocess methods
    #
    # Each runs inside a child process launched by run_subprocess_function.
    #
    # Followers don't create stable table metadata when they create a layered
    # table  stable tables are only created by leaders or populated via
    # checkpoint pickup.  This gives us a natural "ingest present, stable
    # absent" state without needing to corrupt existing metadata.
    # -----------------------------------------------------------------------

    def subprocess_leader_complete(self):
        """Create a complete layered table as leader, then reopen as leader."""
        self._create_layered_table()
        self.reopen_conn(config=self._reopen_config('leader'))

    def subprocess_follower_complete(self):
        """Create a layered table as follower (no stable), then reopen as follower."""
        self.reopen_conn(config=self._reopen_config('follower'))
        self._create_layered_table()
        self.reopen_conn(config=self._reopen_config('follower'))

    def subprocess_leader_missing_stable(self):
        """Create a layered table as follower (no stable), then reopen as leader.

        The leader asserts that file:*.wt_stable exists in metadata, so this
        should abort.
        """
        self.reopen_conn(config=self._reopen_config('follower'))
        self._create_layered_table()
        self.reopen_conn(config=self._reopen_config('leader'))

    # -----------------------------------------------------------------------
    # Tests
    # -----------------------------------------------------------------------

    def test_leader_complete(self):
        """Leader with complete metadata reopens cleanly."""
        subdir = 'SUBPROCESS_leader_complete'
        func = 'test_layered90.test_layered90.subprocess_leader_complete'
        [returncode, _] = self.run_subprocess_function(subdir, func, silent=True)
        self.assertEqual(returncode, 0,
            'Expected subprocess to succeed: complete leader table should reopen cleanly')

    def test_follower_complete(self):
        """Follower with no stable metadata reopens cleanly."""
        subdir = 'SUBPROCESS_follower_complete'
        func = 'test_layered90.test_layered90.subprocess_follower_complete'
        [returncode, _] = self.run_subprocess_function(subdir, func, silent=True)
        self.assertEqual(returncode, 0,
            'Expected subprocess to succeed: missing stable metadata is valid on follower')

    def test_leader_missing_stable(self):
        """Leader finding a table with no stable metadata aborts."""
        subdir = 'SUBPROCESS_leader_missing_stable'
        func = 'test_layered90.test_layered90.subprocess_leader_missing_stable'
        [returncode, _] = self.run_subprocess_function(subdir, func, silent=True)
        self.assertNotEqual(returncode, 0,
            'Expected subprocess to abort: leader must have stable metadata for each layered table')
