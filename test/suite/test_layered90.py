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
# layered: entry is present, the corresponding file:*.wt_ingest and
# file:*.wt_stable entries also exist.
#
# Tests:
#   test_complete_metadata  - happy path: all entries present, recovery passes cleanly.
#   test_missing_ingest     - file:*.wt_ingest removed; expects abort on reopen.
#   test_missing_stable     - file:*.wt_stable removed; expects abort on reopen.
#   test_missing_both       - both file entries removed; expects abort on reopen.

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

    def _create_layered_table(self):
        """Create a complete layered table with a small data set and checkpoint."""
        self.session.create(
            self.uri,
            'key_format=S,value_format=S,block_manager=disagg,type=layered')
        cursor = self.session.open_cursor(self.uri)
        for i in range(self.nitems):
            cursor[str(i)] = str(i)
        cursor.close()
        self.session.checkpoint()

    def _remove_metadata_key(self, key):
        """Directly remove a single entry from the metadata store."""
        meta = self.session.open_cursor('metadata:create')
        meta.set_key(key)
        meta.remove()
        meta.close()

    def _check_metadata(self, keys_present):
        """Assert that every key in the list exists in the metadata."""
        meta = self.session.open_cursor('metadata:')
        for key in keys_present:
            meta.set_key(key)
            self.assertEqual(meta.search(), 0, 'Expected metadata key missing: ' + key)
        meta.close()

    # -----------------------------------------------------------------------
    # Subprocess methods
    #
    # Each of these runs inside a child process launched by run_subprocess_function.
    # They create a complete layered table, remove one or more metadata entries to
    # simulate an incomplete state, close the connection cleanly (persisting the
    # corrupt metadata to disk), then reopen it.  The assertion in
    # __metadata_clean_incomplete_table fires and the process aborts with a
    # non-zero exit code.
    # -----------------------------------------------------------------------

    def subprocess_missing_ingest(self):
        self._create_layered_table()
        self._remove_metadata_key('file:' + self.basename + '.wt_ingest')
        self.close_conn()
        self.open_conn()   # assertion fires, process aborts

    def subprocess_missing_stable(self):
        self._create_layered_table()
        self._remove_metadata_key('file:' + self.basename + '.wt_stable')
        self.close_conn()
        self.open_conn()   # assertion fires, process aborts

    def subprocess_missing_both(self):
        self._create_layered_table()
        self._remove_metadata_key('file:' + self.basename + '.wt_ingest')
        self._remove_metadata_key('file:' + self.basename + '.wt_stable')
        self.close_conn()
        self.open_conn()   # assertion fires, process aborts

    # -----------------------------------------------------------------------
    # Tests
    # -----------------------------------------------------------------------

    def test_complete_metadata(self):
        """A fully-created layered table passes the recovery assertion cleanly."""
        self._create_layered_table()

        # Reopen as follower.  During open, __wt_txn_recover calls
        # __recovery_file_scan -> __metadata_clean_incomplete_table for every
        # table: entry.  For the layered table it asserts that both
        # file:*.wt_ingest and file:*.wt_stable exist.
        self.reopen_conn(
            config=self.conn_base_config + 'disaggregated=(role="follower")')

        self._check_metadata([
            self.uri,
            'colgroup:' + self.basename,
            'layered:' + self.basename,
            'file:' + self.basename + '.wt_ingest',
            'file:' + self.basename + '.wt_stable',
        ])

        cursor = self.session.open_cursor(self.uri)
        count = 0
        for (k, v) in cursor:
            self.assertEqual(k, v)
            count += 1
        cursor.close()
        self.assertEqual(count, self.nitems)

    def test_missing_ingest(self):
        """Removing file:*.wt_ingest causes an abort on the next open."""
        subdir = 'SUBPROCESS_missing_ingest'
        func = 'test_layered90.test_layered90.subprocess_missing_ingest'
        [returncode, _] = self.run_subprocess_function(subdir, func, silent=True)
        self.assertNotEqual(returncode, 0,
            'Expected subprocess to abort due to incomplete layered table metadata')

    def test_missing_stable(self):
        """Removing file:*.wt_stable causes an abort on the next open."""
        subdir = 'SUBPROCESS_missing_stable'
        func = 'test_layered90.test_layered90.subprocess_missing_stable'
        [returncode, _] = self.run_subprocess_function(subdir, func, silent=True)
        self.assertNotEqual(returncode, 0,
            'Expected subprocess to abort due to incomplete layered table metadata')

    def test_missing_both(self):
        """Removing both file entries causes an abort on the next open."""
        subdir = 'SUBPROCESS_missing_both'
        func = 'test_layered90.test_layered90.subprocess_missing_both'
        [returncode, _] = self.run_subprocess_function(subdir, func, silent=True)
        self.assertNotEqual(returncode, 0,
            'Expected subprocess to abort due to incomplete layered table metadata')
