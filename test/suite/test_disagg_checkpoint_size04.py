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

import re, os, wttest, subprocess, json
from run import wt_builddir
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# test_disagg_checkpoint_size04.py
#       Test that KEK table size is included in database size tracking.
@disagg_test_class
class test_disagg_checkpoint_size04(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'

    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_disagg_checkpoint_size04', disagg_only = True)
    scenarios = make_scenarios(disagg_storages, [('kek_size', dict())])

    MAIN_KEK_PAGE_ID = 1
    EXPECTED_KEK_VERSION = 1
    current_lsn = 0

    uri = "layered:test_disagg_checkpoint_size04"

    def conn_extensions(self, extlist):
        config = '=(early_load=true,config="verbose=-1,key_expires=0")'
        extlist.extension('test', "key_provider" + config)
        DisaggConfigMixin.conn_extensions(self, extlist)

    # Use sqlite to grab information for read/write validation. Use the builtin sqlite3 to
    # match Palites SQLite version; some system SQLite builds are too old and may fail.
    def sqlite_fetch_information(self, home, database, sql_query):
        sqlite_exe = os.path.join(wt_builddir, "sqlite3")
        database_home = os.path.join(home, 'kv_home', database)
        result = subprocess.run(
            [sqlite_exe, "-json", database_home, sql_query],
            capture_output=True,
            text=True,
            check=True
        )
        result_data = json.loads(result.stdout)
        return result_data[0]

    # Verify KEK rotation occurred
    def validate_meta_file(self, home="."):
        result = self.sqlite_fetch_information(home, "pages_000001.db", "SELECT * FROM pages ORDER BY lsn DESC LIMIT 1;")
        m = re.search(".*page_id=(\d+),lsn=(\d+).*version=(\d+)", result['page_data'])

        self.assertTrue(m)
        if (m):
            page_id, lsn, version = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
            self.assertEqual(page_id, self.MAIN_KEK_PAGE_ID)
            self.assertGreater(lsn, self.current_lsn)
            self.assertEqual(version, self.EXPECTED_KEK_VERSION)

            self.current_lsn = lsn

    def get_database_size(self):
        match = re.search(r'database_size=(\d+)', self.disagg_get_complete_checkpoint_meta())
        assert(match)
        return int(match.group(1))

    def test_kek_size_included(self):
        if self.ds_name != "palite":
            self.skipTest("Must use PALite to verify KEK table contents")

        # Create and populate table
        ds = SimpleDataSet(self, self.uri, 100)
        ds.populate()
        ds.check()

        # Initial checkpoint to establish baseline
        self.session.checkpoint()
        initial_size = self.get_database_size()
        # Verify initial KEK state
        self.validate_meta_file()

        # Force key rotation with second checkpoint
        self.session.checkpoint()
        size_after_rotation = self.get_database_size()
        # Verify KEK rotation occurred
        self.validate_meta_file()

        # Verify database size increased due to KEK table growth
        self.assertGreater(size_after_rotation, initial_size,
            f"Database size should increase after KEK rotation: {initial_size} -> {size_after_rotation}")
