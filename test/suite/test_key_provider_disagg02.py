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
import re
import wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtdataset import SimpleDataSet
import sqlite3

from wtscenario import make_scenarios

# test_key_provider_disagg02.py
#    Test crash scenarios
#
@disagg_test_class
class test_key_provider_disagg02(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_key_provider_disagg02', disagg_only = True)

    crash_points = [
        ('crash_before_key_rotation', dict(crash_point=0)),
        ('crash_during_key_rotation', dict(crash_point=1)),
        ('crash_after_key_rotation', dict(crash_point=2)),
    ]
    scenarios = make_scenarios(disagg_storages, crash_points)

    sqlite_meta_cursor = None
    nentries = 1000
    current_lsn = 0

    MAIN_KEK_PAGE_ID = 1
    EXPECTED_KEK_VERSION = 1

    uri = "layered:test_key_provider_disagg02"

    # Load the storage store extension.
    def conn_extensions(self, extlist):
        config = f'=(early_load=true,config=\"verbose=-1,key_expires=0\")'
        extlist.extension('test', "key_provider" + config)
        DisaggConfigMixin.conn_extensions(self, extlist)

    def subprocess_func(self):
        self.session.checkpoint(f"debug=(crash_point_key_provider={self.crash_point})") # Expected to fail


    def validate_persist_meta_file(self, expect_persisted=False):
        self.sqlite_meta_cursor.execute("SELECT * FROM pages ORDER BY lsn DESC LIMIT 1")
        result = self.sqlite_meta_cursor.fetchone()
        m = re.search(".*page_id=(\d+),lsn=(\d+).*version=(\d+)", result[-1].decode("utf-8"))

        if expect_persisted:
            self.assertTrue(m)
            if (m):
                page_id, lsn, version = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
                self.assertEqual(page_id, self.MAIN_KEK_PAGE_ID)
                self.assertEqual(lsn, self.current_lsn)
                self.assertEqual(version, self.EXPECTED_KEK_VERSION)

                self.current_lsn = lsn
        else:
            self.assertFalse(m)

    # Test simple inserts to a leader/follower
    def test_key_provider_disagg02(self):
        # Open turtle metadata sqlite database
        conn1 = sqlite3.connect("kv_home/pages_000001.db")
        self.sqlite_meta_cursor = conn1.cursor()

        # # Populate table.
        # ds = SimpleDataSet(self, self.uri, self.nentries)
        # ds.populate()
        # ds.check()
        # self.validate_persist_meta_file(expect_persisted=False)

        # # Crashing before key has ever persisted should not persist key provider in the shared turtle file.
        # subdir = 'SUBPROCESS'
        # [ignore_result, new_home_dir] = self.run_subprocess_function(subdir,
        #     'test_key_provider_disagg02.test_key_provider_disagg02.subprocess_func', silent=True)
        # self.validate_persist_meta_file(expect_persisted=False)

        # self.conn = self.setUpConnectionOpen(new_home_dir)
        # self.session = self.setUpSessionOpen(self.conn)

        # # Populate table.
        # ds = SimpleDataSet(self, self.uri, self.nentries)
        # ds.populate()
        # ds.check()

        # # Initiate checkpoint again to trigger key provider semantics.
        # self.session.checkpoint()
        # self.validate_persist_meta_file(expect_persisted=True)

        # # Crashing before key has ever persisted should not persist key provider in the shared turtle file.
        # subdir = 'SUBPROCESS'
        # [ignore_result, new_home_dir] = self.run_subprocess_function(subdir,
        #     'test_key_provider_disagg02.test_key_provider_disagg02.subprocess_func', silent=True)
        # self.validate_persist_meta_file()
