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
from helper import simulate_crash_restart
from wtdataset import SimpleDataSet
import sqlite3

from wtscenario import make_scenarios

# test_key_provider_disagg01.py
#    Test basic key provider scenarios.
@disagg_test_class
class test_key_provider_disagg01(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_key_provider_disagg01', disagg_only = True)

    crash_value = [
        ('reopen', dict(crash=False)),
        ('crash', dict(crash=True)),
    ]

    key_rotate = [
        # FIXME-WT-16055: Re-enable once startup or checkpoint pick-up.
        #('no_key_rotate', dict(key_expire=-1)), # Never perform key rotation
        ('key_rotate', dict(key_expire=0)) # Always perform key rotation
    ]

    disagg_storages = gen_disagg_storages('test_layered17', disagg_only = True)
    scenarios = make_scenarios(disagg_storages, key_rotate, crash_value)

    sqlite_meta_cursor = None
    sqlite_key_provider_cursor = None
    nentries = 1000
    current_lsn = 0

    MAIN_KEK_PAGE_ID = 1
    EXPECTED_KEK_VERSION = 1

    uri = "layered:test_key_provider_disagg01"

    # Load the key provider store extension.
    def conn_extensions(self, extlist):
        config = f'=(early_load=true,config=\"verbose=-1,key_expires={self.key_expire}\")'
        extlist.extension('test', "key_provider" + config)
        DisaggConfigMixin.conn_extensions(self, extlist)

    def validate_number_elements(self):
        self.sqlite_meta_cursor.execute("SELECT COUNT(*) FROM pages")
        (meta_count,) = self.sqlite_meta_cursor.fetchone()

        self.sqlite_key_provider_cursor.execute("SELECT COUNT(*) FROM pages")
        (key_provider_count,) = self.sqlite_key_provider_cursor.fetchone()
        if (self.key_expire == 0):
            self.assertEqual(key_provider_count, meta_count)
        else:
            self.assertGreaterEqual(key_provider_count, meta_count)

    def validate_meta_file(self):
        self.sqlite_meta_cursor.execute("SELECT * FROM pages ORDER BY lsn DESC LIMIT 1")
        result = self.sqlite_meta_cursor.fetchone()
        m = re.search(".*page_id=(\d+),lsn=(\d+).*version=(\d+)", result[-1].decode("utf-8"))

        self.assertTrue(m)
        if (m):
            page_id, lsn, version = (int(m.group(1)), int(m.group(2)), int(m.group(3)))
            self.assertEqual(page_id, self.MAIN_KEK_PAGE_ID)
            if (self.key_expire == 0):
                self.assertGreater(lsn, self.current_lsn)
            else:
                self.assertEqual(lsn, self.current_lsn)
            self.assertEqual(version, self.EXPECTED_KEK_VERSION)

            self.current_lsn = lsn

    def test_key_provider_disagg01(self):
        if (self.ds_name != "palite"):
            self.skipTest("Must use PALite to verify contents")

        # Open turtle metadata sqlite database
        conn1 = sqlite3.connect("kv_home/pages_000001.db")
        self.sqlite_meta_cursor = conn1.cursor()

        # Open key provider sqlite database
        conn2 = sqlite3.connect("kv_home/pages_000002.db")
        self.sqlite_key_provider_cursor = conn2.cursor()

        # Populate table.
        ds = SimpleDataSet(self, self.uri, self.nentries)
        ds.populate()
        ds.check()

        # Initiate checkpoint to trigger key provider semantics.
        self.session.checkpoint()

        # Initiate checkpoint again to trigger key provider semantics.
        self.session.checkpoint()

        first_row = ds.rows + 1
        ds.rows += 1000
        ds.populate(first_row=first_row)
        ds.check()

        # Validate that key persists after crash/restart.
        if (self.crash):
            simulate_crash_restart(self, ".", "RESTART")
        else:
            self.reopen_conn()
        self.validate_meta_file()
        self.validate_number_elements()



