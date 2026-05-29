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
import os, wttest, subprocess
from run import wt_builddir
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages, get_shard_id
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# test_key_provider_disagg03.py
#    Exercise the push-mode set_key flow end-to-end: the test extension calls the
#    WT-installed set_key during init, and a subsequent checkpoint persists the pushed
#    bytes to the key-provider turtle page.
@disagg_test_class
class test_key_provider_disagg03(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_key_provider_disagg03', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    nentries = 1000

    WT_SPECIAL_PALI_KEY_PROVIDER_FILE_ID = 26
    MAIN_KEK_PAGE_ID = 1

    key_provider_table = f'pages_{get_shard_id(WT_SPECIAL_PALI_KEY_PROVIDER_FILE_ID):02d}.db'

    # The marker payload pushed by the test extension during initialization. It must
    # start with DEFAULT_KEY_DATA so kp_load_key/kp_set_key assertions still hold on
    # the priming path after reopen.
    PUSHED_KEY_BYTES = b"abcdefghijklmnopqrstuvwxyz"

    uri = "layered:test_key_provider_disagg03"

    def conn_extensions(self, extlist):
        config = '=(early_load=true,config=\"verbose=-1,key_expires=43200,version=1\")'
        extlist.extension('test', "key_provider" + config)
        DisaggConfigMixin.conn_extensions(self, extlist)

    # Use the builtin sqlite3 to match the Palite SQLite version.
    def sqlite_fetch_blob(self, home, database, sql_query):
        sqlite_exe = os.path.join(wt_builddir, 'sqlite3')
        database_home = os.path.join(home, 'kv_home', database)
        result = subprocess.run(
            [sqlite_exe, database_home, sql_query],
            capture_output=True,
            check=True
        )
        return result.stdout

    # Fetch the latest key-provider page raw bytes. Hex-encode in SQL so that binary
    # bytes survive the subprocess boundary, then decode here.
    def latest_key_provider_page(self, home="."):
        hex_bytes = self.sqlite_fetch_blob(
            home,
            self.key_provider_table,
            f'''SELECT hex(page_data) FROM pages
                WHERE table_id={self.WT_SPECIAL_PALI_KEY_PROVIDER_FILE_ID}
                  AND page_id={self.MAIN_KEK_PAGE_ID}
                ORDER BY lsn DESC LIMIT 1;'''
        )
        hex_str = hex_bytes.decode('ascii').strip()
        self.assertTrue(len(hex_str) > 0, "key-provider page is empty")
        return bytes.fromhex(hex_str)

    def assert_pushed_marker_persisted(self, home="."):
        page = self.latest_key_provider_page(home)
        # The page begins with WT_CRYPT_HEADER (16 bytes) followed by the payload.
        # Use a substring match to avoid duplicating the header layout in Python.
        self.assertIn(self.PUSHED_KEY_BYTES, page,
            "pushed key bytes not found in key-provider page payload")

    def test_key_provider_disagg03(self):
        if (self.ds_name != "palite"):
            self.skipTest("Must use PALite to verify contents")

        # Populate table so checkpoint has work to do.
        ds = SimpleDataSet(self, self.uri, self.nentries)
        ds.populate()
        ds.check()

        # Checkpoint persists the cached push-mode key to the turtle page.
        self.session.checkpoint()
        self.assert_pushed_marker_persisted()

        # Reopen and checkpoint again without an intervening explicit push to exercise
        # the load_key priming path; the new turtle page must still carry the marker.
        self.reopen_conn()
        first_row = ds.rows + 1
        ds.populate(first_row=first_row)
        ds.check()
        self.session.checkpoint()
        self.assert_pushed_marker_persisted()
