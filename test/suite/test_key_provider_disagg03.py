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
import re, os, time, subprocess, json
import wttest
from run import wt_builddir
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages, get_shard_id
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# test_key_provider_disagg03.py
#    Regression test for WT-17692. The bootstrap empty key-load was
#    being emitted on every checkpoint, which kept resetting the key
#    provider's rotation timer and prevented rotation from ever firing.
#    This test runs many checkpoints past the provider's expiry
#    threshold and asserts the persisted key page count grew.
@disagg_test_class
class test_key_provider_disagg03(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_key_provider_disagg03', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    nentries = 50000
    uri = "layered:test_key_provider_disagg03"

    WT_SPECIAL_PALI_TURTLE_FILE_ID = 2
    WT_SPECIAL_PALI_KEY_PROVIDER_FILE_ID = 26
    turtle_table = f'pages_{get_shard_id(WT_SPECIAL_PALI_TURTLE_FILE_ID):02d}.db'
    key_provider_table = f'pages_{get_shard_id(WT_SPECIAL_PALI_KEY_PROVIDER_FILE_ID):02d}.db'

    # Load the key provider with a one-second expiry so the second checkpoint
    # in this test will trigger a rotation.
    def conn_extensions(self, extlist):
        config = f'=(early_load=true,config=\"verbose=-1,key_expires=1\")'
        extlist.extension('test', "key_provider" + config)
        DisaggConfigMixin.conn_extensions(self, extlist)

    def fetch_key_provider_page_count(self, home="."):
        sqlite_exe = os.path.join(wt_builddir, 'sqlite3')
        database_home = os.path.join(home, 'kv_home', self.key_provider_table)
        result = subprocess.run(
            [sqlite_exe, '-json', database_home,
             f'SELECT COUNT(*) AS c FROM pages WHERE table_id={self.WT_SPECIAL_PALI_KEY_PROVIDER_FILE_ID};'],
            capture_output=True, text=True, check=True)
        return int(json.loads(result.stdout)[0]['c'])

    def test_key_rotation_across_checkpoints(self):
        if (self.ds_name != "palite"):
            self.skipTest("Must use PALite to verify contents")

        # Populate the table and take a first checkpoint to persist an
        # initial key.
        ds = SimpleDataSet(self, self.uri, self.nentries)
        ds.populate()
        ds.check()

        self.session.checkpoint()
        count_first = self.fetch_key_provider_page_count()

        cursor = self.session.open_cursor(self.uri, None)
        for cycle in range(40):
            for i in range(self.nentries):
                cursor[f'cycle-{cycle:04d}-key-{i:08d}'] = f'value-{i:08d}'
            self.session.checkpoint()
        cursor.close()

        count_second = self.fetch_key_provider_page_count()

        self.assertGreater(count_second, count_first,
            "Key rotation did not occur across checkpoints; "
            "the empty key-load on every checkpoint is suppressing rotation.")
