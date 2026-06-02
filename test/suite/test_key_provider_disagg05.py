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

import os
import wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from suite_subprocess import suite_subprocess
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# test_key_provider_disagg05.py
#    Push-mode key provider: verify set_key rejects a push whose timestamp is
#    not strictly greater than the stable timestamp. The rejection triggers
#    WT_PANIC + abort, so the failing checkpoint runs in a subprocess and the
#    parent test asserts the expected diagnostic.
@disagg_test_class
class test_key_provider_disagg05(wttest.WiredTigerTestCase, suite_subprocess):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_key_provider_disagg05', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    uri = "layered:test_key_provider_disagg05"

    # force_push_ts=10 makes every push from the extension use the same fixed timestamp.
    def conn_extensions(self, extlist):
        config = '=(early_load=true,config=\"verbose=-1,version=1,force_push_ts=10\")'
        extlist.extension('test', "key_provider" + config)
        DisaggConfigMixin.conn_extensions(self, extlist)

    def subprocess_func(self):
        ds = SimpleDataSet(self, self.uri, 10)
        ds.populate()
        # stable=100 (0x64) > force_push_ts=10; the next push fails the stable check.
        self.conn.set_timestamp("stable_timestamp=64")
        self.session.checkpoint()

    def test_rejects_push_below_stable_timestamp(self):
        if self.ds_name != "palite":
            self.skipTest("Must use PALite")

        self.conn.close()
        subdir = 'SUBPROCESS'
        returncode, new_home_dir = self.run_subprocess_function(
            subdir,
            'test_key_provider_disagg05.test_key_provider_disagg05.subprocess_func',
            silent=True)
        self.assertNotEqual(returncode, 0,
            "Subprocess was expected to abort on an invalid set_key push")
        with open(os.path.join(new_home_dir, "stderr.txt"), "r") as f:
            stderr = f.read()
        self.assertIn('must be strictly greater than the stable timestamp', stderr)
        self.assertIn("WiredTiger library panic", stderr)
