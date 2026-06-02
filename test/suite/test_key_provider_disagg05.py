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

import errno
import wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# test_key_provider_disagg05.py
#    Push-mode key provider: drive set_key from a normal user thread (no
#    checkpoint, no panic) via the extension's test hook, and verify both
#    timestamp validation paths return EINVAL.
@disagg_test_class
class test_key_provider_disagg05(wttest.WiredTigerTestCase):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),'
    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages('test_key_provider_disagg05', disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    def conn_extensions(self, extlist):
        config = '=(early_load=true,config=\"verbose=-1,version=1\")'
        extlist.extension('test', "key_provider" + config)
        DisaggConfigMixin.conn_extensions(self, extlist)

    def test_set_key_validation(self):
        # Opening the connection registers the extension which caches the WT_KEY_PROVIDER
        # pointer used by the test hook. We never run a checkpoint here, so the only set_key
        # calls are the ones this test issues directly through wiredtiger_test_kp_push_key.
        push = wiredtiger.wiredtiger_test_kp_push_key

        # Monotonic check: first push at ts=10 accepted; equal or lower timestamps rejected.
        self.assertEqual(push(10), 0)
        self.assertEqual(push(10), errno.EINVAL)
        self.assertEqual(push(5), errno.EINVAL)
        self.assertEqual(push(20), 0)

        # Stable-timestamp check: pushes must be strictly above the stable timestamp.
        self.conn.set_timestamp("stable_timestamp=64")    # 0x64 = 100
        self.assertEqual(push(100), errno.EINVAL)         # Equal to stable.
        self.assertEqual(push(50), errno.EINVAL)          # Below stable.
        self.assertEqual(push(101), 0)                    # Above stable and last pushed.

        # Suppress the four expected rejection diagnostics from the captured stderr.
        self.skipStderrLinesWithPattern("set_key timestamp .* must be strictly greater than")
