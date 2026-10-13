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

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

# A leader uses precise checkpoints. Opening a leader without them warns once, at connection open.
@disagg_test_class
class test_layered_config16(wttest.WiredTigerTestCase):
    test_name = __qualname__
    conn_base_config = ',statistics=(all),'
    warning = 'disaggregated storage requires precise checkpoints'

    conn_modes = [
        ('fuzzy_leader', dict(role='leader', precise=False, expect_warning=True)),
        ('precise_leader', dict(role='leader', precise=True, expect_warning=False)),
        ('fuzzy_follower', dict(role='follower', precise=False, expect_warning=False)),
    ]
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=True), conn_modes)

    def conn_config(self):
        config = self.extensionsConfig() + self.conn_base_config + \
            f'disaggregated=(role="{self.role}")'
        if self.precise:
            config += ',precise_checkpoint=true'
        return config

    def test_open_warning(self):
        # A precise checkpoint, including the one taken when the connection closes, needs a stable
        # timestamp.
        stable_ts = 'oldest_timestamp=1,stable_timestamp=1'
        self.conn.set_timestamp(stable_ts)
        # Reopen so the open-time warning is produced within the test, then read it back.
        self.close_conn()
        self.cleanStdout()
        self.open_conn()
        self.conn.set_timestamp(stable_ts)
        output = self.readStdout()
        if self.expect_warning:
            self.assertIn(self.warning, output)
        else:
            self.assertNotIn(self.warning, output)
