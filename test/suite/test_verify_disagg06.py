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
#
# Verify file:WiredTigerShared.wt_stable after successive checkpoints,
# and the startup verify_metadata path after a restart, to make sure
# we don't get an EBUSY for any of them.

import wiredtiger, wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios

@disagg_test_class
class test_verify_disagg06(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only = True)
    scenarios = make_scenarios(disagg_storages)

    def try_verify(self):
        s = self.conn.open_session()
        s.verify('file:WiredTigerShared.wt_stable', None)
        s.close()

    def insert_data(self, uri, tag):
        self.session.create(uri, 'key_format=S,value_format=S')
        c = self.session.open_cursor(uri, None, None)
        for i in range(100):
            c['%s-k%d' % (tag, i)] = 'v' * 100
        c.close()

    def test_verify_disagg06(self):
        self.try_verify()
        for i in range(3):
            self.session.checkpoint()
            self.try_verify()

        self.insert_data('layered:t1', 'a')
        for i in range(3):
            self.session.checkpoint()
            self.try_verify()

        # New data, then verify without an intervening checkpoint.
        self.insert_data('layered:t2', 'b')
        self.try_verify()

        # Restart with metadata verification enabled.
        self.insert_data('layered:t3', 'c')
        self.session.checkpoint()
        self.reopen_conn(config = self.conn_config + ',verify_metadata=true' +
                         ',disaggregated=(page_log=%s)' % self.page_log())
        self.try_verify()
