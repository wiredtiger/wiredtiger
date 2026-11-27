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

# test_layered_ingest_skip_open01.py
# Creating a layered table on a leader must not open the ingest constituent.
# The ingest btree is in-memory, so a newly created one has nothing to check and is not opened.

import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat

@disagg_test_class
class test_layered_ingest_skip_open01(wttest.WiredTigerTestCase):
    # Long idle time so the sweep server cannot close the handle during the test.
    conn_config = 'statistics=(all),' + \
        'file_manager=(close_idle_time=100000,close_scan_interval=100000),' + \
        'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def handle_count(self):
        return self.get_stat(stat.conn.dh_conn_handle_count)

    def test_ingest_not_opened_on_create(self):
        before = self.handle_count()
        self.session.create('layered:ingest_skip_open01', 'key_format=S,value_format=S')
        after = self.handle_count()

        # Only the stable constituent's handle is created. The in-memory ingest constituent is
        # not opened at all.
        self.assertEqual(after - before, 1)
