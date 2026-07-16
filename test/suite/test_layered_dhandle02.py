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

# test_layered_dhandle02.py
# Simple table: + type=layered cursors must not retain a table: dhandle. The hot
# path opens layered: from metadata, and sweep must not pin table: to the
# immortal layered handle.

import time
import wttest
from helper_disagg import disagg_test_class, gen_disagg_storages
from wtscenario import make_scenarios
from wiredtiger import stat

@disagg_test_class
class test_layered_dhandle02(wttest.WiredTigerTestCase):
    uri = 'table:test_layered_dhandle02'
    create_config = 'key_format=S,value_format=S,block_manager=disagg,type=layered'

    conn_base_config = 'create,statistics=(all),' + \
        'file_manager=(close_idle_time=1,close_scan_interval=1,close_handle_minimum=0),'
    conn_config = conn_base_config + 'disaggregated=(role="leader")'

    disagg_storages = gen_disagg_storages(disagg_only=True)
    scenarios = make_scenarios(disagg_storages)

    def get_stat(self, field):
        c = self.session.open_cursor('statistics:', None, 'statistics=(fast)')
        value = c[field][2]
        c.close()
        return value

    def wait_for_table_dhandles(self, expect, timeout=20):
        for _ in range(timeout * 2):
            # Drop session-cached references that would keep the table handle alive.
            self.session.reset()
            if self.get_stat(stat.conn.dh_conn_handle_table_count) == expect:
                return
            time.sleep(0.5)
        self.fail('timed out waiting for dh_conn_handle_table_count == %d (have %d)' % (
            expect, self.get_stat(stat.conn.dh_conn_handle_table_count)))

    def test_simple_layered_elides_table_dhandle(self):
        table_before = self.get_stat(stat.conn.dh_conn_handle_table_count)
        layered_before = self.get_stat(stat.conn.dh_conn_handle_layered_count)

        # Create on a disposable session so its dhandle cache is not retained.
        s = self.conn.open_session()
        s.create(self.uri, self.create_config)
        s.close()

        c = self.session.open_cursor(self.uri)
        self.assertEqual(c.uri, self.uri)
        c['a'] = 'b'
        self.assertEqual(c['a'], 'b')
        c.close()

        # Hot path should have opened the layered handle without leaving a table handle in use.
        self.assertEqual(
            self.get_stat(stat.conn.dh_conn_handle_layered_count), layered_before + 1)

        # Create briefly opens a table handle; sweep must be allowed to reclaim it even while
        # the layered handle remains open.
        self.wait_for_table_dhandles(table_before)

        # Cursor still works through table: after the table dhandle is gone.
        c = self.session.open_cursor(self.uri)
        self.assertEqual(c['a'], 'b')
        c['c'] = 'd'
        c.close()
        self.wait_for_table_dhandles(table_before)
        self.assertEqual(
            self.get_stat(stat.conn.dh_conn_handle_layered_count), layered_before + 1)
