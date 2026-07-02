#!/usr/bin/env python
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

import re, wiredtiger, wttest
from helper_disagg import DisaggConfigMixin, gen_disagg_storages
from wtscenario import make_scenarios

# test_wiredtiger_repair01.py
#    Exercise the wiredtiger_repair() API for config-error paths and fetch_database_size. Both run
#    in non-disaggregated and disaggregated scenarios; the disagg scenario cross-validates the
#    reported size against the disagg_database_size connection statistic.
class test_wiredtiger_repair01(wttest.WiredTigerTestCase, DisaggConfigMixin):
    conn_base_config = 'statistics=(all),'
    scenarios = make_scenarios(gen_disagg_storages(disagg_only=False))

    def conn_config(self):
        if not self.is_disagg_scenario():
            return self.conn_base_config
        return self.conn_base_config + \
            'disaggregated=(page_log=%s,role="leader",lose_all_my_data=true),' % self.ds_name

    def conn_extensions(self, extlist):
        DisaggConfigMixin.conn_extensions(self, extlist)

    def repair(self, config):
        return wiredtiger.wiredtiger_repair(self.conn, config)

    def populate(self):
        uri = 'layered:tbl' if self.is_disagg_scenario() else 'table:tbl'
        self.session.create(uri, 'key_format=S,value_format=S')
        cursor = self.session.open_cursor(uri)
        for i in range(1000):
            cursor['key%06d' % i] = 'v' * 100
        cursor.close()
        self.session.checkpoint()

    def reported_size(self):
        result = self.repair('fetch_database_size=(local=true)')
        return int(re.search(r': (\d+)$', result).group(1))

    def test_config_errors(self):
        self.assertEqual(self.repair(''), 'wiredtiger_repair: empty config')
        self.assertIn('No command found', self.repair('uri="table:tbl"'))

    def test_fetch_database_size(self):
        self.populate()

        reported = self.reported_size()

        if not self.is_disagg_scenario():
            self.assertEqual(reported, 0)
            return

        # Cross-validate against the disagg_database_size connection statistic.
        stat_size = self.get_stat(wiredtiger.stat.conn.disagg_database_size)
        self.assertEqual(reported, stat_size)
        self.assertGreater(reported, 0)
