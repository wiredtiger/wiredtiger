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

import os
import helper, wiredtiger, wttest
from suite_subprocess import suite_subprocess
from wtscenario import make_scenarios

# test_prefetch02.py
#    Run a scenario which will benefit from pre-fetching and ensure that
#    pre-fetching is running properly by checking various statistics.

class test_prefetch02(wttest.WiredTigerTestCase, suite_subprocess):
    new_dir = 'new.dir'
    uri = 'file:test_prefetch02'

    config_options = [
        ('config_a', dict(conn_cfg='prefetch=(available=true,default=true),statistics=(all)', 
                            session_cfg='')),
        ('config_b', dict(conn_cfg='prefetch=(available=true,default=false),statistics=(all)', 
                            session_cfg='prefetch=(enabled=true)')),
    ]

    prefetch_scenarios = [
        ('forward-traversal', dict(prefetch_scenario='forward-traversal', scenario_type='traversal')),
        ('backward-traversal', dict(prefetch_scenario='backward-traversal', scenario_type='traversal')),
        # ('verify', dict(prefetch_scenario='verify', scenario_type='verify')),
        # ('verify-utility', dict(prefetch_scenario='verify-utility', scenario_type='verify')),
    ]

    scenarios = make_scenarios(config_options, prefetch_scenarios)

    def get_stat(self, stat, session_name):
        stat_cursor = session_name.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def check_prefetch_stats(self, session_name):
        prefetch_skips = self.get_stat(wiredtiger.stat.conn.block_prefetch_skipped, session_name)
        self.assertGreaterEqual(prefetch_skips, 1)
        prefetch_attempts = self.get_stat(wiredtiger.stat.conn.block_prefetch_attempts, session_name)
        self.assertGreaterEqual(prefetch_attempts, 1)
        pages_queued = self.get_stat(wiredtiger.stat.conn.block_prefetch_pages_queued, session_name)
        self.assertGreaterEqual(pages_queued, 1)
        prefetch_pages_read = self.get_stat(wiredtiger.stat.conn.block_prefetch_pages_read, session_name)
        self.assertGreaterEqual(prefetch_pages_read, 1)

    def test_prefetch_scenarios(self):
        os.mkdir(self.new_dir)
        helper.copy_wiredtiger_home(self, '.', self.new_dir)

        new_conn = self.wiredtiger_open(self.new_dir, self.conn_cfg)
        s = new_conn.open_session(self.session_cfg)
        s.create(self.uri, 'key_format=i,value_format=i')
        c1 = s.open_cursor(self.uri)
        s.begin_transaction()
        for i in range(100000):
            c1[i] = i
        c1.close()
        s.commit_transaction()
        s.checkpoint()

        if self.scenario_type == 'traversal':
            c2 = s.open_cursor(self.uri)
            while True:
                if self.prefetch_scenario == 'forward-traversal':
                    ret = c2.next()
                else:
                    ret = c2.prev()
                if ret != 0:
                    break
            c2.close()
            self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
            self.check_prefetch_stats(s)

if __name__ == '__main__':
    wttest.run()
