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

import wttest
from wiredtiger import stat
from wtscenario import make_scenarios


class test_eviction11(wttest.WiredTigerTestCase):
    scenarios = make_scenarios([
        ('small', dict(config='cache_size=1GB', expected=9500)),
        ('below', dict(config='cache_size=19GB', expected=9500)),
        ('boundary', dict(config='cache_size=20GB', expected=9500)),
        ('above', dict(config='cache_size=21474836481', expected=9500)),
        ('opt_out', dict(config='cache_size=1GB,eviction=(updates_trigger_auto_high=false)',
                         expected=1000)),
        ('explicit_zero', dict(config='cache_size=1GB,eviction_updates_trigger=0',
                               expected=1000)),
        ('explicit_value', dict(config='cache_size=21GB,eviction_updates_trigger=30',
                                expected=3000)),
        ('explicit_with_opt_out',
         dict(config='cache_size=21GB,eviction=(updates_trigger_auto_high=false),'
                     'eviction_updates_trigger=30', expected=3000)),
        ('overall_cap', dict(config='cache_size=21GB,eviction_trigger=85', expected=8500)),
    ])

    def conn_config(self):
        return 'statistics=(all),' + self.config

    def check_trigger(self, expected):
        cursor = self.session.open_cursor('statistics:')
        self.assertEqual(cursor[stat.conn.eviction_threshold_updates_trigger][2], expected)
        cursor.close()

    def test_initial_and_reopen(self):
        self.check_trigger(self.expected)
        self.reopen_conn()
        self.check_trigger(self.expected)
        if ',eviction_updates_trigger=' in self.config:
            self.reopen_conn(config='statistics=(all),' + self.config.split(',')[0])
            self.check_trigger(self.expected)

    def test_readonly_reopen(self):
        self.reopen_conn(config='statistics=(all),readonly=true,' + self.config)
        self.check_trigger(self.expected)


class test_eviction11_reconfigure(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),cache_size=20GB'

    def check_trigger(self, expected):
        cursor = self.session.open_cursor('statistics:')
        self.assertEqual(cursor[stat.conn.eviction_threshold_updates_trigger][2], expected)
        cursor.close()

    def test_reconfigure(self):
        self.check_trigger(9500)
        self.conn.reconfigure('cache_size=21GB')
        self.check_trigger(9500)
        self.conn.reconfigure('eviction=(updates_trigger_auto_high=false)')
        self.check_trigger(1000)
        self.conn.reconfigure('eviction_dirty_trigger=30')
        self.check_trigger(1500)
        self.conn.reconfigure('eviction=(updates_trigger_auto_high=true)')
        self.check_trigger(9500)
        self.conn.reconfigure('cache_size=20GB')
        self.check_trigger(9500)
        self.conn.reconfigure('cache_size=21GB,eviction_updates_trigger=0')
        self.check_trigger(1500)
        self.conn.reconfigure('cache_size=19GB')
        self.conn.reconfigure('cache_size=21GB')
        self.check_trigger(1500)
        self.conn.reconfigure('eviction_updates_trigger=30')
        self.check_trigger(3000)
        self.conn.reconfigure('cache_size=19GB')
        self.conn.reconfigure('cache_size=21GB')
        self.check_trigger(3000)

    def test_user_config_file(self):
        self.close_conn()
        with open('WiredTiger.config', 'w') as config:
            config.write('eviction_updates_trigger=0\n')
        self.open_conn(config='statistics=(all),cache_size=21GB')
        self.check_trigger(1000)
        self.conn.reconfigure('cache_size=22GB')
        self.check_trigger(1000)
