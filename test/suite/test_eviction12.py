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

import threading, time, wttest
from wiredtiger import stat
from wtscenario import make_scenarios
from wtthread import checkpoint_thread


class test_eviction12(wttest.WiredTigerTestCase):
    scenarios = make_scenarios([
        ('default', dict(config='cache_size=100MB', expected=9500)),
        ('explicit_zero', dict(config='cache_size=100MB,eviction_updates_trigger=0',
                               expected=1000)),
        ('explicit_value', dict(config='cache_size=100MB,eviction_updates_trigger=30',
                                expected=3000)),
        ('overall_cap', dict(config='cache_size=100MB,eviction_trigger=85', expected=8500)),
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


class test_eviction12_reconfigure(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),cache_size=100MB'

    def check_trigger(self, expected):
        cursor = self.session.open_cursor('statistics:')
        self.assertEqual(cursor[stat.conn.eviction_threshold_updates_trigger][2], expected)
        cursor.close()

    def test_reconfigure(self):
        self.check_trigger(9500)
        self.conn.reconfigure('cache_size=200MB')
        self.check_trigger(9500)
        self.conn.reconfigure('eviction_updates_trigger=0')
        self.check_trigger(1000)
        self.conn.reconfigure('eviction_dirty_trigger=30')
        self.check_trigger(1500)
        self.conn.reconfigure('eviction_updates_trigger=95')
        self.check_trigger(9500)
        self.conn.reconfigure('eviction_updates_trigger=0')
        self.check_trigger(1500)
        self.conn.reconfigure('cache_size=100MB')
        self.check_trigger(1500)
        self.conn.reconfigure('eviction_updates_trigger=30')
        self.check_trigger(3000)
        self.conn.reconfigure('cache_size=200MB')
        self.check_trigger(3000)

    def test_user_config_file(self):
        self.close_conn()
        with open('WiredTiger.config', 'w') as config:
            config.write('eviction_updates_trigger=0\n')
        self.open_conn(config='statistics=(all),cache_size=100MB')
        self.check_trigger(1000)
        self.conn.reconfigure('cache_size=200MB')
        self.check_trigger(1000)


class test_eviction12_shared_cache(wttest.WiredTigerTestCase):
    conn_config = 'statistics=(all),shared_cache=(name=pool,size=200MB)'

    def check_trigger(self, expected):
        cursor = self.session.open_cursor('statistics:')
        self.assertEqual(cursor[stat.conn.eviction_threshold_updates_trigger][2], expected)
        cursor.close()

    def test_shared_cache_trigger(self):
        self.check_trigger(9500)
        self.conn.reconfigure('eviction_updates_trigger=0')
        self.check_trigger(1000)
        self.conn.reconfigure('eviction_updates_trigger=30')
        self.check_trigger(3000)


class test_eviction12_checkpoint(wttest.WiredTigerTestCase):
    scenarios = make_scenarios([
        ('default', dict(trigger_config='', expected=9500)),
        ('explicit', dict(trigger_config=',eviction_updates_trigger=10', expected=1000)),
    ])

    def conn_config(self):
        return ('cache_size=100MB,statistics=(all),precise_checkpoint=true' +
                self.trigger_config)

    def test_checkpoint_trigger(self):
        self.session.create('table:checkpoint_trigger', 'key_format=S,value_format=S')
        self.conn.set_timestamp('stable_timestamp=1')
        cursor = self.session.open_cursor('table:checkpoint_trigger')
        cursor['key'] = 'value'
        cursor.close()
        self.conn.reconfigure('timing_stress_for_test=[checkpoint_stop]')

        done = threading.Event()
        ckpt = checkpoint_thread(self.conn, done, checkpoint_count_max=1)
        ckpt.start()
        try:
            deadline = time.monotonic() + 60
            while self.get_stat(stat.conn.checkpoint_stop_stress_active) == 0:
                self.assertTrue(ckpt.is_alive(), 'checkpoint exited before reaching stop')
                self.assertLess(time.monotonic(), deadline, 'checkpoint did not reach stop')
                time.sleep(0.1)
            self.assertEqual(self.get_stat(stat.conn.eviction_threshold_updates_trigger),
                             self.expected)
            self.assertEqual(self.get_stat(stat.conn.eviction_threshold_dirty_trigger), 2600)
        finally:
            done.set()
            ckpt.join()
            self.conn.reconfigure('timing_stress_for_test=[]')
        self.assertEqual(ckpt.checkpoint_count, 1)
        self.assertEqual(self.get_stat(stat.conn.eviction_threshold_updates_trigger), self.expected)
        self.assertEqual(self.get_stat(stat.conn.eviction_threshold_dirty_trigger), 2000)
