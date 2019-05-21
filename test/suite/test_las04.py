#!/usr/bin/env python
#
# Public Domain 2014-2019 MongoDB, Inc.
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
# test_las04.py
#   Test the file_max configuration and reconfiguration for the lookaside table.
#

import wiredtiger, wttest
from wtscenario import make_scenarios

class test_las04(wttest.WiredTigerTestCase):

    uri = 'table:las_04'
    init_file_max_values = [
        ('default', dict(init_file_max=None)),
        ('zero', dict(init_file_max='0')),
        ('non-zero', dict(init_file_max='100MB'))
    ]
    reconfig_file_max_values = [
        ('zero', dict(reconfig_file_max='0')),
        ('too-low', dict(reconfig_file_max='99MB')),
        ('non-zero', dict(reconfig_file_max='100MB'))
    ]
    scenarios = make_scenarios(init_file_max_values, reconfig_file_max_values)
    # Taken from misc.h.
    WT_MB = 1048576

    def conn_config(self):
        config = 'statistics=(fast)'
        if self.init_file_max is not None:
            config += ',cache_overflow=(file_max={})'.format(self.init_file_max)
        return config

    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def config_value_to_expected(self, config_value):
        if config_value is None:
            return 0
        elif config_value == '0':
            return 0
        elif config_value == '100MB':
            return self.WT_MB * 100
        else:
            raise Exception('unrecognised config value')

    def test_las(self):
        self.session.create(self.uri, 'key_format=S,value_format=S')

        self.assertEqual(
            self.get_stat(wiredtiger.stat.conn.cache_lookaside_ondisk_max),
            self.config_value_to_expected(self.init_file_max))

        # In the 99MB case, we're expecting an error here.
        with self.expectedStderrPattern(''):
            try:
                self.conn.reconfigure('cache_overflow=(file_max={})'.format(
                    self.reconfig_file_max))
            except wiredtiger.WiredTigerError:
                # Ensure that we raised an error on the invalid value only.
                self.assertEqual(self.reconfig_file_max, '99MB')
                return

        self.assertEqual(
            self.get_stat(wiredtiger.stat.conn.cache_lookaside_ondisk_max),
            self.config_value_to_expected(self.reconfig_file_max))

if __name__ == '__main__':
    wttest.run()
