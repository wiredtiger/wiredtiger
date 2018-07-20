#!/usr/bin/env python
#
# Public Domain 2014-2018 MongoDB, Inc.
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

import fnmatch, os
import wiredtiger, wttest
from wtscenario import make_scenarios

# test_config07.py
#    Test that log files extend as configured and as documented.
class test_config07(wttest.WiredTigerTestCase):
    uri = "table:test"
    entries = 100000
    K = 1024

    extend_len = [
        ('default', dict(log_extend_len='()', expected_log_size = 100 * K)),
        ('empty', dict(log_extend_len='(log=)', expected_log_size = 100 * K)),
        ('disable', dict(log_extend_len='(log=0)', expected_log_size = 128)),
        ('20K', dict(log_extend_len='(log=20K)', expected_log_size = 20 * K)),
    ]

    scenarios = make_scenarios(extend_len)

    def populate(self):
        cur = self.session.open_cursor(self.uri, None, None)
        for i in range(0, self.entries):
            cur[i] = i
        cur.close()

    def checkLogFileSize(self, size):
        logs = fnmatch.filter(os.listdir('.'), "*Prep*")
        f = logs[-1]
        file_size = os.stat(f).st_size
        self.assertEqual(size, file_size)

    def test_log_extend(self):
        self.conn.close()

        config = 'log=(enabled,file_max=100K),file_extend=' + self.log_extend_len
        # Create a table, insert data in it to trigger log file writes.
        configarg = 'create,statistics=(fast)' + ',' + config
        self.conn = self.wiredtiger_open('.', configarg)
        self.session = self.conn.open_session(None)
        self.session.create(self.uri, 'key_format=i,value_format=i')
        self.populate()
        self.session.checkpoint()

        self.checkLogFileSize(self.expected_log_size)

if __name__ == '__main__':
    wttest.run()
