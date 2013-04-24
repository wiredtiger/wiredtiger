#!/usr/bin/env python
#
# Public Domain 2008-2013 WiredTiger, Inc.
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

import glob
import os.path
import time
import helper, wiredtiger, wttest
from wiredtiger import stat

# test_stat_log01.py
#    Statistics log
class test_stat_log01(wttest.WiredTigerTestCase):
    """
    Test statistics log
    """

    # Tests need to setup the connection in their own way.
    def setUpConnectionOpen(self, dir):
        return None

    def setUpSessionOpen(self, conn):
        return None

    def test_stats_log_default(self):
        self.conn = wiredtiger.wiredtiger_open(None, "create,statistics_log=(wait=1)")
        # Wait for the default interval, to ensure stats have been written.
        time.sleep(2)
        self.check_stats_file("WiredTigerStat")

    def test_stats_log_name(self):
        self.conn = wiredtiger.wiredtiger_open(None, "create,statistics_log=(wait=1,path=foo)")
        # Wait for the default interval, to ensure stats have been written.
        time.sleep(2)
        self.check_stats_file("foo")

    def check_stats_file(self, filename):
        if filename == "WiredTigerStat":
            files = glob.glob(filename + '.[0-9]*')
            self.assertTrue(files)
        else:
            self.assertTrue(os.path.isfile(filename))

if __name__ == '__main__':
    wttest.run()

