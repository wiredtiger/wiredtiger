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
#
# test_timestamp13.py
#   Timestamps: session query_timestamp
#

import random
import wiredtiger, wttest
from suite_subprocess import suite_subprocess
from wiredtiger import stat
from wtscenario import make_scenarios

class test_read_once(wttest.WiredTigerTestCase, suite_subprocess):
    tablename = 'test_read_once'
    uri = 'table:' + tablename

    def ConnectionOpen(self):
        '''
        Specify a custom ConnectionOpen method to flush WT's cache during a test
        after inserting data.
        '''
        self.home = '.'

        conn_config = 'cache_size=1M,statistics=(all)'
        self.conn = wiredtiger.wiredtiger_open(self.home, conn_config)
        self.session = self.conn.open_session()

    def test_read_once(self):
        # This test is configured to use 1MB of cache. It will insert 20
        # documents, each 100KB. Manipulate the table to result in one page per
        # document.
        self.session.create(self.uri,
                            'key_format=i,value_format=S,leaf_page_max=108K,leaf_value_max=108K')

        cursor = self.session.open_cursor(self.uri, None, None)
        for key in range(0, 20):
            cursor[key] = '1' * (100 * 1024)
        cursor.close()

        # Restart the database to clear the cache.
        self.conn.close()
        self.ConnectionOpen()

        # Table scan ~2MB of data when only given 1MB of cache.
        cursor = self.session.open_cursor(self.uri, None, "read_once=true")
        for key, value in cursor:
            pass
        cursor.close()

        # Although 2MB of data was touched, paging in from disk never had to
        # perform eviction. Additionally, the eviction server was not enlisted
        # in performing eviction.
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        self.assertEqual(stat_cursor[stat.conn.cache_eviction_get_ref][2], 0)
        self.assertEqual(stat_cursor[stat.conn.cache_eviction_server_evicting][2], 0)
