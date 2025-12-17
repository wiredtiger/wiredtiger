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

import wiredtiger, wttest
import time
from wiredtiger import stat
from wtdataset import SimpleDataSet, simple_key
from wtscenario import make_scenarios
from helper import WiredTigerStat, WiredTigerCursor

# test_stat14.py
# Check block reusable percentage works as expected.

class test_stat14(wttest.WiredTigerTestCase):
    uri = 'table:test_stat14'

    conn_config = 'statistics=(all),statistics_log=(wait=1,json=true,on_close=true)'
    # Try to occupy more pages for given keys
    long_v = 's'*1000
    
    table_cnt = 4

    table_scale = [
        ('small_table', dict(is_small = True, key_cnt = int(1.3e3))),
        ('large_table', dict(is_small = False, key_cnt = int(1.3e5))),
    ]
    scenarios = make_scenarios(table_scale)

    def populate(self, uri):
        with WiredTigerCursor(self.session, uri, None, None) as cursor:
            for i in range(self.key_cnt):
                cursor[i] = self.long_v
        with WiredTigerCursor(self.session, uri, None, "debug=(release_evict)") as cursor:
            for i in range(self.key_cnt):
                cursor.set_key(i)
                cursor.search()
        self.session.checkpoint()
    
    def stat_check(self, uri, reuse_50, reuse_90):
        with WiredTigerStat(self.session, uri) as stat_cursor:
            reusable_size = stat_cursor[stat.dsrc.block_reuse_bytes][2]
            block_size = stat_cursor[stat.dsrc.block_size][2]
            ratio_over_50 = stat_cursor[stat.dsrc.block_reusable_over_50][2]
            ratio_over_90 = stat_cursor[stat.dsrc.block_reusable_over_90][2]
            if self.is_small:
                self.assertEqual(ratio_over_50, 0)
                self.assertEqual(ratio_over_90, 0)
            else:
                self.assertEqual(ratio_over_50, 1 if reusable_size >= 0.5*block_size else 0)
                self.assertEqual(ratio_over_90, 1 if reusable_size >= 0.9*block_size else 0)
                self.assertEqual(ratio_over_50, reuse_50)
                self.assertEqual(ratio_over_90, reuse_90)
    
    def clear_between(self, uri, start, end):
        with WiredTigerCursor(self.session, uri, None, None) as cursor:
            # Target to 60%
            for i in range(start, end):
                cursor.set_key(i)
                cursor.remove()
        with WiredTigerCursor(self.session, uri, None, "debug=(release_evict)") as cursor:
            for i in range(start, end):
                cursor.set_key(i)
                cursor.search()
        self.session.checkpoint()
    
    def clean(self, uri):
        self.stat_check(uri, 0, 0)
        split_60 = int(self.key_cnt*0.6)
        split_95 = int(self.key_cnt*0.95)
        self.clear_between(uri, 0, split_60)
        self.stat_check(uri, 1, 0)
        self.clear_between(uri, split_60, split_95)
        self.stat_check(uri, 1, 1)
        
    def table_name(self, i:int):
        return f'{self.uri}_{i}'

    def test_reusable_percentage(self):
        # Populate a table with a few records. This will create a two-level tree with a root
        # page and one or more leaf pages. We aren't inserting nearly enough records to need
        # an additional level
        create_params = 'key_format=i,value_format=S'
        for i in range(self.table_cnt):
            self.session.create(self.table_name(i), create_params)
            self.populate(self.table_name(i))
        for i in range(self.table_cnt):
            with WiredTigerStat(self.session) as stat_cursor:
                files_over_50 = stat_cursor[stat.conn.block_reusable_over_50][2]
                files_over_90 = stat_cursor[stat.conn.block_reusable_over_90][2]
                if self.is_small:
                    self.assertEqual(files_over_50, 0)
                    self.assertEqual(files_over_90, 0)
                else:
                    self.assertEqual(files_over_50, i)
                    self.assertEqual(files_over_90, i)
            self.clean(self.table_name(i))
        with WiredTigerStat(self.session) as stat_cursor:
            files_over_50 = stat_cursor[stat.conn.block_reusable_over_50][2]
            files_over_90 = stat_cursor[stat.conn.block_reusable_over_90][2]
            if self.is_small:
                self.assertEqual(files_over_50, 0)
                self.assertEqual(files_over_90, 0)
            else:
                self.assertEqual(files_over_50, self.table_cnt)
                self.assertEqual(files_over_90, self.table_cnt)
                
            