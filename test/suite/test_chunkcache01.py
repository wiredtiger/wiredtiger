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

import time
import wiredtiger, wttest
from wtscenario import make_scenarios


class test_chunkcache(wttest.WiredTigerTestCase):
    uri = "table:test_chunkcache01"
    format_values = [
        ('row_integer', dict(key_format='i', value_format='S')),
    ]
    conn_config = "cache_size=70MB,statistics=(all),statistics_log=(wait=1,json=true,on_close=true)"
    restart_config = "cache_size=70MB,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),chunk_cache=[enabled=true,chunk_size=10MB,capacity=500MB,type=FILE,storage_path=/home/ubuntu/wiredtiger/w/chunk_file]"
    scenarios = make_scenarios(format_values)
    rows = 7000000
    def get_stat(self, stat):
        stat_cursor = self.session.open_cursor('statistics:')
        val = stat_cursor[stat][2]
        stat_cursor.close()
        return val

    def cache_stuff(self):
        self.prout("chunks: " + str(self.get_stat(wiredtiger.stat.conn.chunk_cache_chunks_inuse)))
        self.prout("chunks (bytes): " + str(self.get_stat(wiredtiger.stat.conn.chunk_cache_bytes_inuse)))
        self.prout("chunks_evcited: " + str(self.get_stat(wiredtiger.stat.conn.chunk_cache_chunks_evicted)))
        self.prout("page_images: " + str(self.get_stat(wiredtiger.stat.conn.cache_bytes_image)))
        
        self.prout("\n\n")

    def insert(self, rows):
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, rows):
            cursor.set_key(i)
            cursor.set_value((str(i)))
            cursor.insert()
    

    def read(self, rows):
        cursor = self.session.open_cursor(self.uri)
        for i in range(1, rows):
            cursor.set_key(i)
            cursor.search()


    def test_practice(self):
        format = 'key_format={},value_format={}'.format(self.key_format, self.value_format)
        self.session.create(self.uri, format)
        
        self.conn.set_timestamp("oldest_timestamp=" + self.timestamp_str(1) + ",stable_timestamp=" + self.timestamp_str(2))

        for i in range(1, 10000):
            self.insert(200)
        
        self.reopen_conn(".", self.restart_config)

        for i in range(4,1000):
            self.read(200)
        
        self.cache_stuff()

# Check bitmap correctly reflects the file / vice versa 
# Check all the functions - setup, alloc, free
# 

# All code touched by the memkind removal is covered
# Edge cases (running out of cache, fragmentation-prone workloads) are covered.
# Both DRAM-backed and file-backed cache types work
# Checking the size does not exceed the threshold
# Randomising chunk sizes to test the various interactions possible with block size overlaps
