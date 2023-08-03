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

import time, random
import wiredtiger, wttest
from wtscenario import make_scenarios
from wtdataset import SimpleDataSet
from wiredtiger import stat
from suite_random import suite_random

megabyte = 1024 * 1024

# test_compact07.py
# Test background compaction server.
class test_compact07(wttest.WiredTigerTestCase):
    uri_prefix = 'file:test_compact06'
    conn_config = 'cache_size=100MB,statistics=(all)'
    # key_format_values = (
    #     ('integer-row', dict(key_format='i', value_format='S'))
    # )
    # scenarios = make_scenarios(key_format_values)
    key_format='i'
    value_format='S'
    
    delete_range_len = 10 * 1000
    delete_ranges_count = 4
    table_numkv = 200 * 1000
    n_tables = 5
    
    # Return the size of the file
    def get_size(self, uri):
        # To allow this to work on systems without ftruncate,
        # get the portion of the file allocated, via 'statistics=(all)',
        # not the physical file size, via 'statistics=(size)'.
        stat_cursor = self.session.open_cursor(
            'statistics:' + uri, None, 'statistics=(all)')
        size = stat_cursor[stat.dsrc.block_size][2]
        stat_cursor.close()
        return size
    
    def get_free_space(self, uri):
        stat_cursor = self.session.open_cursor('statistics:' + uri, None, 'statistics=(all)')
        bytes = stat_cursor[stat.drsc.block_reuse_bytes][2]
        stat_cursor.close()
        return bytes
        
    def delete_range(self, uri):
        # Now let's delete a lot of data ranges. Create enough space so that compact runs in more
        # than one iteration.
        c = self.session.open_cursor(uri, None)
        for r in range(self.delete_ranges_count):
            start = r * self.table_numkv // self.delete_ranges_count
            for i in range(self.delete_range_len):
                c.set_key(start + i)
                c.remove()
        c.close()
    
    # Test the basic functionality of the background compaction server. 
    def test_background_compact_usage(self):        
        # Create ten tables for background compaction to loop through.
        for i in range(self.n_tables):
            uri = self.uri_prefix + f'_{i}'
            ds = SimpleDataSet(self, uri, self.table_numkv, 
                            key_format=self.key_format, 
                            value_format=self.value_format)
            ds.populate()
            self.session.create(uri, 
                                f'key_format={self.key_format},value_format={self.value_format}')
    
            # Now let's delete a lot of data ranges. Create enough space so that compact runs in more
            # than one iteration.
            self.delete_range(uri)
    
        # Create a small table that compact should skip over.
        uri = self.uri_prefix + '_small'
        ds = SimpleDataSet(self, uri, 10, key_format=self.key_format, value_format=self.value_format)
        ds.populate()
        self.session.create(uri, f'key_format={self.key_format},value_format={self.value_format}')
        
        self.session.close()
        
        # Reopen the connection to force the object to disk.
        self.reopen_conn()
        
        # Allow background compaction to run for some time. Set a low free_space_target to avoid
        # having to run compaction on large files which can take a long time.
        self.session.compact(None,'background=true,free_space_target=1MB')
        
        # Check that the background server is running
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        compact_running = stat_cursor[stat.conn.session_background_compact_running[2]]
        # files_compacted = stat_cursor[stat.conn.background_compact_files_compacted][2]
        self.assertEqual(compact_running, 1)
        stat_cursor.close()
        
        # Background compaction should run through every file as listed in the metadata file.
        # Periodically check how many files we've compacted until we compact all ten.
        # TODO: Add statistic to count how many files the background server has compacted.
        # FIXME: Use this statistic to check the progression of the test, instead of using an
        # arbitrary time.
        # while (files_compacted < self.n_files):
        #     time.sleep(10)
            
        #     # Check how many files we've compacted
        #     stat_cursor = self.session.open_cursor('statistics:', None, None)
        #     files_compacted = stat_cursor[stat.conn.background_compact_files_compacted][2]
        #     stat_cursor.close()
        
        time.sleep(60)
        
        # Disable the background compaction server. It should be interruptable.
        self.session.compact(None,'background=false')
        
        # Check that the background server is no longer running.
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        compact_running = stat_cursor[stat.conn.session_background_compact_running[2]]
        self.assertEqual(compact_running, 0)
        stat_cursor.close()
        
        # Check that we've made some progress on all the files.
        for i in range(self.n_tables):
            uri = self.uri_prefix + f'_{i}'
            stat_cursor = self.session.open_cursor('statistics:' + uri, None, None)
            pages_rewritten = stat_cursor[stat.dsrc.btree_compact_pages_rewritten][2]
            stat_cursor.close()
            
            self.assertGreater(pages_rewritten, 0)
            
        # Check that we made no progress on the small file.
        uri = self.uri_prefix + '_small'
        stat_cursor = self.session.open_cursor('statistics:' + uri, None, None)
        pages_rewritten = stat_cursor[stat.dsrc.btree_compact_pages_rewritten][2]
        stat_cursor.close()
        
        self.assertEqual(pages_rewritten, 0)
        
    # Test that the background compaction server can run concurrently with another session running
    # foreground compaction.
    def test_background_compact_concurrency(self):
        # Create ten tables for background compaction to loop through.
        for i in range(self.n_tables):
            uri = self.uri_prefix + f'_{i}'
            ds = SimpleDataSet(self, uri, self.table_numkv, 
                            key_format=self.key_format, 
                            value_format=self.value_format)
            ds.populate()
    
            # Now let's delete a lot of data ranges. Create enough space so that compact runs in more
            # than one iteration.
            self.delete_range(uri)
            
            size = self.get_size(uri)
            self.pr(f"{uri} file size = {size // megabyte}MB")
            
        self.session.close()
            
        # Reopen the connection to force the object to disk.
        self.reopen_conn()
            
        # Create another file of a smaller size (about half) that the background compaction should 
        # skip over when set with a sufficiently high free_space_target.
        uri_small = self.uri_prefix + '_small'
        ds = SimpleDataSet(self, uri_small, 100 * 1000, key_format=self.key_format, value_format=self.value_format)
        ds.populate()
        self.delete_range(uri)
        
        small_file_free_space = self.get_free_space(uri_small)
                
        # assert that the small file is smaller than the other 10 files.
        
        # Start background compaction with a free_space_target larger than the size of the small file
        # but smaller than the size of the other files.
        compact_cfg = f'background=true,free_space_target={small_file_free_space + 1}MB'
        self.session.compact(None,compact_cfg)
        
        # Give some time for background compaction to do some work.
        time.sleep(10)
        
        # Check we've made no progress on the small file.
        stat_cursor = self.session.open_cursor('statistics:' + uri_small, None, None)
        pages_rewritten = stat_cursor[stat.dsrc.btree_compact_pages_rewritten][2]
        stat_cursor.close()
        
        self.assertEqual(pages_rewritten, 0)
        
        # Open a new session for foreground compaction, while leaving the background
        # compaction server running.
        session2 = self.conn.open_session()
        # Use a free_space_target that is guaranteed to run on the small file.
        compact_cfg = f'free_space_target={small_file_free_space - 1}MB'
        session2.compact(uri_small,compact_cfg)
        
        # Check that foreground compaction has done some work.
        stat_cursor = self.session.open_cursor('statistics:' + uri_small, None, None)
        pages_rewritten = stat_cursor[stat.dsrc.btree_compact_pages_rewritten][2]
        stat_cursor.close()
        
        self.assertGreater(pages_rewritten, 0)
    
    # Test the background server 
    def test_background_compact_dynamic(self):
        uris = []
        # Trigger the background compaction server first, before creating tables.
        self.session.compact(None,'background=true,free_space_target=10MB')
        
        # Create a new table
        uris.append(self.uri_prefix + '_0')
        ds = SimpleDataSet(self, uris[0], self.table_numkv,
                           key_format=self.key_format,
                           value_format=self.value_format)
        ds.populate()
        
        # Create another table
        uris.append(self.uri_prefix + '_1')
        ds = SimpleDataSet(self, uris[1], self.table_numkv,
                           key_format=self.key_format,
                           value_format=self.value_format)
        ds.populate()
                
        # Double the size of the first table so that it should now be larger than the 
        # free_space_target.
        ds = SimpleDataSet(self, uris[0], self.table_numkv * 2,
                    key_format=self.key_format,
                    value_format=self.value_format)
        ds.populate(create=False)
        self.delete_range(uris[0])
                
        # Periodically check that background compaction has done some work on the first table.
        # stat_cursor = self.session.open_cursor('statistics:' + uris[0], None, None)
        # pages_rewritten = stat_cursor[stat.dsrc.btree_compact_pages_rewritten][2]
        # while (pages_rewritten == 0):
        #     time.sleep(10)
        #     pages_rewritten = stat_cursor[stat.dsrc.btree_compact_pages_rewritten][2]
            
        # stat_cursor.close()
        
        self.dropUntilSuccess(self.session, uris[0])
        self.dropUntilSuccess(self.session, uris[1])

if __name__ == '__main__':
    wttest.run()
