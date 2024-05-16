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

import time, wttest, threading
from wiredtiger import stat
from wtthread import checkpoint_thread

kilobyte = 1024

# test_compact_checkpoint.py
# This test creates:
#
# - One table with the first 1/4 of keys deleted.
class test_compact_checkpoint(wttest.WiredTigerTestCase):
    create_params = 'key_format=i,value_format=S,allocation_size=4KB,leaf_page_max=32KB,leaf_value_max=16MB'
    conn_config = 'cache_size=1GB,statistics=(all),verbose=[compact:4]'
    uri = 'table:test_compact_checkpoint'

    table_numkv = 100 * 1000
    value_size = kilobyte # The value should be small enough so that we don't create overflow pages.
    
    # Return stats that track the progress of compaction.
    def getCompactProgressStats(self):
        cstat = self.session.open_cursor(
            'statistics:' + self.uri, None, 'statistics=(all)')
        statDict = {}
        statDict["bytes_rewritten_expected"] = cstat[stat.dsrc.btree_compact_bytes_rewritten_expected][2]
        statDict["pages_reviewed"] = cstat[stat.dsrc.btree_compact_pages_reviewed][2]
        statDict["pages_skipped"] = cstat[stat.dsrc.btree_compact_pages_skipped][2]
        statDict["pages_rewritten"] = cstat[stat.dsrc.btree_compact_pages_rewritten][2]
        statDict["pages_rewritten_expected"] = cstat[stat.dsrc.btree_compact_pages_rewritten_expected][2]
        cstat.close()
        return statDict

    def get_size(self, uri):
        stat_cursor = self.session.open_cursor('statistics:' + uri, None, 'statistics=(all)')
        size = stat_cursor[stat.dsrc.block_size][2]
        stat_cursor.close()
        return size

    def get_fast_truncated_pages(self):
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        pages = stat_cursor[stat.conn.rec_page_delete_fast][2]
        stat_cursor.close()
        return pages

    def truncate(self, uri, key1, key2):
        lo_cursor = self.session.open_cursor(uri)
        hi_cursor = self.session.open_cursor(uri)
        lo_cursor.set_key(key1)
        hi_cursor.set_key(key2)
        self.session.truncate(None, lo_cursor, hi_cursor, None)
        lo_cursor.close()
        hi_cursor.close()

    def populate(self, uri, num_keys):
        c = self.session.open_cursor(uri, None)
        for k in range(num_keys // 10 * 9):
            c[k] = ('%07d' % k) + '_' + 'a' * self.value_size

        for k in range(num_keys // 10 * 9, num_keys):
            c[k] = ('%07d' % k) + '_' + 'b' * self.value_size
        c.close()

    def wait_for_cc_to_run(self):
        c = self.session.open_cursor( 'statistics:')
        cc_success = prev_cc_success = c[stat.conn.checkpoint_cleanup_success][2]
        c.close()
        while cc_success - prev_cc_success == 0:
            time.sleep(0.1)
            c = self.session.open_cursor( 'statistics:')
            cc_success = c[stat.conn.checkpoint_cleanup_success][2]
            c.close()

    def test_compact_checkpoint(self):
        # FIXME-WT-11399
        if self.runningHook('tiered'):
            self.skipTest("this test does not yet work with tiered storage")

        # Create and populate a table.
        uri = self.uri
        self.session.create(uri, self.create_params)

        self.populate(uri, self.table_numkv)

        # Write to disk.
        ckpt_session = self.conn.open_session()
        ckpt_session.checkpoint()

        # Remove 1/4 of the keys.
        c = self.session.open_cursor(uri, None)
        for i in range(self.table_numkv // 4):
            c.set_key(i)
            c.remove()
        c.close()

        # Reopen connection to ensure everything is on disk.
        self.reopen_conn()

        # Check the size of the table.
        size_before_compact = self.get_size(uri)
        self.pr(f'size_before_compact={size_before_compact}')

        # Checkpoint in the background.
        done = threading.Event()
        ckpt = checkpoint_thread(self.conn, done)
        
        try:
            ckpt.start()
            self.session.compact(uri, 'free_space_target=1MB')
            
        finally:
            done.set()
            ckpt.join()
            
        size_after_compact = self.get_size(uri)
        
        self.assertLess(size_after_compact, size_before_compact)

        statDict = self.getCompactProgressStats()
        self.assertGreater(statDict["pages_reviewed"],0)
        self.assertGreater(statDict["pages_rewritten"],0)
        
        # Ignore compact verbose messages used for debugging.
        self.ignoreStdoutPatternIfExists('WT_VERB_COMPACT')
        self.ignoreStderrPatternIfExists('WT_VERB_COMPACT')

if __name__ == '__main__':
    wttest.run()
