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

import wiredtiger, wttest, time
from wtdataset import SimpleDataSet

# test_txn28.py
#   Test that checking the snapshot array is correctly outputted
class test_txn28(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=1MB'

    def get_lines_containing_substring(self, text, substring):
        lines = text.split('\n')
        return [line for line in lines if substring in line]

    def get_numbers_after_substring(self, text, sub_str):
        pattern = rf'{re.escape(sub_str)}\s*(\d+)'
        match = re.search(pattern, text)
        if match:
            return match.group(1)
        else:
            return None

    # Get the content in the first square bracket
    def count_numbers_in_string(self, s):
        return len(re.findall(r'\d+', s))
    
    def test_snapshot_array_dump(self):
        uri = "table:txn28"
        # Create a very basic table.
        ds = SimpleDataSet(self, uri, 10, key_format='S', value_format='S')
        ds.populate()

        # The first session.
        session1 = self.session
        cursor1 = session1.open_cursor(uri)
        session1.begin_transaction()
        cursor1[ds.key(5)] = "aaa"

        # The second session
        session2 = self.conn.open_session()
        cursor2 = session2.open_cursor(uri)
        session2.begin_transaction()
        cursor2.set_key(ds.key(6))
        cursor2.set_value("bbb")
        self.conn.debug_info('sessions')
        max_snapshot_list_item_count = 0         
        
        # Get the "transaction state dump" info, get content of " snapshot count: " and "snapshot: [xxx]"
        with self.expectedStdoutPattern('transaction state dump'):
            txn_debug_info=self.conn.debug_info('txn')
            snapshot_lines = self.get_lines_containing_substring(txn_debug_info, " snapshot count: ")
            for line in snapshot_lines:
                snapshot_count = self.get_numbers_after_substring(line, ", snapshot count: ")
                snapshot_list_item_count = self.count_numbers_in_string(line)
                self.assertEquals(snapshot_count, snapshot_list_item_count)        
                if max_snapshot_list_item_count < snapshot_list_item_count:
                    max_snapshot_list_item_count = snapshot_list_item_count
        self.assertEquals(max_snapshot_list_item_count, 1)
 
        session2.commit_transaction()
        session1.commit_transaction()
