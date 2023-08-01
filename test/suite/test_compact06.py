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
from wtscenario import make_scenarios
from wtdataset import SimpleDataSet

# test_compact06.py
# Test background compaction API usage.
class test_compact06(wttest.WiredTigerTestCase):
    uri = 'file:test_compact06'
    conn_config = 'cache_size=2MB,statistics=(all),verbose=(compact:2)'
    # key_format_values = (
    #     ('integer-row', dict(key_format='i', value_format='S'))
    # )
    # scenarios = make_scenarios(key_format_values)
    key_format='i'
    value_format='S'
    
    delete_range_len = 10 * 1000
    delete_ranges_count = 4
    table_numkv = 100 * 1000

    def test_background_compact_api(self):
        # Create a table.
        ds = SimpleDataSet(self, self.uri, self.table_numkv, 
                           key_format=self.key_format, 
                           value_format=self.value_format)
        ds.populate()
        self.session.create(self.uri, 
                            f'key_format={self.key_format},value_format={self.value_format}')
        
        # Now let's delete a lot of data ranges. Create enough space so that compact runs in more
        # than one iteration.
        c = self.session.open_cursor(self.uri, None)
        for r in range(self.delete_ranges_count):
            start = r * self.table_numkv // self.delete_ranges_count
            for i in range(self.delete_range_len):
                c.set_key(start + i)
                c.remove()
        c.close()
        
        # Test for invalid uses of the compact API:
        #   1. We cannot trigger the background compaction on a specific API.
        with self.expectedStdoutPattern('Background compaction does not work on specific URIs.'):
            self.assertRaisesException(wiredtiger.WiredTigerError, lambda:
                self.session.compact(self.uri, 'background=true'))        
        #   2. We cannot set other configurations while turning off the background server.
        with self.expectedStdoutPattern('Other compaction configurations cannot be set'):
            self.assertRaisesException(wiredtiger.WiredTigerError, lambda:
                self.session.compact(None, 'background=false,free_space_target=10MB'))
        #   3. We cannot reconfigure the background server.
        # TODO: Once WT-11342 is complete.

if __name__ == '__main__':
    wttest.run()
