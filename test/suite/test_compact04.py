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
#
# test_compact04.py
#   Test that compact doesn't reduce the file size when there are overflow values at the
#   end of file.
#

import os
import time, wiredtiger, wttest
from wiredtiger import stat
from wtscenario import make_scenarios

# Test compact behaviour with overflow values.
class test_compact04(wttest.WiredTigerTestCase):

    uri='table:test_compact04'
    file_name='test_compact04.wt'

    fileConfig = [
        #('1KB', dict(fileConfig='allocation_size=1KB,leaf_page_max=1KB')),
        ('4KB', dict(fileConfig='allocation_size=4KB,leaf_page_max=4KB')),
    ]

    scenarios = make_scenarios(fileConfig)

    # Enable stats and use a cache size that can fit table in the memory.
    conn_config = 'statistics=(all),cache_size=100MB'

    normalValue = "abcde" * 20
    nrecords = 100000 # To create ~25 MB table

    # Create a table, add keys with both big and small values.
    def test_compact04(self):

        # 1. Create a table with relatively small page size.
        params = 'key_format=Q,value_format=QSQS,' + self.fileConfig
        self.session.create(self.uri, params)
        c = self.session.open_cursor(self.uri, None)
        for i in range(self.nrecords):
            c[i] = (i, self.normalValue, i, self.normalValue)
        c.close()

        # 2. Checkpoint and get stats on the table to confirm the size.
        self.session.checkpoint()

        # 3. Delete middle ~90% of the normal values in the table.
        c = self.session.open_cursor(self.uri, None)
        for i in range((self.nrecords // 100) * 5, (self.nrecords // 100) * 95):
            c.set_key(i)
            self.assertEqual(c.remove(), 0)
        c.close()

        # 4. Checkpoint and get stats on the table to confirm the size.
        self.session.checkpoint()

        # 5. Call compact with punch_holes.
        blocks_before = os.stat(self.file_name).st_blocks
        self.session.compact(self.uri, "punch_holes=1")
        
        blocks_after = os.stat(self.file_name).st_blocks

        self.pr("Blocks before: %d; blocs after: %d" % (blocks_before, blocks_after))
        self.assertGreater(blocks_before / 10, blocks_after)

if __name__ == '__main__':
    wttest.run()
