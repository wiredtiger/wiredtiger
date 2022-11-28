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
# test_count01.py
#   Tests WT_SESSION->count
#

import wiredtiger, wttest
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

class test_count01(wttest.WiredTigerTestCase):
    tablename = 'test_count01'
    tablename2 = 'test'

    keyfmt = [
        ('integer-row', dict(keyfmt='i')),
        ('column', dict(keyfmt='r')),
    ]

    types = [
        ('file', dict(uri='file:' + tablename)),
        ('table', dict(uri='table:' + tablename)),
    ]
    scenarios = make_scenarios(keyfmt, types)

    def test_count_api(self):
        self.session.create(self.uri, 'key_format=i,value_format=i')
        c = self.session.open_cursor(self.uri, None, None)

        size='allocation_size=512,internal_page_max=512'
        ds = SimpleDataSet(self, self.uri, 3000, config=size, key_format=self.keyfmt)

        # Insert some values and persist them to disk.
        for i in range (0, 2000):
            c[ds.key(i)] = i
        c.close()

        self.session.checkpoint()

        # Check that the number of records stored in the page stat matches.
        self.assertEqual(self.session.count(self.uri), 2000)

    def test_count_api_empty(self):
        self.session.create(self.uri, 'key_format=i,value_format=i')
        c = self.session.open_cursor(self.uri, None, None)

        size='allocation_size=512,internal_page_max=512'
        ds = SimpleDataSet(self, self.uri, 3000, config=size, key_format=self.keyfmt)
        c.close()

        # Persist the empty table to disk.
        self.session.checkpoint()

        # Check that querying for the row count of an empty table triggers an error.
        self.assertRaisesException(
             wiredtiger.WiredTigerError, lambda: self.session.count(self.uri))


if __name__ == '__main__':
    wttest.run()
