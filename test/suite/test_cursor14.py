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

import wiredtiger, wttest
from wtdataset import SimpleDataSet, ComplexDataSet, ComplexLSMDataSet
from wtscenario import make_scenarios

# test_cursor14.py
#    Test that more than 64K cursors can be opened on a data source
class test_cursor14(wttest.WiredTigerTestCase):
    scenarios = make_scenarios([
        ('file-r', dict(type='file:', keyfmt='r', dataset=SimpleDataSet)),
        ('file-S', dict(type='file:', keyfmt='S', dataset=SimpleDataSet)),
        ('lsm-S', dict(type='lsm:', keyfmt='S', dataset=SimpleDataSet)),
        ('table-r', dict(type='table:', keyfmt='r', dataset=SimpleDataSet)),
        ('table-S', dict(type='table:', keyfmt='S', dataset=SimpleDataSet)),
        ('table-r-complex', dict(type='table:', keyfmt='r',
            dataset=ComplexDataSet)),
        ('table-S-complex', dict(type='table:', keyfmt='S',
            dataset=ComplexDataSet)),
        ('table-S-complex-lsm', dict(type='table:', keyfmt='S',
            dataset=ComplexLSMDataSet)),
    ])

    def test_cursor14(self):
        uri = self.type + 'cursor14'

        ds = self.dataset(self, uri, 100, key_format=self.keyfmt)
        ds.populate()

        for i in xrange(66000):
            cursor = self.session.open_cursor(uri, None, None)

if __name__ == '__main__':
    wttest.run()
