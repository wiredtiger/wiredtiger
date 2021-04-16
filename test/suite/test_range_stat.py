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
# test_range_stat.py

import wiredtiger, wttest
from wtdataset import ComplexDataSet, SimpleIndexDataSet, SimpleDataSet
from wtscenario import make_scenarios

class test_range_stat(wttest.WiredTigerTestCase):
    keyfmt = [
        ('integer', dict(keyfmt='i')),
        ('recno', dict(keyfmt='r')),
        ('string', dict(keyfmt='S')),
    ]
    types = [
        ('file', dict(uri='file', ds=SimpleDataSet)),
        ('table-complex', dict(uri='table', ds=ComplexDataSet)),
        ('table-index', dict(uri='table', ds=SimpleIndexDataSet)),
        ('table-simple', dict(uri='table', ds=SimpleDataSet)),
    ]
    scenarios = make_scenarios(types, keyfmt)

    def populate(self, uri):
        size = 'allocation_size=512,internal_page_max=512'
        ds = self.ds(self, uri, 25678, config=size, key_format=self.keyfmt)
        ds.populate()
        self.session.checkpoint()

    def test_range_stat_uri(self):
        uri = self.uri + ':test_range_stat'
        self.populate(uri)

        self.session.range_stat(uri, None, None)

    def Xtest_range_stat_cursor(self):
        uri = self.uri + ':test_range_stat'
        self.populate(uri)

        cstart = self.session.open_cursor(uri, None, None)
        cstart.set_key(ds.key(12000))
        cstop = self.session.open_cursor(uri, None, None)
        cstop.set_key(ds.key(13000))
        self.session.range_stat(None, cstart, cstop)

if __name__ == '__main__':
    wttest.run()
