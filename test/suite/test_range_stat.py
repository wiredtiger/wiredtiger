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

import random, wiredtiger, wttest
from wtdataset import ComplexDataSet, SimpleIndexDataSet, SimpleDataSet
from wtscenario import make_scenarios

class test_range_stat(wttest.WiredTigerTestCase):
    keyfmt = [
        ('integer-row', dict(keyfmt='i')),
        ('column', dict(keyfmt='r')),
        ('string-row', dict(keyfmt='S')),
    ]
    types = [
        ('file', dict(uri='file', ds=SimpleDataSet)),
        ('table-complex', dict(uri='table', ds=ComplexDataSet)),
        ('table-index', dict(uri='table', ds=SimpleIndexDataSet)),
        ('table-simple', dict(uri='table', ds=SimpleDataSet)),
    ]
    scenarios = make_scenarios(types, keyfmt)

    def populate(self, uri):
        # Create lots of small pages.
        size = 'allocation_size=512,internal_page_max=512'
        rows = random.randint(12345, 67890)
        ds = self.ds(self, uri, rows, config=size, key_format=self.keyfmt)
        ds.populate()

        # Force to disk to guarantee all pages have counts; pages without the counts calculated
        # during reconciliation can result in unavailable results.
        self.reopen_conn()
        return ds, rows

    def test_range_stat_uri(self):
        uri = self.uri + ':test_range_stat'
        ds, rows = self.populate(uri)

        with self.expectedStderrPattern(''):
            try:
                (ret_rows, ret_bytes) = self.session.range_stat(uri, None, None)
            except Exception as e:
                self.assertEquals(str(e), 'Operation not supported')
                return

        self.verbose(3, "{}: rows {}, returned rows {}, bytes {}".
            format(self.keyfmt, rows, ret_rows, ret_bytes))

        # We know exactly the number of rows.
        self.assertEquals(ret_rows, rows)
        # There is some cost in bytes of storing each row, but it's hard to guess what it is.
        self.assertGreater(ret_bytes, rows * 10)

    def test_range_stat_cursor(self):
        uri = self.uri + ':test_range_stat'
        ds, rows = self.populate(uri)

        cstart = self.session.open_cursor(uri, None, None)
        pct = int (rows / 5)
        kstart = random.randint(1, pct)
        cstart.set_key(ds.key(kstart))

        cstop = self.session.open_cursor(uri, None, None)
        kstop = random.randint(pct, rows - pct)
        cstop.set_key(ds.key(kstop))

        # The rows and bytes returned are estimates, we can't really check them.
        with self.expectedStderrPattern(''):
            try:
                (ret_rows, ret_bytes) = self.session.range_stat(None, cstart, cstop)
            except Exception as e:
                self.assertEquals(str(e), 'Operation not supported')
                return

        self.verbose(3, "{}: rows {}, start {}, stop {}, returned rows {}, bytes {}".
            format(self.keyfmt, rows, kstart, kstop, ret_rows, ret_bytes))

if __name__ == '__main__':
    wttest.run()
