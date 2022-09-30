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

import wttest
from helper import simulate_crash_restart
from wiredtiger import stat, WiredTigerError, wiredtiger_strerror, WT_NOTFOUND, WT_ROLLBACK
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios

# test_truncate19.py
#
# WT-9715 testing thingo

class test_truncate19(wttest.WiredTigerTestCase):
    def test_truncate19(self):
        nrows = 1000
        extraconfig = ',log=(enabled=true,file_max=318905344)'

        # VLCS and FLCS tables
        row_uri = "table:truncate19_row"
        row_format = "key_format=i,value_format=S"

        flcs_uri = "table:truncate19_flcs"
        flcs_format = "key_format=r,value_format=8t"

        self.session.create(row_uri, row_format+extraconfig)
        self.session.create(flcs_uri, flcs_format+extraconfig)

        row_cursor = self.session.open_cursor(row_uri)
        flcs_cursor = self.session.open_cursor(flcs_uri)

        self.session.begin_transaction()
        # insert keys 1-100
        for i in range(1, 100):
            row_cursor[i] = str(i)
            flcs_cursor[i] = i

        # 2. truncate from 90-110
        row_start = self.session.open_cursor(row_uri, None)
        row_start.set_key(90)
        flcs_start = self.session.open_cursor(flcs_uri, None)
        flcs_start.set_key(90)

        row_end = self.session.open_cursor(row_uri, None)
        row_end.set_key(110)
        flcs_end = self.session.open_cursor(flcs_uri, None)
        flcs_end.set_key(110)

        self.session.truncate(None, row_start, row_end, None)
        self.session.truncate(None, flcs_start, flcs_end, None)

        self.session.commit_transaction()

        # checkpoint
        self.session.checkpoint()

        # then restart
        simulate_crash_restart(self, ".", "RESTART")
        self.assertEqual(0, 1)

        # TODO figure out success/failure condition - check all data?


if __name__ == '__main__':
    wttest.run()
