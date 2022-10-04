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
    conn_config = 'log=(enabled=true)'
    
    def test_truncate19(self):
        nrows = 1000
        #extraconfig = ''
        extraconfig = ',log=(enabled=true)'

        # VLCS and FLCS tables
        row_uri = "table:truncate19_row"
        row_format = "key_format=i,value_format=i"

        flcs_uri = "table:truncate19_flcs"
        flcs_format = "key_format=r,value_format=8t"

        session2 = self.conn.open_session()
        self.session.create(row_uri, row_format+extraconfig)
        self.session.create(flcs_uri, flcs_format+extraconfig)


        row_cursor2 = session2.open_cursor(row_uri)
        flcs_cursor2 = session2.open_cursor(flcs_uri)

        row_cursor = self.session.open_cursor(row_uri)
        flcs_cursor = self.session.open_cursor(flcs_uri)

        self.session.begin_transaction()
        # insert keys 1-100
        for i in range(1, 100):
            row_cursor[i] = i
            flcs_cursor[i] = i
        self.session.commit_transaction()


        self.session.begin_transaction()
        for i in range(150, 200):
            row_cursor[i] = i
            flcs_cursor[i] = i
        self.session.commit_transaction()

        # 2. truncate from 90-110
        session2.begin_transaction()
        self.session.begin_transaction()

        # 3. Truncate FLCS
        flcs_start = self.session.open_cursor(flcs_uri, None)
        flcs_start.set_key(120)
        flcs_end = self.session.open_cursor(flcs_uri, None)
        flcs_end.set_key(130)
        # flcs_end.search_near()
        # flcs_end.prev()
        self.session.truncate(None, flcs_start, flcs_end, None)
        
        # 4. Modify 120
        row_cursor2[120] = 120
        flcs_cursor2[120] = 120

        # 5. Truncate Row
        row_start = self.session.open_cursor(row_uri, None)
        row_start.set_key(120)
        row_end = self.session.open_cursor(row_uri, None)
        row_end.set_key(130)
        # row_end.search_near()
        # row_end.prev()

        self.session.truncate(None, row_start, row_end, None)

        # print(row_end.get_key(), flcs_end.get_key())


        session2.commit_transaction()
        self.session.commit_transaction()

        self.session.checkpoint()


if __name__ == '__main__':
    wttest.run()


# // T1 Truncate 520978 [4, 105344] -> Begins earlier and commits later
# // T2 Update 521192 [4, 76032] -> Begins later and commits earlier

# // T1, T2 Begins
# // 11169 -> 15874 do not exist
# // search_near(13000)
# // T1 truncates 1 -> 15874 T1 (FLCS) -> position (snapshot)
# // T2 updates and commits 13777 
# // T2 truncates 1 -> 11169 T2 (ROW-STORE) -> 11169 
# //

# // T1, T2 Begins
# // 11169 -> 15874 do not exist
# // search_near(13000)
# // T2 updates and commits 13777
# // T1 truncates 1 -> 15874 T1 (FLCS) -> position 
# // T2 truncates 1 -> 11169 T2 (ROW-STORE) -> 11169 
# //