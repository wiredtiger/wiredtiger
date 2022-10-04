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
        #extraconfig = ''
        extraconfig = ',log=(enabled=true)'

        # VLCS and FLCS tables
        row_uri = "table:trunca2te19_vlcs"
        row_format = "key_format=r,value_format=S"

        flcs_uri = "table:trunca2te19_flcs"
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
            row_cursor[i] = str(i)
            flcs_cursor[i] = i

        for i in range(150, 200):
            row_cursor[i] = str(i)
            flcs_cursor[i] = i
        self.session.commit_transaction()

        self.session.checkpoint()

        # 2. truncate from 90-110
        self.session.begin_transaction()

        # 3. Truncate FLCS
        flcs_start = self.session.open_cursor(flcs_uri, None)
        flcs_start.set_key(80)
        flcs_end = self.session.open_cursor(flcs_uri, None)
        flcs_end.set_key(130)
        # flcs_end.search_near()
        # flcs_end.prev()
        self.session.truncate(None, flcs_start, flcs_end, "log=(enabled=true)")
        
        # 4. Modify 120
        session2.begin_transaction()
        row_cursor2[120] = str(120)
        flcs_cursor2[120] = 120
        session2.commit_transaction()

        # 5. Truncate Row
        row_start = self.session.open_cursor(row_uri, None)
        row_start.set_key(80)
        row_end = self.session.open_cursor(row_uri, None)
        row_end.set_key(130)
        # row_end.search_near()
        # row_end.prev()

        self.session.truncate(None, row_start, row_end, "log=(enabled=true)")
        # print(row_end.get_key(), flcs_end.get_key())


        self.session.commit_transaction()

        self.conn_config = 'log=(enabled=true),verbose=(recovery)'
        simulate_crash_restart(self, ".", "RESTART")
        row_cursor2 = self.session.open_cursor(row_uri)
        flcs_cursor2 = self.session.open_cursor(flcs_uri)

        row_cursor2.set_key(120)
        flcs_cursor2.set_key(120)
        
        self.assertEqual(row_cursor2.search(), flcs_cursor2.search())

if __name__ == '__main__':
    wttest.run()


# // T1 Truncate 520978 [4, 105344] -> Begins earlier and commits later
# // T2 Update 521192 [4, 76032] -> Begins later and commits earlier

# // T1, T2 Begins, Logging
# // 11169 -> 15874 do not exist
# // search_near(13000)
# // T1 truncates 1 -> 15874 Table1 (FLCS) -> position (snapshot)
# // T2 updates and commits 13777 Logging #1
# // T1 truncates 1 -> 11169 Table2 (ROW-STORE) -> 11169 
# // T1 commits Logs #2 
# Live vs Replayed
# 
# Playing from the logs
# #1 and then #2
# Update on 13777
# Truncate

# T2 first and then 

# // T1, T2 Begins
# // 11169 -> 15874 do not exist
# // search_near(13000)
# // T2 updates and commits 13777
# // T1 truncates 1 -> 15874 T1 (FLCS) -> position 
# // T2 truncates 1 -> 11169 T2 (ROW-STORE) -> 11169 