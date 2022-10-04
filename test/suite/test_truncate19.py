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
import time

# test_truncate19.py
#
# WT-9715 testing thingo

class test_truncate19(wttest.WiredTigerTestCase):
    conn_config = 'log=(enabled=true)'
    
    def test_truncate19(self):
        extraconfig = ',log=(enabled=true)'

        # VLCS and FLCS tables
        vlcs_uri = "table:trunca2te19_vlcs"
        vlcs_format = "key_format=r,value_format=i"

        flcs_uri = "table:trunca2te19_flcs"
        flcs_format = "key_format=r,value_format=8t"

        session2 = self.conn.open_session()
        self.session.create(vlcs_uri, vlcs_format+extraconfig)
        self.session.create(flcs_uri, flcs_format+extraconfig)

        vlcs_cursor2 = session2.open_cursor(vlcs_uri)
        flcs_cursor2 = session2.open_cursor(flcs_uri)

        vlcs_cursor = self.session.open_cursor(vlcs_uri)
        flcs_cursor = self.session.open_cursor(flcs_uri)

        self.session.begin_transaction()

        # 1. Insert keys 1-100 and 150-200
        for i in range(1, 100):
            vlcs_cursor[i] = i
            flcs_cursor[i] = i

        for i in range(150, 200):
            vlcs_cursor[i] = i
            flcs_cursor[i] = i
        self.session.commit_transaction()

        self.session.checkpoint()

        # 2. Truncate from 90-110
        self.session.begin_transaction()

        # 3. Truncate FLCS
        flcs_start = self.session.open_cursor(flcs_uri, None)
        flcs_start.set_key(80)
        flcs_end = self.session.open_cursor(flcs_uri, None)
        flcs_end.set_key(130)
        self.session.truncate(None, flcs_start, flcs_end, "log=(enabled=true)")
        
        # 4. Insert 120 on a second transaction.
        session2.begin_transaction()
        vlcs_cursor2[120] = 120
        flcs_cursor2[120] = 120
        session2.commit_transaction()

        # 5. Truncate VLCS
        vlcs_start = self.session.open_cursor(vlcs_uri, None)
        vlcs_start.set_key(80)
        vlcs_end = self.session.open_cursor(vlcs_uri, None)
        vlcs_end.set_key(130)

        self.session.truncate(None, vlcs_start, vlcs_end, "log=(enabled=true)")
        self.session.commit_transaction()

        time.sleep(5)
        # 6. Simulate crash and log recovery.
        simulate_crash_restart(self, ".", "RESTART")
        vlcs_cursor2 = self.session.open_cursor(vlcs_uri)
        flcs_cursor2 = self.session.open_cursor(flcs_uri)

        # 7. Both keys and values should be the same, but are not.
        vlcs_cursor2.set_key(120)
        flcs_cursor2.set_key(120)
        
        self.assertEqual(vlcs_cursor2.search(), flcs_cursor2.search())
        self.assertEqual(vlcs_cursor2.get_value(), flcs_cursor2.get_value())

if __name__ == '__main__':
    wttest.run()
