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
import os
from helper import copy_wiredtiger_home
from wtscenario import make_scenarios
from wiredtiger import WT_NOTFOUND

# test_truncate21.py
# Test truncate logging and recovery when it has no work to do. 
class test_truncate21(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=2MB,log=(enabled)'
    dir = "newdir"
    nentries = 1000

    uri_fix = 'table:trunc_fix'
    uri_row = 'table:trunc_row'
    uri_var = 'table:trunc_var'
    create_fix = 'key_format=r,value_format=8t'
    create_row = 'key_format=i,value_format=S'
    create_var = 'key_format=r,value_format=S'

    start_key = nentries // 4
    end_key = nentries // 2
    # Pick a key in the range we just truncated to re-insert.
    insert_key = (start_key + end_key) // 2

    def trunc_range(self):
        # In a transaction, truncate the same range from all three tables.
        # Then truncate the same range again, also inserting one key into the range for
        # the row-store table. Delete a range from the middle of the table.
        cfix_start = self.session.open_cursor(self.uri_fix)
        crow_start = self.session.open_cursor(self.uri_row)
        #cvar_start = self.session.open_cursor(self.uri_var)
        cfix_end = self.session.open_cursor(self.uri_fix)
        crow_end = self.session.open_cursor(self.uri_row)
        #cvar_end = self.session.open_cursor(self.uri_var)
        cfix_start.set_key(self.start_key)
        crow_start.set_key(self.start_key)
        #cvar_start.set_key(self.start_key)
        cfix_end.set_key(self.end_key)
        crow_end.set_key(self.end_key)
        #cvar_end.set_key(self.end_key)
        # Do the truncate on teach table.
        self.session.truncate(None, cfix_start, cfix_end, None)
        self.session.truncate(None, crow_start, crow_end, None)
        #self.session.truncate(None, cvar_start, cvar_end, None)
        cfix_start.close()
        crow_start.close()
        #cvar_start.close()
        cfix_end.close()
        crow_end.close()
        #cvar_end.close()

    def test_truncate21(self):

        # Create one table of each type: FLCS, row-store and VLCS.
        # Put the same data into each (per allowed by type). 
        self.session.create(self.uri_fix, self.create_fix)
        self.session.create(self.uri_row, self.create_row)
        #self.session.create(self.uri_var, self.create_var)

        cfix = self.session.open_cursor(self.uri_fix)
        crow = self.session.open_cursor(self.uri_row)
        #cvar = self.session.open_cursor(self.uri_var)
        for i in range(1, self.nentries):
            self.session.begin_transaction()
            cfix[i] = 97
            crow[i] = 'rowval'
            #cvar[i] = 'varval'
            self.session.commit_transaction()
        cfix.close()
        crow.close()
        #cvar.close()

        # In a transaction, truncate the same range from all three tables.
        # Then truncate the same range again, also inserting one key into the range for
        # the row-store table. Delete a range from the middle of the table.
        self.pr('first trunc')
        self.session.begin_transaction()
        self.trunc_range()
        self.session.commit_transaction()

        # Open a second session and transaction. In one we truncate the same range again.
        # In the other we insert into the FLCS and row-store tables. (The VLCS table will be
        # used in a later test.)
        self.pr('open session 2')
        session2 = self.conn.open_session()

        self.pr('begin both transactions')
        self.session.begin_transaction()
        session2.begin_transaction()
        # In the other session, truncate the same range again.
        self.pr('second trunc')
        self.trunc_range()
        # Commit the insert.
        # With overlapping transactions, insert into the key range for FLCS and row.
        cfix = session2.open_cursor(self.uri_fix)
        crow = session2.open_cursor(self.uri_row)
        cfix[self.insert_key] = 98
        crow[self.insert_key] = 'newval'

        self.pr('commit insert')
        session2.commit_transaction()
        # Commit the truncate.
        self.pr('commit trunc')
        self.session.commit_transaction()

        self.pr('close session 2')
        cfix.close()
        crow.close()
        session2.close()

        # Flush all log records.
        self.pr('log flush')
        self.session.log_flush('sync=on')

        # Make a copy of the database and open it. That forces recovery to be run, which should
        # replay both truncate calls and then 
        os.mkdir(self.dir)
        copy_wiredtiger_home(self, '.', self.dir)

        self.pr('open new conn')
        new_conn = self.wiredtiger_open(self.dir)
        new_sess = new_conn.open_session()
        self.pr('check keys')
        cfix = new_sess.open_cursor(self.uri_fix)
        crow = new_sess.open_cursor(self.uri_row)
        cfix.set_key(self.insert_key)
        crow.set_key(self.insert_key)
        ret_fix = cfix.search()
        ret_row = crow.search()
        # The key should exist in both or neither. In FLCS the record number always exists.
        # So check the value based on the row return.
        val_fix = cfix.get_value()
        if ret_row == wiredtiger.WT_NOTFOUND:
            self.assertEqual(val_fix, 0)
        else:
            val_row = crow.get_value()
            self.assertEqual(val_row, 'newval')
            self.assertEqual(val_fix, 98)
        cfix.close()
        crow.close()
        self.pr('close new conn')
        new_conn.close()

        # Reopen the connection to force all content to disk.
        #self.reopen_conn()
        self.pr('end test, close orig conn')

if __name__ == '__main__':
    wttest.run()
