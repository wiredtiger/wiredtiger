#!/usr/bin/env python
#
# Public Domain 2014-2016 MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.

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

import glob
import os
import shutil
import string
from suite_subprocess import suite_subprocess
import wiredtiger, wttest
from wiredtiger import stat
from helper import compare_files,\
    complex_populate, complex_populate_lsm, simple_populate

# test_backup06.py
#    Test the backup_schema_protect configuration setting.
class test_backup06(wttest.WiredTigerTestCase, suite_subprocess):
    conn_config = 'statistics=(fast)'
    num_tables = 0
    pfx='test_backup'

    # We try to do some schema operations.  Have some well
    # known names.
    schema_uri = 'file:schema_test'
    rename_uri = 'file:new_test'
    trename_uri = 'table:new_test'

    fobjs = [
        ( 'file:' + pfx + '.1', simple_populate),
        ( 'file:' + pfx + '.2', simple_populate),
    ]
    tobjs = [
        ('table:' + pfx + '.3', simple_populate),
        ('table:' + pfx + '.4', simple_populate),
        ('table:' + pfx + '.5', complex_populate),
        ('table:' + pfx + '.6', complex_populate),
        ('table:' + pfx + '.7', complex_populate_lsm),
        ('table:' + pfx + '.8', complex_populate_lsm),
    ]

    # Populate a set of objects.
    def populate(self):
        for i in self.fobjs:
            i[1](self, i[0], 'key_format=S', 100)
            self.num_tables += 1
        for i in self.tobjs:
            i[1](self, i[0], 'key_format=S', 100)
            self.num_tables += 1

    # Test that the open handle count does not change.
    def test_cursor_open_handles(self):
        self.populate()
        # Close and reopen the connection so the populate dhandles are
        # not in the list.
        self.reopen_conn()

        # Confirm that opening a backup cursor does not open
        # file handles.
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        dh_before = stat_cursor[stat.conn.dh_conn_handle_count][2]
        stat_cursor.close()
        cursor = self.session.open_cursor('backup:', None, None)
        stat_cursor = self.session.open_cursor('statistics:', None, None)
        dh_after = stat_cursor[stat.conn.dh_conn_handle_count][2]
        stat_cursor.close()
        self.assertEqual(dh_after == dh_before, True)
        cursor.close()

    def test_cursor_schema_protect(self):
        schema_uri = 'file:schema_test'
        rename_uri = 'file:new_test'
        trename_uri = 'table:new_test'

        #
        # Set up a number of tables.  Close and reopen the connection so that
        # we do not have open dhandles.  Then we want to open a backup cursor
        # testing both with and without the configuration setting.
        # We want to confirm that we open data handles when using schema
        # protection and we do not open the data handles when set to false.
        # We also want to make sure we detect and get an error when set to
        # false.  When set to true the open handles protect against schema
        # operations.
        self.populate()
        cursor = self.session.open_cursor('backup:', None, None);
        # Check that we can create.
        self.session.create(schema_uri, None)
        for i in self.fobjs:
            self.assertRaises(wiredtiger.WiredTigerError,
                lambda: self.session.drop(i[0], None))
            self.assertRaises(wiredtiger.WiredTigerError,
                lambda: self.session.rename(i[0], rename_uri))
        for i in self.tobjs:
            self.assertRaises(wiredtiger.WiredTigerError,
                lambda: self.session.drop(i[0], None))
            self.assertRaises(wiredtiger.WiredTigerError,
                lambda: self.session.rename(i[0], trename_uri))
        cursor.close()

    # Test cursor reset runs through the list twice.
    def test_cursor_reset(self):
        self.populate()
        cursor = self.session.open_cursor('backup:', None, None)
        i = 0
        while True:
            ret = cursor.next()
            if ret != 0:
                break
            i += 1
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        total = i * 2
        cursor.reset()
        while True:
            ret = cursor.next()
            if ret != 0:
                break
            i += 1
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        self.assertEqual(i, total)
        cursor.close()

if __name__ == '__main__':
    wttest.run()
