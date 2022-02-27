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
import os, re, time
from wtbackup import backup_base
from wtscenario import make_scenarios
from wtdataset import SimpleDataSet

# test_backup26.py
# Test selective backup with large amount of tables. Recovering a partial backup should take
# longer when there are more active tables. Also test recovery correctness with both file and
# table schemas in a partial backup.
class test_backup26(backup_base):
    dir='backup.dir'                    # Backup directory name
    uri="table_backup"
    ntables = 10000 if wttest.islongtest() else 500

    types = [
        ["table", dict(uri='table:')],
        ["file", dict(uri='file:')],
    ]

    # Percentage of tables in table format, and inverse for file format.
    percentage = [
        ('hundred_precent', dict(percentage=1)),
        ('fifty_percent', dict(percentage=0.5)),
        ('ten_percent', dict(percentage=0.1)),
        ('zero_percent', dict(percentage=0)),
    ]
    remove_list = [
        ('empty remove list', dict(ext=None, type=None)),
        ('table remove list', dict(ext=".wt",type="table:")),
        ('file remove list', dict(ext="",type="file:")),
    ]
    scenarios = make_scenarios(remove_list, percentage)

    def test_backup26(self):
        selective_remove_uri_list = []
        selective_file_list = []
        selective_remove_file_list = []
        for i in range(0, int(self.ntables * self.percentage)):
            uri = "file:{0}_file".format(self.uri + str(i))
            dataset = SimpleDataSet(self, uri, 100, key_format="S")
            dataset.populate()
            if (self.type and self.type == "file:"):
                selective_remove_uri_list.append(uri)
                selective_remove_file_list.append("{0}_file".format(self.uri + str(i)))
            else:
                selective_file_list.append(uri)

        for i in range(0, int(self.ntables * (1 - self.percentage))):
            uri = "table:{0}_table".format(self.uri + str(i))
            dataset = SimpleDataSet(self, uri, 100, key_format="S")
            dataset.populate()
            if (self.type and self.type == "table:"):
                selective_remove_uri_list.append(uri)
                selective_remove_file_list.append("{0}_table.wt".format(self.uri + str(i)))
            else:
                selective_file_list.append(uri)
        self.session.checkpoint()

        os.mkdir(self.dir)

        # Now copy the files using full backup. This should not include the tables inside the remove list.
        all_files = self.take_selective_backup(self.dir, selective_remove_file_list)

        starttime = time.time()        
        # After the full backup, open and recover the backup database.
        backup_conn = self.wiredtiger_open(self.dir, "backup_partial_restore=true")
        elapsed = time.time() - starttime
        self.pr("%s partial backup has taken %.2f seconds." % (str(self), elapsed))
        
        bkup_session = backup_conn.open_session()
        # Open the cursor from uris that was part of the selective backup and expect failure
        # since file doesn't exist.
        for remove_uri in selective_remove_uri_list:
            self.assertRaisesException(
                wiredtiger.WiredTigerError,lambda: bkup_session.open_cursor(remove_uri, None, None))

        # Open the cursors on tables that copied over to the backup directory. They should still 
        # recover properly.
        for uri in selective_file_list:
            c = bkup_session.open_cursor(uri, None, None)
            ds = SimpleDataSet(self, uri, 100, key_format="S")
            ds.check_cursor(c)
            c.close()
        backup_conn.close()

if __name__ == '__main__':
    wttest.run()
