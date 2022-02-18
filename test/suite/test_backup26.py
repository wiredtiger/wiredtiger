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

import os, wiredtiger, wttest
from wtbackup import backup_base
from wtscenario import make_scenarios

# test_backup26.py
# Test selective backup with large amount of tables. Recovering a partial backup should take
# longer when there are more active tables. Therefore test recovery correctness in a partial backup.
class test_backup26(backup_base):
    dir='backup.dir'                    # Backup directory name
    uri="table_backup"
    ntables = 10000 if wttest.islongtest() else 500

    remove_list = [
        ('empty remove list', dict(remove_list=False)),
        ('remove list', dict(remove_list=True)),
    ]
    scenarios = make_scenarios(remove_list)

    def validate_timestamp_data(self, session, uri, key, expected_err, timestamp):
        session.begin_transaction('read_timestamp=' + self.timestamp_str(timestamp))
        c = session.open_cursor(uri, None, None)
        for i in range(0, 1000):
            k = key + str(i)
            c.set_key(k)
            self.assertEqual(c.search(), expected_err)
        c.close()
        session.commit_transaction()

    def test_backup26(self):
        selective_remove_list = []
        for i in range(0, self.ntables):
            self.session.create("table:{0}".format(self.uri + str(i)), "key_format=S,value_format=S")
            self.add_data("table:{0}".format(self.uri + str(i)), "key", "val")
            if (self.remove_list and i != 0):
                selective_remove_list.append(self.uri + str(i))
        self.session.checkpoint()

        os.mkdir(self.dir)

        # Now copy the files using full backup. This should not include the tables inside the remove list.
        all_files = self.take_selective_backup(self.dir, [remove_uri + ".wt" for remove_uri in selective_remove_list])

        # After the full backup, open and recover the backup database.
        backup_conn = self.wiredtiger_open(self.dir, "backup_load_partial=true")
        bkup_session = backup_conn.open_session()
        for remove_uri in selective_remove_list:
            # Open the cursor and expect failure since file doesn't exist.
            self.assertRaisesException(
                wiredtiger.WiredTigerError,lambda: bkup_session.open_cursor("table:" + remove_uri, None, None))

        # Only one table should be restored. Open cursor to check that it doesn't throw an error.
        c = bkup_session.open_cursor("table:{0}".format(self.uri + "0"), None, None)
        
        c.close()
        backup_conn.close()

if __name__ == '__main__':
    wttest.run()
