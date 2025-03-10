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

# test_backup27.py
# Test selective backup with history store contents. Recovering a partial backup should
# clear the history entries of the table that does not exist in the backup directory.
class test_backup27(backup_base):
    dir='backup.dir'                    # Backup directory name
    newuri="table:newuri"
    newuri1="table:newuri1"
    newuri_file="newuri.wt"
    newuri1_file="newuri1.wt"
    uri="table:uri"
    uri2="table:uri2"

    def add_timestamp_data(self, uri, key, val, timestamp):
        self.session.begin_transaction()
        c = self.session.open_cursor(uri, None, None)
        for i in range(0, 1):
            k = key + str(i)
            v = val
            c[k] = v
        c.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(timestamp))

    def validate_timestamp_data(self, session, uri, key, expected_val, expected_err, timestamp):
        session.begin_transaction('read_timestamp=' + self.timestamp_str(timestamp))
        c = session.open_cursor(uri, None, None)
        for i in range(0, 1000):
            k = key + str(i)
            c.set_key(k)
            self.assertEqual(c.search(), expected_err)
            if (expected_err == 0):
                self.assertEqual(c.get_value(), expected_val)
        c.close()
        session.commit_transaction()

    def add_one_timestamp_data(self, uri, key, val, timestamp):
        self.session.begin_transaction()
        c = self.session.open_cursor(uri, None, None)
        c[key] = val
        c.close()
        self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(timestamp))

    def test_backup27(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        self.add_one_timestamp_data(self.uri, "keyA", "v5", 5)
        self.add_one_timestamp_data(self.uri, "keyA", "v8", 8)
        self.session.checkpoint()

        self.session.drop(self.uri)
        self.reopen_conn()

        self.session.create(self.uri2,  "key_format=S,value_format=S")
        self.add_one_timestamp_data(self.uri2, "keyA", "v10", 15)
        self.session.checkpoint()
        #self.reopen_conn()

        self.session.begin_transaction('read_timestamp=' + self.timestamp_str(6))
        c = self.session.open_cursor(self.uri2, None)
        c.set_key("keyA")
        result = c.search()

        if result == 0: 
            value = c.get_value()
            self.prout("value: " + str(value))
        else: 
            self.prout("error: " + str(result))
        self.session.rollback_transaction()
        # self.pr("ssssssss---")
        # # Pin oldest and stable to timestamp 1.
        # # self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
        # #     ',stable_timestamp=' + self.timestamp_str(1))
        # self.session.create(self.uri, "key_format=S,value_format=S")
        
        # self.add_one_timestamp_data(self.uri, "keyA", "v3", 3)
        # self.add_one_timestamp_data(self.uri, "keyA", "v5", 5)
        # self.add_one_timestamp_data(self.uri, "keyA", "v8", 8)

        # # self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(8) +
        # #     ',stable_timestamp=' + self.timestamp_str(8))
        # # self.session.create(self.uri, "key_format=S,value_format=S")
        # self.session.checkpoint()

        # self.session.drop(self.uri)
        # self.reopen_conn()
        # # self.conn.set_timestamp('oldest_timestamp=' + self.timestamp_str(1) +
        # #     ',stable_timestamp=' + self.timestamp_str(1))

        # self.session.create(self.uri,  "key_format=S,value_format=S")

        # self.add_one_timestamp_data(self.uri, "keyA", "v8", 8)
        # self.add_one_timestamp_data(self.uri, "keyA", "v10", 10)
        # self.session.checkpoint()

        # self.session.begin_transaction('read_timestamp=9')
        # c = self.session.open_cursor(self.uri, None)
        # c.set_key("keyA")
        # self.pr("hhhhhhhhhhhhh---")
        # result = c.search()

        # if result == 0: 
        #     value = c.get_value()
        #     self.pr("value: " + str(value))
        #     self.session.rollback_transaction()
        # else: 
        #     self.pr("error: " + str(result))

        # self.session.begin_transaction();
        # result = c.search()
        # self.pr("searched result for keyA: " + str(result));
        #   # Create 3 tables. 
        # self.session.create(self.uri, "key_format=S,value_format=S")
        # self.session.create(self.newuri, "key_format=S,value_format=S")
        # self.session.create(self.newuri1, "key_format=S,value_format=S")

        # # Add data to the tables.
        # self.add_timestamp_data(self.uri, "keyA", "valA", 1)
        # self.add_timestamp_data(self.newuri, "key", "val", 2)
        # self.add_timestamp_data(self.newuri1, "keyB", "valB", 3)

        # # Add updates to the same records.
        # self.add_timestamp_data(self.uri, "keyA", "uriA", 7)
        # self.add_timestamp_data(self.newuri, "key", "newuri", 5)
        # self.add_timestamp_data(self.newuri1, "keyB", "newuri1B", 9)
        # self.session.checkpoint()

        # # Drop one of the tables. - 'newuri1'. Now this entry gets removed from the metadata.
        # # Only newuri and uri should be present in the metadata.
        # self.session.begin_transaction()
        # self.session.drop(self.newuri1)

        # self.session.commit_transaction('commit_timestamp=' + self.timestamp_str(7))

        # # Stable timestamp at 10, so that we can retain history store data.
        # self.conn.set_timestamp('stable_timestamp=' + self.timestamp_str(10))
        # self.session.checkpoint()
        
        # #self.reopen_conn()
        # self.session.create("table:src_zunyi", "key_format=S,value_format=S")

        # os.mkdir(self.dir)

        # # Now copy the files using selective backup. This should not include one of the tables.
        # # We only wish to have `uri` present in the selective db, don't consider the others (we dont have to include self.newuri1_file here)
        # all_files = self.take_selective_backup(self.dir, [self.newuri_file, self.newuri1_file]) 


        # # After the full backup, open and partially recover the backup database on only one table.
        # backup_conn = self.wiredtiger_open(self.dir, "backup_restore_target=[\"{0}\"]".format(self.uri))
        # bkup_session = backup_conn.open_session()
        # bkup_session.create("table:zunyi", "key_format=S,value_format=S")
      
        # In the history store data still exists for the `newuri1` table that was not included in the backup.
        # Ideally, the history store data should be removed for the table that was not included in the backup.
        # However, since we dropped the table, and the metadata entry got removed, there is no way to know if
        # the history store data for that table needs to be removed. So, the history store data is retained. 



#drop a file won't remove anything on hs, but it will be cleaned up later. drop->close->reopen wiredtiger will this clean up the hs?
