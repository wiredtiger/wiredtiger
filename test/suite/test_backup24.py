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
import fnmatch, os, shutil
from helper import compare_files
from wtbackup import backup_base

# test_backup24.py
# Test recovering a selective backup with some logged tables, some not logged tables
# and creating more of each during backup.
class test_backup24(backup_base):
    dir='backup.dir'                    # Backup directory name
    config_log='key_format=S,value_format=S'
    config_nolog='key_format=S,value_format=S,log=(enabled=false)'
    log_t1="table:logged1"
    log_t2="table:logged2"
    log_tnew="table:loggednew"
    log_tnew_file="loggednew.wt"
    logmax="100K"
    nolog_t1="table:not1"
    nolog_t2="table:not2"
    nolog_t2_file="not2.wt"
    nolog_tnew="table:notnew"
    nolog_tnew_file="notnew.wt"
    newuri="table:newtable"

    # Create a large cache, otherwise this test runs quite slowly.
    def conn_config(self):
        return 'debug_mode=(table_logging=true),cache_size=1G,log=(enabled,file_max=%s,remove=false)' % \
            self.logmax

    # Run background inserts while running checkpoints repeatedly.
    def test_backup24(self):
        log2 = "WiredTigerLog.0000000002"

        # Create two logged and two not-logged tables.
        self.session.create(self.log_t1, self.config_log)
        self.session.create(self.log_t2, self.config_log)
        self.session.create(self.nolog_t1, self.config_nolog)
        self.session.create(self.nolog_t2, self.config_nolog)

        # Insert small amounts of data at a time stopping just after we
        # cross into log file 2.
        while not os.path.exists(log2):
            self.add_data(self.log_t1, 'key', 'value')
            self.add_data(self.log_t2, 'key', 'value')
            self.add_data(self.nolog_t1, 'key', 'value')
            self.add_data(self.nolog_t2, 'key', 'value')

        self.session.checkpoint()
        # Add more data after the checkpoint.
        self.add_data(self.log_t1, 'newkey', 'newvalue')
        self.add_data(self.log_t2, 'newkey', 'newvalue')
        self.add_data(self.nolog_t1, 'newkey', 'newvalue')
        self.add_data(self.nolog_t2, 'newkey', 'newvalue')

        # We allow creates during backup because the file doesn't exist
        # when the backup metadata is created on cursor open and the newly
        # created file is not in the cursor list.

        # Create and add data to a new table and then copy the files with a full backup.
        os.mkdir(self.dir)

        # Open the backup cursor and then create new tables and add data to them.
        # Then copy the files.
        bkup_c = self.session.open_cursor('backup:', None, None)

        # Now create and populate the new table. Make sure the log records
        # are on disk and will be copied to the backup.
        self.session.create(self.log_tnew, self.config_log)
        self.session.create(self.nolog_tnew, self.config_nolog)
        self.add_data(self.log_tnew, 'key', 'value')
        self.add_data(self.nolog_tnew, 'key', 'value')
        self.session.log_flush('sync=on')

        # Now copy the files using full backup but as a selective backup. We want the logged
        # tables but only the first not-logged table. Skip the second not-logged table.
        orig_logs = []
        while True:
            ret = bkup_c.next()
            if ret != 0:
                break
            newfile = bkup_c.get_key()
            self.assertNotEqual(newfile, self.log_tnew)
            self.assertNotEqual(newfile, self.nolog_tnew)
            if newfile == self.nolog_t2_file:
                continue
            sz = os.path.getsize(newfile)
            self.pr('Copy from: ' + newfile + ' (' + str(sz) + ') to ' + self.dir)
            shutil.copy(newfile, self.dir)
            if "WiredTigerLog" in newfile:
                orig_logs.append(newfile)

        # After copying files, catch up the logs.
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        dupc = self.session.open_cursor(None, bkup_c, 'target=("log:")')
        dup_logs = []
        while True:
            ret = dupc.next()
            if ret != 0:
                break
            newfile = dupc.get_key()
            self.assertTrue("WiredTigerLog" in newfile)
            sz = os.path.getsize(newfile)
            if (newfile not in orig_logs):
                self.pr('DUP: Copy from: ' + newfile + ' (' + str(sz) + ') to ' + self.dir)
                shutil.copy(newfile, self.dir)
            # Record all log files returned for later verification.
            dup_logs.append(newfile)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        dupc.close()
        bkup_c.close()
        flist = os.listdir(self.dir)
        self.pr("===== After log backup")
        for f in flist:
            self.pr(f)

        # After the full backup, open and recover the backup database.
        # Make sure we properly recover even though the log file will have
        # records for the newly created table file id.
        backup_conn = self.wiredtiger_open(self.dir)
        #backup_conn = self.wiredtiger_open(self.dir, 'verbose=(recovery)')
        flist = os.listdir(self.dir)
        self.pr("===== After recovery")
        for f in flist:
            self.pr(f)
        self.assertFalse(self.nolog_t2_file in flist)
        self.assertFalse(self.nolog_tnew_file in flist)
        backup_conn.close()

if __name__ == '__main__':
    wttest.run()