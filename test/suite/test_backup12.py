#!/usr/bin/env python
#
# Public Domain 2014-2020 MongoDB, Inc.
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
import os, shutil
from helper import compare_files
from wtbackup import backup_base
from wtdataset import simple_key
from wtscenario import make_scenarios

# test_backup12.py
# Test cursor backup with a block-based incremental cursor.
class test_backup12(backup_base):
    conn_config='cache_size=1G,log=(enabled,file_max=100K)'
    dir='backup.dir'                    # Backup directory name
    logmax="100K"
    uri="table:test"
    uri2="table:test2"
    uri_rem="table:test_rem"

    pfx = 'test_backup'
    # Set the key and value big enough that we modify a few blocks.
    bigkey = 'Key' * 100
    bigval = 'Value' * 100

    nops = 1000
    def test_backup12(self):

        self.session.create(self.uri, "key_format=S,value_format=S")
        self.session.create(self.uri2, "key_format=S,value_format=S")
        self.session.create(self.uri_rem, "key_format=S,value_format=S")
        self.add_data(self.uri, self.bigkey, self.bigval, True)
        self.add_data(self.uri2, self.bigkey, self.bigval, True)
        self.add_data(self.uri_rem, self.bigkey, self.bigval, True)

        # Open up the backup cursor. This causes a new log file to be created.
        # That log file is not part of the list returned. This is a full backup
        # primary cursor with incremental configured.
        os.mkdir(self.dir)
        #
        # Note, this first backup is actually done before a checkpoint is taken.
        #
        config = 'incremental=(enabled,granularity=1M,this_id="ID1")'
        bkup_c = self.session.open_cursor('backup:', None, config)

        # Add more data while the backup cursor is open.
        self.add_data(self.uri, self.bigkey, self.bigval, True)

        # Now copy the files returned by the backup cursor.
        all_files = []
        while True:
            ret = bkup_c.next()
            if ret != 0:
                break
            newfile = bkup_c.get_key()
            sz = os.path.getsize(newfile)
            self.pr('Copy from: ' + newfile + ' (' + str(sz) + ') to ' + self.dir)
            shutil.copy(newfile, self.dir)
            all_files.append(newfile)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)

        # Now open a duplicate backup cursor.
        # We *can* use a log target duplicate on an incremental primary backup so that
        # a backup process can get all the log files that occur while that primary cursor
        # is open.
        config = 'target=("log:")'
        dupc = self.session.open_cursor(None, bkup_c, config)
        dup_logs = []
        while True:
            ret = dupc.next()
            if ret != 0:
                break
            newfile = dupc.get_key()
            self.assertTrue("WiredTigerLog" in newfile)
            sz = os.path.getsize(newfile)
            if (newfile not in all_files):
                self.pr('DUP: Copy from: ' + newfile + ' (' + str(sz) + ') to ' + self.dir)
                shutil.copy(newfile, self.dir)
            # Record all log files returned for later verification.
            dup_logs.append(newfile)
            all_files.append(newfile)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        dupc.close()
        bkup_c.close()

        # Add more data.
        self.add_data(self.uri, self.bigkey, self.bigval, True)
        self.add_data(self.uri2, self.bigkey, self.bigval, True)

        # Drop a table.
        self.session.drop(self.uri_rem)

        # Now do an incremental backup.
        config = 'incremental=(src_id="ID1",this_id="ID2")'
        bkup_c = self.session.open_cursor('backup:', None, config)
        self.pr('Open backup cursor ID1')
        bkup_files = []
        while True:
            ret = bkup_c.next()
            if ret != 0:
                break
            newfile = bkup_c.get_key()
            config = 'incremental=(file=' + newfile + ')'
            self.pr('Open incremental cursor with ' + config)
            dup_cnt = 0
            dupc = self.session.open_cursor(None, bkup_c, config)
            bkup_files.append(newfile)
            all_files.append(newfile)
            while True:
                ret = dupc.next()
                if ret != 0:
                    break
                incrlist = dupc.get_keys()
                offset = incrlist[0]
                size = incrlist[1]
                curtype = incrlist[2]
                # 1 is WT_BACKUP_FILE
                # 2 is WT_BACKUP_RANGE
                self.assertTrue(curtype == 1 or curtype == 2)
                if curtype == 1:
                    self.pr('Copy from: ' + newfile + ' (' + str(sz) + ') to ' + self.dir)
                    shutil.copy(newfile, self.dir)
                else:
                    self.pr('Range copy file ' + newfile + ' offset ' + str(offset) + ' len ' + str(size))
                    rfp = open(newfile, "r+b")
                    wfp = open(self.dir + '/' + newfile, "w+b")
                    rfp.seek(offset, 0)
                    wfp.seek(offset, 0)
                    buf = rfp.read(size)
                    wfp.write(buf)
                    rfp.close()
                    wfp.close()
                dup_cnt += 1
            dupc.close()
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        bkup_c.close()

        # We need to remove files in the backup directory that are not in the current backup.
        all_set = set(all_files)
        bkup_set = set(bkup_files)
        rem_files = list(all_set - bkup_set)
        for l in rem_files:
            self.pr('Remove file: ' + self.dir + '/' + l)
            os.remove(self.dir + '/' + l)
        # After the full backup, open and recover the backup database.
        backup_conn = self.wiredtiger_open(self.dir)
        backup_conn.close()

if __name__ == '__main__':
    wttest.run()
