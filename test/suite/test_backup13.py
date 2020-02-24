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
from suite_subprocess import suite_subprocess
from wtdataset import simple_key
from wtscenario import make_scenarios
import glob

# test_backup14.py
# Test cursor backup with a block-based incremental cursor.
class test_backup14(wttest.WiredTigerTestCase, suite_subprocess):
    conn_config='cache_size=1G,log=(enabled,file_max=100K)'
    dir='backup.dir'                    # Backup directory name
    logmax="100K"
    uri="table:main"
    uri2="table:extra"
    uri_logged="table:logged_table"
    uri_not_logged="table:not_logged_table"
    full_out = "./backup_block_full"
    incr_out = "./backup_block_incr"
    bkp_home = "WT_BLOCK"
    home_full = "WT_BLOCK_LOG_FULL"
    home_incr = "WT_BLOCK_LOG_INCR"
    logpath = "logpath"
    nops=10
    mult=0
    max_iteration=7
    counter=0
    new_table=False
    initial_backup=False
    all_files = []
    bkup_files = []

    pfx = 'test_backup'
    # Set the key and value big enough that we modify a few blocks.
    bigkey = 'Key' * 100
    bigval = 'Value' * 100

    def compare_backups(self, t_uri):
        #
        # Run wt dump on full backup directory
        #
        full_backup_out = self.full_out + '.' + str(self.counter)
        if self.counter == 0:
            self.runWt(['-R', '-h', self.home, 'dump', t_uri], outfilename=full_backup_out)
        else:
            home_dir = self.home_full + '.' + str(self.counter)
            self.runWt(['-R', '-h', home_dir, 'dump', t_uri], outfilename=full_backup_out)
        #
        # Run wt dump on incremental backup directory
        #
        incr_backup_out = self.incr_out + '.' + str(self.counter)
        home_dir = self.home_incr + '.' + str(self.counter)
        self.runWt(['-R', '-h', home_dir, 'dump', t_uri], outfilename=incr_backup_out)

        #self.assertEqual(True,
            #compare_files(self, full_backup_out, incr_backup_out))
        if compare_files(self, full_backup_out, incr_backup_out) == True:
            self.pr(full_backup_out + " & " + full_backup_out + " are identical.")
        else:
            self.pr(" ***** NOT identical ****")

    def setup_directories(self):

        for i in range(0, self.max_iteration):
            remove_dir = self.home_incr + '.' + str(i)
            self.pr("Remove " + remove_dir)

            create_dir = self.home_incr + '.' + str(i) + '/' + self.logpath
            if os.path.exists(remove_dir):
                os.remove(remove_dir)
            os.makedirs(create_dir)

            if i == 0:
                continue
            
            remove_dir = self.home_full + '.' + str(i)
            create_dir = self.home_full + '.' + str(i) + '/' + self.logpath
            if os.path.exists(remove_dir):
                os.remove(remove_dir)
            os.makedirs(create_dir)
    
    def process_finalize_files(self):
        if self.initial_backup == True:
            return

        # We need to remove files in the backup directory that are not in the current backup.
        all_set = set(self.all_files)
        bkup_set = set(self.bkup_files)
        rem_files = list(all_set - bkup_set)
        for l in rem_files:
            path = 'WT_BLOCK_LOG_*/'
            if ("WiredTigerLog" in l):
                path = path + self.logpath
            path = path + '/' + l
            self.pr('Remove file: ' + path)
            fileList = glob.glob(path, recursive=True)
            for filePath in fileList:
                os.remove(filePath)
            #os.remove(self.dir + '/' + l)
            #os.remove(path)

    def take_full_backup(self):

        if self.counter != 0:
            hdir = self.home_full + '.' + str(self.counter)
        else:
            hdir = self.home_incr

        if self.initial_backup == True:
            buf = 'incremental=(granularity=1M,enabled=true,this_id=ID0)'
            cursor = self.session.open_cursor('backup:', None, buf)
        else:
            cursor = self.session.open_cursor('backup:', None, None)

        #self.all_files = []
        while True:
            ret = cursor.next()
            if ret != 0:
                break
            newfile = cursor.get_key()
            # If it is log file, prpend the path for cp
            if ("WiredTigerLog" in newfile):
                #copy_file = self.logpath + '/' + newfile
                copy_file = newfile
            else:
                copy_file = newfile
            
            if self.counter == 0:
                # Take a full bakcup into each incremental directory
                for i in range(0, self.max_iteration):
                    #copy_from = self.home + '/' + copy_file
                    copy_from = copy_file
                    if ("WiredTigerLog" in copy_file):
                        copy_to = self.home_incr + '.' + str(i) + '/' + self.logpath
                    else:
                        copy_to = self.home_incr + '.' + str(i)
                    shutil.copy(copy_from, copy_to)
            else:
                #copy_from = self.home + '/' + copy_file
                copy_from = copy_file
                if ("WiredTigerLog" in copy_file):
                    copy_to = hdir + '/' + self.logpath
                else:
                    copy_to = hdir

                shutil.copy(copy_from, copy_to)
            self.all_files.append(newfile)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        cursor.close()
        #self.process_finalize_files()

    def take_incr_backup(self):
        buf = 'incremental=(src_id="ID' +  str(self.counter-1) + '",this_id="ID' + str(self.counter) + '")'
        #print("Increment Backup Config : " , buf)
        bkup_c = self.session.open_cursor('backup:', None, buf)
        #self.bkup_files = []
        while True:
            ret = bkup_c.next()
            if ret != 0:
                break
            newfile = bkup_c.get_key()
            h = self.home_incr + '.0'
            if ("WiredTigerLog" in newfile):
                #copy_from = self.home + '/' + self.logpath + '/' + newfile
                copy_from = newfile
                copy_to = h + '/' + self.logpath
            else:
                #copy_from = self.home + '/' + newfile
                copy_from = newfile
                copy_to = h
        
            shutil.copy(copy_from, copy_to)

            first = True

            config = 'incremental=(file=' + newfile + ')'
            #self.pr('Open incremental cursor with Config ' + config)
            dup_cnt = 0
            incr_c = self.session.open_cursor(None, bkup_c, config)
            self.bkup_files.append(newfile)
            self.all_files.append(newfile)
            while True:
                ret = incr_c.next()
                if ret != 0:
                    break
                incrlist = incr_c.get_keys()
                offset = incrlist[0]
                size = incrlist[1]
                curtype = incrlist[2]
                # 1 is WT_BACKUP_FILE
                # 2 is WT_BACKUP_RANGE
                self.assertTrue(curtype == 1 or curtype == 2)
                if curtype == 1:
                    if first == True:
                        h = self.home_incr + '.' + str(self.counter)
                        first = False

                    #self.pr('Copy from: ' + newfile + ' (' + str(sz) + ') to ' + self.dir)
                    if ("WiredTigerLog" in newfile):
                        #copy_from = self.home + '/' + self.logpath + '/' + newfile
                        copy_from = newfile
                        copy_to = h + '/' + self.logpath
                    else:
                        #copy_from = self.home + '/' + newfile
                        copy_from = newfile
                        copy_to = h
                    shutil.copy(copy_from, copy_to)
                else:
                    self.pr('Range copy file ' + newfile + ' offset ' + str(offset) + ' len ' + str(size))
                    #write_from = self.home + '/' + newfile
                    write_from = newfile
                    write_to = self.home_incr + '.' + str(self.counter) + '/' + newfile
                    rfp = open(write_from, "r+b")
                    wfp = open(write_to, "w+b")
                    rfp.seek(offset, 0)
                    wfp.seek(offset, 0)
                    buf = rfp.read(size)
                    wfp.write(buf)
                    rfp.close()
                    wfp.close()
                dup_cnt += 1
            self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
            incr_c.close()

            # For each file, we want to copy the file into each of the later incremental directories
            for i in range(self.counter, self.max_iteration):
                h = self.home_incr + '.' + str(i)
                if ("WiredTigerLog" in newfile):
                    #copy_from = self.home + '/' + self.logpath + '/' + newfile
                    copy_from = newfile
                    copy_to = h + '/' + self.logpath
                else:
                    #copy_from = self.home + '/' + newfile
                    copy_from = newfile
                    copy_to = h
                shutil.copy(copy_from, copy_to)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        bkup_c.close()
        #self.process_finalize_files()


    def add_data(self, uri, bulk_option):
        c = self.session.open_cursor(uri, None, bulk_option)
        for i in range(0, self.nops):
            num = i + (self.counter * self.nops)
            #key = self.bigkey + str(num)
            key = str(num)
            #val = self.bigval + str(num)
            val = str(num)
            c[key] = val
        #self.session.checkpoint()
        c.close()
        # Increase the multiplier so that later calls insert unique items.
        if self.initial_backup == False:
            self.counter += 1

    def remove_data(self):
        c = self.session.open_cursor(self.uri)
        for i in range(0, self.nops):
            num = i + (self.counter * self.nops)
            key = self.bigkey + str(num)
            c.set_key(key)
            self.assertEquals(c.remove(), 0)
        c.close()
        self.counter += 1

    #
    # This function will add records to the table (table:main), take incremental/full backups and
    # validate the backups.
    #
    def add_data_validate_backups(self):
        self.pr('Adding initial data')
        self.initial_backup = True
        self.add_data(self.uri, None)
        self.take_full_backup()
        self.initial_backup = False
        self.session.checkpoint()

        self.add_data(self.uri, None)
        self.take_full_backup()
        self.take_incr_backup()
        self.compare_backups(self.uri)

    #
    # This function will remove all the records from table (table:main), take backup and validate the
    # backup.
    #
    def remove_all_records_validate(self):
        self.remove_data()
        self.take_full_backup()
        self.take_incr_backup()
        self.compare_backups(self.uri)

    #
    # This function will drop the existing table uri (table:main) that is part of the backups and
    # create new table uri2 (table:extra), take incremental backup and validate.
    #
    def drop_old_add_new_table(self):

        # Drop main table.
        self.session.drop(self.uri)

        # Create uri2 (table:extra)
        self.session.create(self.uri2, "key_format=S,value_format=S")

        self.new_table = True
        self.add_data(self.uri2, None)
        self.take_incr_backup()

        full_backup_out = 'OutFile.txt'
        # Assert if the dropped table (table:main) exists in the incremental folder.
        self.runWt(['-R', '-h', self.home, 'list'], outfilename=full_backup_out)
        ret = os.system("grep " + self.uri + " " + full_backup_out)
        self.assertNotEqual(ret, 0, self.uri + " dropped, but table exists in " + self.home)

    #
    # This function will create previously dropped table uri (table:main) and add different content to
    # it, take backups and validate the backups.
    #
    def create_dropped_table_add_new_content(self):
        self.session.create(self.uri, "key_format=S,value_format=S")
        self.add_data(self.uri, None)
        self.take_full_backup()
        self.take_incr_backup()
        self.compare_backups(self.uri)

    #
    # This function will insert bulk data in logged and not-logged table, take backups and validate the
    # backups.
    #
    def insert_bulk_data(self):
        #
        # Insert bulk data into logged table.
        #
        self.session.create(self.uri_logged, "key_format=S,value_format=S")
        self.add_data(self.uri_logged, 'bulk')
        self.take_full_backup()
        self.take_incr_backup()
        self.compare_backups(self.uri_logged)

        #
        # Insert bulk data into not-logged table.
        #
        self.session.create(self.uri_not_logged, "key_format=S,value_format=S,log=(enabled=false)")
        self.add_data(self.uri_not_logged, 'bulk')
        self.take_full_backup()
        self.take_incr_backup()
        self.compare_backups(self.uri_not_logged)

    def test_backup14(self):
        os.mkdir(self.bkp_home)
        self.home = self.bkp_home
        self.session.create(self.uri, "key_format=S,value_format=S")

        self.setup_directories()

        self.pr('*** Add data, checkpoint, take backups and validate ***')
        self.add_data_validate_backups()

        self.pr('*** Remove old records and validate ***')
        self.remove_all_records_validate()

        self.pr('*** Drop old and add new table ***')
        self.drop_old_add_new_table()

        self.pr('*** Create previously dropped table and add new content ***')
        self.create_dropped_table_add_new_content()

        self.pr('*** Insert data into Logged and Not-Logged tables ***')
        self.insert_bulk_data()

if __name__ == '__main__':
    wttest.run()
