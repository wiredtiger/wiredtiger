#!/usr/bin/env python
#
# Public Domain 2014-2021 MongoDB, Inc.
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
import os, glob, shutil
import wttest, wiredtiger
from suite_subprocess import suite_subprocess
from helper import compare_files

# Shared base class used by backup tests.
class backup_base(wttest.WiredTigerTestCase, suite_subprocess):
    cursor_config = None        # a config string for cursors
    mult = 0                    # counter to have variance in data
    nops = 100                  # number of operations added to uri

    # counter to used to produce unique backup ids the backup id, and is
    # generally used only for using add_data() first time.
    counter = 0
    # To determine whether to increase/decrease counter, which determines
    initial_backup = False
    # Used for populate function
    rows = 100
    populate_big = None

    # Used for tests that test multiple incremental backups, copying into future
    # incremental backup directories, is used in conjunction with id argument that is
    # passed in
    max_iteration = 0
    #specify a logpath directory to be used to place wiredtiger log files
    logpath=''
    #
    # Add data to the given uri.
    # Allows the option for doing a session checkpoint after adding data.
    #
    def add_data(self, uri, key, val, do_checkpoint=False):
        assert(self.nops != 0)
        c = self.session.open_cursor(uri, None, self.cursor_config)
        for i in range(0, self.nops):
            num = i + (self.mult * self.nops)
            k = key + str(num)
            v = val + str(num)
            c[k] = v
        c.close()
        if do_checkpoint:
            self.session.checkpoint()
        # Increase the counter so that later backups have unique ids.
        if not self.initial_backup and self.max_iteration != 0:
            self.counter += 1
        # Increase the multiplier so that later calls insert unique items.
        self.mult += 1
    
    #
    # Populate a set of objects.
    #
    def populate(self, objs, do_checkpoint=False, skiplsm=False):
        cg_config = ''
        for i in objs:
            if len(i) > 2:
                if i[2] and skiplsm:
                    continue
                if i[2] == self.populate_big:
                    self.rows = 50000 # Big Object
                else:
                    self.rows = 1000  # Small Object
            if len(i) > 3:
                cg_config = i[3] 
            i[1](self, i[0], self.rows, cgconfig = cg_config).populate()

        # Backup needs a checkpoint
        if do_checkpoint:
            self.session.checkpoint()

    #
    # Set up all the directories needed for the test. We have a full backup directory for each
    # iteration and an incremental backup for each iteration. That way we can compare the full and
    # incremental each time through.
    #
    def setup_directories(self, home_incr, home_full):
        for i in range(0, self.max_iteration):
            # The log directory is a subdirectory of the home directory,
            # creating that will make the home directory also.

            home_incr_dir = home_incr + '.' + str(i)
            if os.path.exists(home_incr_dir):
                os.remove(home_incr_dir)
            os.makedirs(home_incr_dir + '/' + self.logpath)

            if i == 0:
                continue
            home_full_dir = home_full + '.' + str(i)
            if os.path.exists(home_full_dir):
                os.remove(home_full_dir)
            os.makedirs(home_full_dir + '/' + self.logpath)
    
    #
    # Check that a URI doesn't exist, both the meta-data and the file names.
    #
    def confirmPathDoesNotExist(self, uri, dir):
        conn = self.wiredtiger_open(dir)
        session = conn.open_session()
        self.assertRaises(wiredtiger.WiredTigerError,
            lambda: session.open_cursor(uri, None, None))
        conn.close()

        self.assertEqual(
            glob.glob(dir + '*' + uri.split(":")[1] + '*'), [],
            'confirmPathDoesNotExist: URI exists, file name matching \"' +
            uri.split(":")[1] + '\" found')

    #
    # Copy a file into given directory
    #
    def copy_file(self, file, dir):
        copy_from = file
        # If it is log file, prepend the path.
        if self.logpath and "WiredTigerLog" in file:
            copy_to = dir + '/' + self.logpath
        else:
            copy_to = dir
        shutil.copy(copy_from, copy_to)

    #
    # Uses a backup cursor to perform a full backup, by iterating through the cursor
    # grabbing files to copy over into a given directory.
    # Optional arguments:
    # backup_cur: A backup cursor that can be given into the function, but function caller
    #    holds reponsibility of closing the cursor. 
    #
    def take_full_backup(self, backup_dir, backup_cur=None):
        #
        # First time through we take a full backup into the incremental directories. Otherwise only
        # into the appropriate full directory.
        #
        bkup_c = backup_cur
        if backup_cur == None:
            config = None
            if self.initial_backup and self.max_iteration != 0:
                config = 'incremental=(granularity=1M,enabled=true,this_id=ID0)'
            bkup_c = self.session.open_cursor('backup:', None, config)
        all_files = []
        # We cannot use 'for newfile in bkup_c:' usage because backup cursors don't have
        # values and adding in get_values returns ENOTSUP and causes the usage to fail.
        # If that changes then this, and the use of the duplicate below can change.
        while True:
            ret = bkup_c.next()
            if ret != 0:
                break
            newfile = bkup_c.get_key()
            if self.initial_backup and self.max_iteration != 0:
                # Take a full backup into each incremental directory
                for i in range(0, self.max_iteration):
                    self.copy_file(newfile, backup_dir  + '.' + str(i))
            else:
                sz = os.path.getsize(newfile)
                self.pr('Copy from: ' + newfile + ' (' + str(sz) + ') to ' + self.dir)
                self.copy_file(newfile, backup_dir)
                all_files.append(newfile)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        if backup_cur == None:
            bkup_c.close()
        return all_files

    #
    # Compare against two directory paths using the wt dump command.
    # The suffix allows the option to add distinctive tests adding suffix to both the output files and directories
    #
    def compare_backups(self, uri, base_dir_home, other_dir_home, suffix = None):
        sfx = ""
        if suffix != None:
            sfx = "." + suffix
        base_out = "./backup_base" + sfx
        base_dir = base_dir_home + sfx

        if os.path.exists(base_out):
            os.remove(base_out)

        # Run wt dump on base backup directory
        self.runWt(['-R', '-h', base_dir, 'dump', uri], outfilename=base_out)
        other_out = "./backup_other" + sfx
        if os.path.exists(other_out):
            os.remove(other_out)
        # Run wt dump on incremental backup
        other_dir = other_dir_home + sfx
        self.runWt(['-R', '-h', other_dir, 'dump', uri], outfilename=other_out)
        self.assertEqual(True, compare_files(self, base_out, other_out))

    #
    # Perform a block range copy for a given offset and file. 
    #
    # id: To distinguish between multiple incremental directories, used in conjunction with max_iterations.
    #
    def range_copy(self, filename, offset, size, id, backup_incr_dir):
        read_from = filename
        write_to = backup_incr_dir  + '/' + filename
        if self.max_iteration != 0:
            write_to = backup_incr_dir  + '.' + str(id) + '/' + filename
        rfp = open(read_from, "r+b")
        rfp.seek(offset, 0)
        buf = rfp.read(size)
        # Perform between previous incremental directory, to check that
        # the old file and the new file is different.
        if self.max_iteration != 0:
            old_to = backup_incr_dir + '.' + str(id - 1) + '/' + filename
            if os.path.exists(old_to):
                self.pr('RANGE CHECK file ' + old_to + ' offset ' + str(offset) + ' len ' + str(size))
                old_rfp = open(old_to, "r+b")
                old_rfp.seek(offset, 0)
                old_buf = old_rfp.read(size)
                old_rfp.close()
                # This assertion tests that the offset range we're given actually changed
                # from the previous backup.
                self.assertNotEqual(buf, old_buf)
        wfp = open(write_to, "w+b")
        wfp.seek(offset, 0)
        wfp.write(buf)
        rfp.close()
        wfp.close()

    #
    # With a given backup cursor, open an incremental block cursor to copy the blocks of a 
    # given file. If the type of file is WT_BACKUP_FILE, perform full copy into given directory, 
    # otherwise if type of file is WT_BACKUP_RANGE, perform partial copy of the file using range copy
    # Optional arguments:
    #   id: To distinguish between multiple incremental directories, used in conjunction with max_iterations.
    # Note: we return the sizes of WT_BACKUP_RANGE type files for tests that check for the consolidate config
    #
    def take_incr_backup_block(self, bkup_c, newfile, backup_incr_dir, id=0):
        config = 'incremental=(file=' + newfile + ')'
        # For each file listed, open a duplicate backup cursor and copy the blocks.
        incr_c = self.session.open_cursor(None, bkup_c, config)
        # For consolidate
        lens = []
        # We cannot use 'for newfile in incr_c:' usage because backup cursors don't have
        # values and adding in get_values returns ENOTSUP and causes the usage to fail.
        # If that changes then this, and the use of the duplicate below can change.
        while True:
            ret = incr_c.next()
            if ret != 0:
                break
            incrlist = incr_c.get_keys()
            offset = incrlist[0]
            size = incrlist[1]
            curtype = incrlist[2]
            self.assertTrue(curtype == wiredtiger.WT_BACKUP_FILE or curtype == wiredtiger.WT_BACKUP_RANGE)
            if curtype == wiredtiger.WT_BACKUP_FILE:
                # Copy the whole file.
                h = backup_incr_dir
                if self.max_iteration != 0:
                    h += '.' + str(id)
                self.copy_file(newfile, h)
            else:
                # Copy the block range.
                self.pr('Range copy file ' + newfile + ' offset ' + str(offset) + ' len ' + str(size))
                self.range_copy(newfile, offset, size, id, backup_incr_dir)
                lens.append(size)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        incr_c.close()
        return lens

    #
    # Given a backup cursor, open a log cursor, and copy all log files that are not
    # in the given log list. Return all the log files, for tests that do verification
    # Optional argument:
    #   truncate: perform truncate/archive option on the logs.
    #
    def take_log_backup(self, bkup_c, backup_dir, orig_logs, truncate=False):
        # Now open a duplicate backup cursor.
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
            if (newfile not in orig_logs):
                self.pr('DUP: Copy from: ' + newfile + ' (' + str(sz) + ') to ' + backup_dir)
                shutil.copy(newfile, backup_dir)
            # Record all log files returned for later verification.
            dup_logs.append(newfile)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        if truncate:
            self.session.truncate('log:', cursor, None, None)
        dupc.close()
        return dup_logs

    #
    # Open incremental backup cursor, with a given id and iterate through all the files
    # and perform incremental block copy for each of them. This function is used for tests 
    # that perform multiple incremental backups, and through utilising max_iteration
    #
    def take_incr_backup(self, backup_incr_dir, id):
        self.assertTrue(id > 0 and self.max_iteration > 0)
        # Open the backup data source for incremental backup.
        config = 'incremental=(src_id="ID' +  str(id - 1) + '",this_id="ID' + str(id) + '")'
        self.pr(config)
        bkup_c = self.session.open_cursor('backup:', None, config)

        # We cannot use 'for newfile in bkup_c:' usage because backup cursors don't have
        # values and adding in get_values returns ENOTSUP and causes the usage to fail.
        # If that changes then this, and the use of the duplicate below can change.
        while True:
            ret = bkup_c.next()
            if ret != 0:
                break
            newfile = bkup_c.get_key()
            self.copy_file(newfile, backup_incr_dir + '.0')
            self.take_incr_backup_block(bkup_c, newfile, backup_incr_dir, id)
            # For each file, we want to copy it into each of the later incremental directories.
            for i in range(id, self.max_iteration):
                h = backup_incr_dir + '.' + str(i)
                self.copy_file(newfile, h)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        bkup_c.close()

    #
    # Open incremental backup cursor, with an id and iterate through all the files
    # and perform incremental block copy for each of them. This function is used for tests
    # that require the information about the files. Default returns a list of filenames
    # Optional arguments:
    # consolidate: Used to add consolidate option to the cursor
    # ret_sizes: Flag to return sizes of files instead of default
    #
    def take_incr_backup_file(self, backup_incr_dir, id, consolidate=False, ret_sizes=False):
        self.assertTrue(id > 0)
        # Open the backup data source for incremental backup.
        config = 'incremental=(src_id="ID' +  str(id - 1) + '",this_id="ID' + str(id) + '"'
        if consolidate:
            config += ',consolidate=true'
        config += ')'
        self.pr(config)
        bkup_c = self.session.open_cursor('backup:', None, config)
        
        files_info = []
        # We cannot use 'for newfile in bkup_c:' usage because backup cursors don't have
        # values and adding in get_values returns ENOTSUP and causes the usage to fail.
        # If that changes then this, and the use of the duplicate below can change.
        while True:
            ret = bkup_c.next()
            if ret != 0:
                break
            newfile = bkup_c.get_key()
            file_sizes = self.take_incr_backup_block(bkup_c, newfile, backup_incr_dir)
            if ret_sizes:
                files_info += file_sizes
            else:
                files_info.append(newfile)
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        bkup_c.close()
        return files_info