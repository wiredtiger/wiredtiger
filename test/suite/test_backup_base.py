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
import os, shutil
import wiredtiger, wttest
from suite_subprocess import suite_subprocess
from helper import compare_files

# Shared base class used by backup tests.
class test_backup_base(wttest.WiredTigerTestCase, suite_subprocess):
    def copy_file(self, file, dir, logpath):
        copy_from = file
        # If it is log file, prepend the path.
        if logpath and "WiredTigerLog" in file:
            copy_to = dir + '/' + logpath
        else:
            copy_to = dir
        shutil.copy(copy_from, copy_to)

    #options: initial_backup, max_iteration, logpath
    #so if counter == 0, do full backup on each incremental directory (for setting up the test)
    #if counter != 0 do full backup on the full backup dir
    def take_full_backup(self, backup_dir, options):
        #
        # First time through we take a full backup into the incremental directories. Otherwise only
        # into the appropriate full directory.
        #
        buf = None
        if options.get('initial_backup'):
            buf = 'incremental=(granularity=1M,enabled=true,this_id=ID0)'
        bkup_c = self.session.open_cursor('backup:', None, buf)

        # We cannot use 'for newfile in bkup_c:' usage because backup cursors don't have
        # values and adding in get_values returns ENOTSUP and causes the usage to fail.
        # If that changes then this, and the use of the duplicate below can change.
        while True:
            ret = bkup_c.next()
            if ret != 0:
                break
            newfile = bkup_c.get_key()
            if options.get('initial_backup'):
                # Take a full backup into each incremental directory
                for i in range(0, options.get('max_iteration', 0)):
                    self.copy_file(newfile, backup_dir  + '.' + str(i), options.get('logpath'))
            else:
                self.copy_file(newfile, backup_dir, options.get('logpath'))
        self.assertEqual(ret, wiredtiger.WT_NOTFOUND)
        bkup_c.close()

    #
    # Set up all the directories needed for the test. We have a full backup directory for each
    # iteration and an incremental backup for each iteration. That way we can compare the full and
    # incremental each time through.
    #
    def setup_directories(self, max_iteration, home_incr, home_full, logpath):
        for i in range(0, max_iteration):
            # The log directory is a subdirectory of the home directory,
            # creating that will make the home directory also.

            home_incr_dir = home_incr + '.' + str(i)
            if os.path.exists(home_incr_dir):
                os.remove(home_incr_dir)
            os.makedirs(home_incr_dir + '/' + logpath)

            if i == 0:
                continue
            home_full_dir = home_full + '.' + str(i)
            if os.path.exists(home_full_dir):
                os.remove(home_full_dir)
            os.makedirs(home_full_dir + '/' + logpath)
    #
    # Compare against two directory paths using the wt dump command. If the second directory path is not provided
    # it checks if the compare
    #
    def compare_backups(self, uri, dir1, file1, run_recovery = False, dir2 = None, file2 = None):
        if os.path.exists(file1):
            os.remove(file1)
        # Run wt dump on first backup directory.
        self.runWt(['-R', '-h', dir1, 'dump', uri], outfilename=file1)
        # Run wt dump on second backup directory if provided.
        # If not provided, run dump on original directory, and check if we run recovery or not
        if dir2 == None:
            file2 = 'orig'
            if os.path.exists(file2):
                os.remove(file2)
            if run_recovery:
                self.runWt(['-R', 'dump', uri], outfilename=file2)
            else:
                self.runWt(['dump', uri], outfilename=file2)
        else:
            self.runWt(['-R', '-h', dir2, 'dump', uri], outfilename=file2)
        self.assertEqual(True, compare_files(self, file1, file2))
