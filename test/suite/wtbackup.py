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
import os
import wttest
from suite_subprocess import suite_subprocess
from helper import compare_files

# Shared base class used by backup tests.
class test_backup_base(wttest.WiredTigerTestCase, suite_subprocess):
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
    def compare_backups(self, uri, backup_home, is_incremental = False, suffix = "", run_recovery):
        full_dir = backup_home
        full_out = "./backup"
        if suffix == "0":
            full_out = "./backup_block_full" + "." + suffix
        elif suffix != "":
            full_dir = backup_home + "_LOG_FULL" + "." + suffix
            full_out = "./backup_block_full" + "." + suffix
            
        if os.path.exists(full_out):
            os.remove(full_out)
        #
        # Run wt dump on full backup directory
        #
        self.runWt(['-R', '-h', full_dir, 'dump', uri], outfilename=full_out)
        other_out = "./orig"
        if os.path.exists(other_out):
            os.remove(other_out)
        if is_incremental:
            #
            # Run wt dump on incremental backup
            #
            if suffix != "":
                other_dir = backup_home + "_LOG_INCR" + "." + suffix
                other_out = "./backup_block_incr" + "." + suffix
                self.runWt(['-R', '-h', other_dir, 'dump', uri], outfilename=other_out)
            else:
                self.runWt(['-R', 'dump', uri], outfilename=other_out)
        else:
            self.runWt(['dump', uri], outfilename=other_out)
        self.assertEqual(True, compare_files(self, full_out, other_out))