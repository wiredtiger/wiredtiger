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
import os, random, re, shutil, string
import wiredtiger, wttest
from suite_subprocess import suite_subprocess
from helper import compare_files

# Shared base class used by import tests.
class test_backup_base(wttest.WiredTigerTestCase, suite_subprocess):
    # Set up all the directories needed for the test. We have a full backup directory for each
    # iteration and an incremental backup for each iteration. That way we can compare the full and
    # incremental each time through.
    def setup_directories(self, max_iteration, home_incr_dir, home_full_dir, logpath):
        # We're only coming through once so just set up the 0 and 1 directories.
        for i in range(0, max_iteration):
            # The log directory is a subdirectory of the home directory,
            # creating that will make the home directory also.
            log_dir = home_incr_dir + '.' + str(i) + '/' + logpath
            os.makedirs(log_dir)
            if i != 0:
                log_dir = home_full_dir + '.' + str(i) + '/' + logpath
                os.makedirs(log_dir)

    def compare_backups(self, uri, home_full_dir, full_backup_out, home_incr_dir, incr_backup_out):
        # Run wt dump on full backup directory.
        self.runWt(['-R', '-h', home_full_dir, 'dump', uri], outfilename=full_backup_out)
        # Run wt dump on incremental backup directory.
        self.runWt(['-R', '-h', home_incr_dir, 'dump', uri], outfilename=incr_backup_out)
        self.assertEqual(True, compare_files(self, full_backup_out, incr_backup_out))
