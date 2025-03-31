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

import os, time, wttest, threading
from helper import copy_wiredtiger_home
from wtbackup import backup_base

# test_live_restore09.py
# Test that crashing during the log copy phase won't break the logging subsystem.
@wttest.skip_for_hook("tiered", "using multiple WT homes")
class test_live_restore09(backup_base):
    conn_config = 'log=(enabled=true)'

    def test_live_restore09(self):
        # Live restore is not supported on Windows.
        if os.name == 'nt':
            return
        uri = "file:live_restore"
        self.session.create(uri, 'key_format=i,value_format=S')
        cursor = self.session.open_cursor(uri)
        # Insert a lot of data to leave us with several very big log files.
        for i in range (0, 700000):
            cursor[i] = "a" * 1000

        # Close the default connection and back it up.
        os.mkdir("SOURCE")
        self.session.checkpoint()
        self.take_full_backup("SOURCE")
        self.close_conn()

        # Make a directory to restore to.
        os.mkdir("DEST")

        # Start a thread to perform the restore, we need to do this in a separate thread as the
        # call to open_conn will return after live restore has atomically migrated the log files
        # into the destination. We want to catch the copy, prior to rename.
        thread = threading.Thread(target=self.restore)
        thread.start()
        time.sleep(0.5)
        # Copy over the WiredTiger home simulating a crash. We can't call simulate_crash_restart
        # here as we need the thread to finish its open prior to restarting.
        copy_wiredtiger_home(self, "DEST", "RESTART")
        # Wait for the open to finish.
        thread.join()
        # Close the newly opened connection and startup on the restart directory. This used to throw
        # an error while trying to extract the log number from a temporary file.
        self.close_conn()
        self.open_conn("RESTART", config="statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=1)")
        self.ignoreStdoutPatternIfExists('recreating metadata from backup')

    def restore(self):
        self.open_conn("DEST", config="statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=1)")

